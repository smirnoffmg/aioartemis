"""Use aioartemis in a Litestar application through `ArtemisPlugin`.

The same application as `litestar_app.py`, with the wiring done by the plugin
from `pip install "aioartemis[litestar]"`:

- The plugin runs the client for the application's lifetime and injects it into
  handlers as `artemis`. Broker errors a retry can cure become
  `503 Service Unavailable` with `Retry-After`, an ambiguous send with the
  message id to retry with; the rest stay `500`.
- `POST /orders` publishes an order. The order's id is the message id, so a
  client that retries the HTTP request after a timeout cannot create a second,
  undetectable copy.
- A background consumer is a lifespan of the application's own; it runs inside
  the plugin's, so the client is there on startup and until it stops.
- `GET /health` is a readiness probe that reports why the broker is unreachable.

Run it with `uv run python examples/litestar_plugin.py`, then:

    curl -X POST localhost:8000/orders -H 'content-type: application/json' \\
         -d '{"id": "o-1", "item": "book", "quantity": 2}'
    curl localhost:8000/orders/o-1
    curl localhost:8000/health
"""

import asyncio
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
from dataclasses import asdict, dataclass
from typing import Any

from litestar import Litestar, Request, Response, get, post
from litestar.datastructures import State
from litestar.di import NamedDependency
from litestar.exceptions import NotFoundException
from litestar.params import FromPath, JSONBody
from litestar.status_codes import HTTP_202_ACCEPTED, HTTP_503_SERVICE_UNAVAILABLE

from aioartemis import Client, DeliveryLost
from aioartemis.litestar import ArtemisPlugin

URLS = os.environ.get("ARTEMIS_URLS", "amqp://localhost:5672,amqp://localhost:5673").split(",")
USER = os.environ.get("ARTEMIS_USER", "artemis")
PASSWORD = os.environ.get("ARTEMIS_PASSWORD", "artemis")

ORDERS = "examples.litestar_plugin.orders"


@dataclass
class Order:
    id: str
    item: str
    quantity: int


async def consume_orders(client: Client, processed: dict[str, dict[str, Any]]) -> None:
    async with client.subscribe(ORDERS) as orders:
        async for message in orders:
            order = message.body
            processed[order["id"]] = order  # keyed by id: a redelivered order changes nothing
            with suppress(DeliveryLost):  # the connection dropped; the broker redelivers it
                await message.accept()


@asynccontextmanager
async def order_consumer(app: Litestar) -> AsyncGenerator[None, None]:
    app.state.processed = {}
    consumer = asyncio.create_task(consume_orders(app.state.artemis, app.state.processed))
    try:
        yield
    finally:
        consumer.cancel()
        with suppress(asyncio.CancelledError):
            await consumer


@post("/orders", status_code=HTTP_202_ACCEPTED)
async def create_order(data: JSONBody[Order], artemis: NamedDependency[Client]) -> dict[str, Any]:
    # Do not hold the HTTP request for a whole outage: fail fast, let the caller retry.
    async with asyncio.timeout(5):
        message_id = await artemis.send(ORDERS, asdict(data), message_id=data.id)
    return {"message_id": message_id}


@get("/orders/{order_id:str}")
async def get_order(order_id: FromPath[str], state: State) -> dict[str, Any]:
    processed: dict[str, dict[str, Any]] = state.processed
    if order_id not in processed:
        raise NotFoundException(f"order {order_id} has not been processed yet")
    return processed[order_id]


@get("/health")
async def health(artemis: NamedDependency[Client]) -> Response[dict[str, Any]]:
    if artemis.is_connected:
        return Response({"status": "ok", "node": artemis.connected_url})
    return Response(
        {"status": "unavailable", "reason": artemis.last_error}, status_code=HTTP_503_SERVICE_UNAVAILABLE
    )


def broker_timed_out(request: Request[Any, Any, Any], exc: TimeoutError) -> Response[dict[str, Any]]:
    body = {"detail": "the message broker did not accept the message in time; retry"}
    return Response(body, status_code=HTTP_503_SERVICE_UNAVAILABLE)


app = Litestar(
    route_handlers=[create_order, get_order, health],
    plugins=[ArtemisPlugin(lambda: Client(URLS, user=USER, password=PASSWORD))],
    lifespan=[order_consumer],
    exception_handlers={TimeoutError: broker_timed_out},
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app)
