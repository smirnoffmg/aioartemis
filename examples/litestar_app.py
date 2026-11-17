"""Use aioartemis in a Litestar application.

- The client lives in the application's lifespan and reaches handlers through
  dependency injection; one client serves every request. The application does
  not start until a node is reachable, up to the client's `connect_timeout`.
- `POST /orders` publishes an order. The order's id is the message id, so a
  client that retries the HTTP request after a timeout cannot create a second,
  undetectable copy.
- A background consumer starts and stops with the application.
- `GET /health` is a readiness probe that reports why the broker is unreachable.
- Broker failures become `503 Service Unavailable`; an ambiguous send also tells
  the caller which message id to retry with.

Run it with `uv run python examples/litestar_app.py`, then:

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
from litestar.di import NamedDependency, Provide
from litestar.exceptions import NotFoundException
from litestar.params import FromPath, JSONBody
from litestar.status_codes import HTTP_202_ACCEPTED, HTTP_503_SERVICE_UNAVAILABLE

from aioartemis import ArtemisError, Client, ConnectionLost, DeliveryLost

URLS = os.environ.get("ARTEMIS_URLS", "amqp://localhost:5672,amqp://localhost:5673").split(",")
USER = os.environ.get("ARTEMIS_USER", "artemis")
PASSWORD = os.environ.get("ARTEMIS_PASSWORD", "artemis")

ORDERS = "examples.litestar.orders"


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
async def artemis_lifespan(app: Litestar) -> AsyncGenerator[None, None]:
    async with Client(URLS, user=USER, password=PASSWORD) as client:
        app.state.artemis = client
        app.state.processed = {}
        consumer = asyncio.create_task(consume_orders(client, app.state.processed))
        try:
            yield
        finally:
            consumer.cancel()
            with suppress(asyncio.CancelledError):
                await consumer


def provide_artemis(state: State) -> Client:
    client: Client = state.artemis
    return client


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


def outcome_unknown(request: Request[Any, Any, Any], exc: ConnectionLost) -> Response[dict[str, Any]]:
    body = {"detail": "the broker may or may not have the message; retry", "message_id": exc.message_id}
    return Response(body, status_code=HTTP_503_SERVICE_UNAVAILABLE)


def broker_timed_out(request: Request[Any, Any, Any], exc: TimeoutError) -> Response[dict[str, Any]]:
    body = {"detail": "the message broker did not accept the message in time; retry"}
    return Response(body, status_code=HTTP_503_SERVICE_UNAVAILABLE)


def broker_unavailable(request: Request[Any, Any, Any], exc: ArtemisError) -> Response[dict[str, Any]]:
    return Response(
        {"detail": f"message broker unavailable: {exc}"}, status_code=HTTP_503_SERVICE_UNAVAILABLE
    )


app = Litestar(
    route_handlers=[create_order, get_order, health],
    lifespan=[artemis_lifespan],
    dependencies={"artemis": Provide(provide_artemis, sync_to_thread=False)},
    exception_handlers={
        ConnectionLost: outcome_unknown,
        ArtemisError: broker_unavailable,
        TimeoutError: broker_timed_out,
    },
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app)
