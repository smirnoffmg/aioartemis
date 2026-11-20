"""ArtemisPlugin against real Artemis nodes, through HTTP requests."""

import asyncio
import os
import signal
import subprocess
import sys
import uuid
from collections.abc import AsyncGenerator, Callable, Coroutine, Generator
from contextlib import asynccontextmanager, contextmanager, suppress
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
from litestar import Litestar, get, post
from litestar.di import NamedDependency
from litestar.params import FromPath

from aioartemis import AuthenticationFailed, Client
from aioartemis.litestar import ArtemisPlugin
from conftest import CREDENTIALS, Node
from fake_peer import free_port

pytestmark = pytest.mark.integration

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


@post("/send/{queue:str}/{message_id:str}", status_code=202)
async def send(
    queue: FromPath[str], message_id: FromPath[str], artemis: NamedDependency[Client]
) -> dict[str, Any]:
    return {"message_id": await artemis.send(queue, "body", message_id=message_id)}


@get("/node")
async def node(artemis: NamedDependency[Client]) -> dict[str, Any]:
    return {"url": artemis.connected_url}


def plugin(*urls: str, **client_kwargs: Any) -> ArtemisPlugin:
    return ArtemisPlugin(lambda: Client(list(urls), shuffle=False, **CREDENTIALS, **client_kwargs))


@asynccontextmanager
async def serve(app: Litestar) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Run the application on this event loop.

    Litestar's AsyncTestClient runs it on a thread of its own and blocks this
    loop for the length of every request: no concurrent requests, and no
    timeout can fire while a request hangs.
    """
    transport = httpx.ASGITransport(app=cast(Any, app))
    async with app.lifespan(), httpx.AsyncClient(transport=transport, base_url="http://test") as http:
        yield http


def background(work: Callable[[Litestar], Coroutine[Any, Any, None]]) -> Callable[[Litestar], Any]:
    """A lifespan that runs `work` in a task and waits for it on shutdown."""

    @asynccontextmanager
    async def lifespan(app: Litestar) -> AsyncGenerator[None, None]:
        task = asyncio.create_task(work(app))
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    return lifespan


async def test_handler_sends_and_application_lifespan_consumes(queue_name: str, node_b: Node) -> None:
    received: list[Any] = []

    async def consume(app: Litestar) -> None:
        async with app.state.artemis.subscribe(queue_name) as messages:
            async for message in messages:
                received.append(message.message_id)
                await message.accept()

    app = Litestar([send], plugins=[plugin(node_b.url)], lifespan=[background(consume)])
    async with serve(app) as http:
        response = await http.post(f"/send/{queue_name}/m-1")
        assert response.status_code == 202, response.text
        assert response.json() == {"message_id": "m-1"}
        async with asyncio.timeout(10):
            while not received:
                await asyncio.sleep(0.05)
    assert received == ["m-1"]


async def test_application_starts_on_the_next_node_when_the_first_is_down(node_b: Node) -> None:
    down = f"amqp://127.0.0.1:{free_port()}"
    app = Litestar([node], plugins=[plugin(down, node_b.url)])
    async with serve(app) as http:
        assert (await http.get("/node")).json() == {"url": node_b.url}


async def test_requests_fail_over_to_the_next_node(queue_name: str, node_a: Node, node_b: Node) -> None:
    app = Litestar([send, node], plugins=[plugin(node_a.url, node_b.url, heartbeat=2.0)])
    async with serve(app) as http:
        assert (await http.get("/node")).json() == {"url": node_a.url}
        node_a.kill()
        response = await asyncio.wait_for(http.post(f"/send/{queue_name}/after"), 30)
        assert response.status_code == 202, response.text
        assert (await http.get("/node")).json() == {"url": node_b.url}


async def test_send_lost_with_the_connection_is_503_with_its_message_id(
    queue_name: str, node_a: Node
) -> None:
    app = Litestar([send], plugins=[plugin(node_a.url, heartbeat=1.0)])
    async with serve(app) as http:
        assert (await http.post(f"/send/{queue_name}/warm-up")).status_code == 202
        node_a.pause()
        try:
            response = await asyncio.wait_for(http.post(f"/send/{queue_name}/order-42"), 15)
        finally:
            node_a.unpause()
    assert response.status_code == 503, response.text
    assert response.headers["retry-after"] == "1"
    assert response.json()["message_id"] == "order-42"


async def test_send_beyond_the_backlog_is_503_at_once(queue_name: str, node_a: Node) -> None:
    app = Litestar([send], plugins=[plugin(node_a.url, heartbeat=None, max_pending_sends=1)])
    async with serve(app) as http:
        node_a.pause()
        try:
            waiting = asyncio.create_task(http.post(f"/send/{queue_name}/first"))
            await asyncio.sleep(0.5)
            response = await asyncio.wait_for(http.post(f"/send/{queue_name}/second"), 5)
            assert not waiting.done()
        finally:
            node_a.unpause()
        assert (await asyncio.wait_for(waiting, 30)).status_code == 202
    assert response.status_code == 503, response.text
    assert response.headers["retry-after"] == "1"
    assert node_a.url not in response.text


async def test_credentials_revoked_after_login_are_500(queue_name: str, node_a: Node) -> None:
    app = Litestar([send], plugins=[plugin(node_a.url, heartbeat=1.0, open_timeout=2.0)])
    async with serve(app) as http:
        assert (await http.post(f"/send/{queue_name}/warm-up")).status_code == 202
        node_a.revoke_credentials()
        node_a.pause()
        try:
            async with asyncio.timeout(10):
                while app.state.artemis.is_connected:
                    await asyncio.sleep(0.05)
            # Artemis keeps accepting a cached login for security-invalidation-interval (10 s).
            await asyncio.sleep(11)
        finally:
            node_a.unpause()
        response = await asyncio.wait_for(http.post(f"/send/{queue_name}/never-sent"), 20)
    assert response.status_code == 500, response.text


async def test_application_does_not_start_with_refused_credentials(node_b: Node) -> None:
    app = Litestar(
        [], plugins=[ArtemisPlugin(lambda: Client([node_b.url], user="artemis", password="wrong"))]
    )
    # Litestar runs its event emitter in a task group, which wraps the error.
    with pytest.raises(ExceptionGroup) as failed:
        async with serve(app):
            pass
    assert failed.group_contains(AuthenticationFailed)


async def test_application_does_not_start_while_the_node_does_not_answer(node_a: Node) -> None:
    app = Litestar([], plugins=[plugin(node_a.url, connect_timeout=2.0, open_timeout=1.0)])
    node_a.pause()
    try:
        with pytest.raises(ExceptionGroup) as failed:
            async with asyncio.timeout(15), serve(app):
                pass
    finally:
        node_a.unpause()
    assert failed.group_contains(TimeoutError, match="could not connect")


async def test_request_waiting_on_a_send_at_shutdown_is_503(queue_name: str, node_a: Node) -> None:
    app = Litestar([send], plugins=[plugin(node_a.url, heartbeat=None, close_timeout=1.0)])
    try:
        async with asyncio.timeout(30):
            async with serve(app) as http:
                assert (await http.post(f"/send/{queue_name}/warm-up")).status_code == 202
                node_a.pause()
                waiting = asyncio.create_task(http.post(f"/send/{queue_name}/in-flight"))
                await asyncio.sleep(0.5)
                # Leaving serve() shuts the application down under the running request.
            response = await waiting
    finally:
        node_a.unpause()
    assert response.status_code == 503, response.text
    assert response.json()["message_id"] == "in-flight"


async def test_message_held_by_the_application_is_redelivered_after_shutdown(
    queue_name: str, node_b: Node
) -> None:
    holding = asyncio.Event()

    async def hold(app: Litestar) -> None:
        async with app.state.artemis.subscribe(queue_name) as messages:
            await messages.get()
            holding.set()
            await asyncio.Event().wait()

    app = Litestar([send], plugins=[plugin(node_b.url)], lifespan=[background(hold)])
    async with serve(app) as http:
        assert (await http.post(f"/send/{queue_name}/held")).status_code == 202
        await asyncio.wait_for(holding.wait(), 10)

    async with Client([node_b.url], **CREDENTIALS) as client, client.subscribe(queue_name) as messages:
        message = await asyncio.wait_for(messages.get(), 10)
        await message.accept()
    assert message.message_id == "held"


@contextmanager
def uvicorn(
    node: Node, password: str = CREDENTIALS["password"]
) -> Generator[tuple[subprocess.Popen[str], str], None, None]:
    port = free_port()
    env = {
        **os.environ,
        "ARTEMIS_URLS": node.url,
        "ARTEMIS_USER": CREDENTIALS["user"],
        "ARTEMIS_PASSWORD": password,
    }
    command = [sys.executable, "-m", "uvicorn", "litestar_plugin:app", "--app-dir", str(EXAMPLES)]
    process = subprocess.Popen(
        [*command, "--port", str(port)], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    try:
        yield process, f"http://127.0.0.1:{port}"
    finally:
        process.kill()
        process.communicate()


async def test_example_serves_requests_under_uvicorn_and_stops_cleanly(node_b: Node) -> None:
    with uvicorn(node_b) as (process, base_url):
        async with httpx.AsyncClient(base_url=base_url) as http:
            async with asyncio.timeout(30):
                while True:
                    with suppress(httpx.ConnectError):
                        health = await http.get("/health")
                        break
                    await asyncio.sleep(0.2)
            assert health.status_code == 200, health.text

            order = {"id": f"o-{uuid.uuid4().hex}", "item": "book", "quantity": 2}
            assert (await http.post("/orders", json=order)).status_code == 202
            async with asyncio.timeout(10):
                while (fetched := await http.get(f"/orders/{order['id']}")).status_code == 404:
                    await asyncio.sleep(0.1)
            assert fetched.json() == order

        process.send_signal(signal.SIGTERM)
        output, _ = process.communicate(timeout=30)
    # Newer uvicorn re-raises the signal it caught once it has shut down.
    assert process.returncode in (0, -signal.SIGTERM), output
    assert "Application shutdown complete" in output


def test_example_under_uvicorn_exits_when_the_credentials_are_refused(node_b: Node) -> None:
    with uvicorn(node_b, password="wrong") as (process, _):
        output, _ = process.communicate(timeout=60)
    assert process.returncode != 0, output
    assert "AuthenticationFailed" in output
    assert "Application startup failed" in output
