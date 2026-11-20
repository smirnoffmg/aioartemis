import logging
import subprocess
import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from litestar import Litestar, Request, Response, get
from litestar.di import NamedDependency, Provide
from litestar.exceptions import ImproperlyConfiguredException
from litestar.params import FromPath
from litestar.testing import AsyncTestClient

from aioartemis import (
    ArtemisError,
    AuthenticationFailed,
    Client,
    ClientClosed,
    ConnectionLost,
    EngineFailed,
    LinkRefused,
    SendBacklogFull,
    SendRejected,
    SendReleased,
)
from aioartemis.litestar import ArtemisPlugin
from fake_peer import FakePeer, fake_peer, free_port, url_of


@pytest.fixture
def peer() -> Any:
    with fake_peer() as peer:
        yield peer


def plugin_for(peer: FakePeer, **kwargs: Any) -> ArtemisPlugin:
    return ArtemisPlugin(lambda: Client([url_of(peer)], connect_timeout=5), **kwargs)


@get("/node")
async def node(artemis: NamedDependency[Client]) -> dict[str, Any]:
    return {"connected": artemis.is_connected, "url": artemis.connected_url}


SECRET = "amqp://secret-node:5672"
TRANSIENT = [
    ConnectionLost(f"lost {SECRET}", message_id="m-1"),
    SendReleased(f"released by {SECRET}"),
    SendBacklogFull(f"backlog full for {SECRET}"),
    ClientClosed(f"client of {SECRET} closed"),
]
PERMANENT = [
    SendRejected(f"rejected by {SECRET}"),
    LinkRefused(f"refused by {SECRET}"),
    AuthenticationFailed(f"refused by {SECRET}"),
    EngineFailed(f"engine for {SECRET} died"),
]


@get("/raise/{index:int}")
async def raise_error(index: FromPath[int]) -> None:
    raise (TRANSIENT + PERMANENT)[index]


def path_of(error: ArtemisError) -> str:
    return f"/raise/{(TRANSIENT + PERMANENT).index(error)}"


async def get_with(app: Litestar, error: ArtemisError) -> Any:
    async with AsyncTestClient(app) as http:
        return await http.get(path_of(error))


async def test_client_is_injected_into_handlers(peer: FakePeer) -> None:
    app = Litestar([node], plugins=[plugin_for(peer)])
    async with AsyncTestClient(app) as http:
        response = await http.get("/node")
    assert response.json() == {"connected": True, "url": url_of(peer)}


async def test_dependency_and_state_use_the_given_key(peer: FakePeer) -> None:
    @get("/custom")
    async def custom(broker: NamedDependency[Client]) -> bool:
        return broker.is_connected

    app = Litestar([custom], plugins=[plugin_for(peer, key="broker")])
    async with AsyncTestClient(app) as http:
        assert app.state.broker.is_connected
        assert (await http.get("/custom")).json() is True


async def test_application_lifespans_have_the_client_on_startup_and_shutdown(peer: FakePeer) -> None:
    seen: list[bool] = []

    @asynccontextmanager
    async def uses_client(app: Litestar) -> AsyncGenerator[None, None]:
        seen.append(app.state.artemis.is_connected)
        yield
        seen.append(app.state.artemis.is_connected)

    app = Litestar([], plugins=[plugin_for(peer)], lifespan=[uses_client])
    async with AsyncTestClient(app):
        pass
    assert seen == [True, True]


async def test_client_is_closed_on_shutdown(peer: FakePeer) -> None:
    app = Litestar([], plugins=[plugin_for(peer)])
    async with AsyncTestClient(app):
        client: Client = app.state.artemis
    with pytest.raises(ClientClosed):
        await client.send("anywhere", "body")


async def test_every_startup_gets_a_fresh_client(peer: FakePeer) -> None:
    app = Litestar([], plugins=[plugin_for(peer)])
    async with AsyncTestClient(app):
        first = app.state.artemis
    async with AsyncTestClient(app):
        assert app.state.artemis is not first
        assert app.state.artemis.is_connected


@pytest.mark.parametrize("error", TRANSIENT, ids=lambda e: type(e).__name__)
async def test_transient_broker_errors_are_503_with_retry_after(peer: FakePeer, error: ArtemisError) -> None:
    response = await get_with(Litestar([raise_error], plugins=[plugin_for(peer)]), error)
    assert response.status_code == 503
    assert response.headers["retry-after"] == "1"
    assert "secret-node" not in response.text


async def test_ambiguous_send_carries_the_message_id_to_retry_with(peer: FakePeer) -> None:
    response = await get_with(Litestar([raise_error], plugins=[plugin_for(peer)]), TRANSIENT[0])
    assert response.json()["message_id"] == "m-1"


async def test_retry_after_is_configurable(peer: FakePeer) -> None:
    app = Litestar([raise_error], plugins=[plugin_for(peer, retry_after=7)])
    response = await get_with(app, TRANSIENT[0])
    assert response.headers["retry-after"] == "7"


@pytest.mark.parametrize("error", PERMANENT, ids=lambda e: type(e).__name__)
async def test_permanent_broker_errors_are_left_to_the_application(
    peer: FakePeer, error: ArtemisError
) -> None:
    response = await get_with(Litestar([raise_error], plugins=[plugin_for(peer)]), error)
    assert response.status_code == 500
    assert "secret-node" not in response.text


async def test_transient_errors_are_logged_with_their_cause(
    peer: FakePeer, caplog: pytest.LogCaptureFixture
) -> None:
    # Litestar's logging config replaces the root handlers on startup, caplog's among them.
    logger = logging.getLogger("aioartemis.litestar")
    logger.addHandler(caplog.handler)
    try:
        await get_with(Litestar([raise_error], plugins=[plugin_for(peer)]), TRANSIENT[2])
    finally:
        logger.removeHandler(caplog.handler)
    assert any("backlog full for amqp://secret-node" in r.getMessage() for r in caplog.records)


def teapot(request: Request[Any, Any, Any], exc: Exception) -> Response[str]:
    return Response("mine", status_code=418)


@pytest.mark.parametrize("registered", [ConnectionLost, ArtemisError], ids=lambda c: c.__name__)
async def test_application_handlers_for_broker_errors_take_precedence(
    peer: FakePeer, registered: type[Exception]
) -> None:
    app = Litestar([raise_error], plugins=[plugin_for(peer)], exception_handlers={registered: teapot})
    response = await get_with(app, TRANSIENT[0])
    assert response.status_code == 418


@pytest.mark.parametrize("registered", [Exception, 500], ids=str)
async def test_catch_all_handlers_do_not_override_the_plugin(peer: FakePeer, registered: Any) -> None:
    app = Litestar([raise_error], plugins=[plugin_for(peer)], exception_handlers={registered: teapot})
    assert (await get_with(app, TRANSIENT[0])).status_code == 503
    assert (await get_with(app, PERMANENT[0])).status_code == 418


async def test_exception_handlers_can_be_turned_off(peer: FakePeer) -> None:
    app = Litestar([raise_error], plugins=[plugin_for(peer, exception_handlers=False)])
    assert (await get_with(app, TRANSIENT[0])).status_code == 500


def test_a_dependency_with_the_same_key_is_refused(peer: FakePeer) -> None:
    with pytest.raises(ImproperlyConfiguredException, match="artemis"):
        Litestar(
            [], plugins=[plugin_for(peer)], dependencies={"artemis": Provide(lambda: 1, sync_to_thread=False)}
        )


async def test_application_does_not_start_without_a_reachable_node() -> None:
    plugin = ArtemisPlugin(lambda: Client([f"amqp://127.0.0.1:{free_port()}"], connect_timeout=0.5))
    app = Litestar([], plugins=[plugin])
    # The test client runs the lifespan in a task group, which wraps the error.
    with pytest.raises(ExceptionGroup) as failed:
        async with AsyncTestClient(app):
            pass
    assert failed.group_contains(TimeoutError, match="could not connect")


def test_core_package_imports_without_litestar() -> None:
    code = "import sys; sys.modules['litestar'] = None; import aioartemis; print(aioartemis.Client.__name__)"
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
