import asyncio
import threading
import time
from contextlib import suppress

import pytest

from aioartemis import Client, ClientClosed, EngineFailed, LinkRefused
from fake_peer import fake_peer, free_port, url_of


def proton_threads() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name == "aioartemis" and t.is_alive()]


async def test_connection_closed_by_broker_with_error_is_reconnected() -> None:
    with fake_peer(close_with="amqp:internal-error") as peer:
        async with Client([url_of(peer)], heartbeat=1.0) as client:
            async with asyncio.timeout(10):
                while peer.connections < 2 or not client.is_connected:
                    await asyncio.sleep(0.05)

            await asyncio.wait_for(client.send("q", "after"), 5)

    assert peer.received == ["after"]


async def test_node_that_drops_every_connection_is_backed_off() -> None:
    with fake_peer(close_with="amqp:connection:redirect", close_after=0.0, close_every=True) as peer:
        client = Client([url_of(peer)], reconnect_base=0.1, reconnect_max=10.0, connect_timeout=60)
        entering = asyncio.create_task(client.__aenter__())
        await asyncio.sleep(2)
        connections = peer.connections
        await client.aclose()
        # The client may catch a moment between open and close and count as
        # connected; either way only the number of attempts matters here.
        with suppress(ClientClosed):
            await entering

    # Exponential backoff from 0.1 s allows a handful of attempts in 2 s; a
    # counter reset on every successful open would allow thousands.
    assert connections <= 10


async def test_connection_dropped_right_after_first_open_does_not_fail_startup() -> None:
    # The client may catch the first connection between its open and its close and
    # legitimately return from __aenter__ then; it must never return disconnected.
    with fake_peer(close_with="amqp:internal-error", close_after=0.0) as peer:
        async with Client([url_of(peer)], connect_timeout=10) as client:
            assert client.is_connected


async def test_startup_timeout_reports_the_last_error() -> None:
    with pytest.raises(TimeoutError, match=r"(?i)connection refused"):
        async with Client([f"amqp://127.0.0.1:{free_port()}"], connect_timeout=1.5):
            pass


async def test_cancelled_startup_leaves_no_thread_behind() -> None:
    nowhere = f"amqp://127.0.0.1:{free_port()}"
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.5), Client([nowhere]):
            pass
    assert proton_threads() == []


async def test_close_during_startup_returns_promptly() -> None:
    client = Client([f"amqp://127.0.0.1:{free_port()}"], connect_timeout=30)
    entering = asyncio.create_task(client.__aenter__())
    await asyncio.sleep(0.3)
    started = time.monotonic()
    await client.aclose()
    with pytest.raises(ClientClosed):
        await asyncio.wait_for(entering, 3)
    assert time.monotonic() - started < 3


async def test_engine_crash_fails_calls_and_closes_the_connection() -> None:
    def boom() -> None:
        raise RuntimeError("bug on the proton thread")

    with fake_peer() as peer:
        client = Client([url_of(peer)])
        await client.__aenter__()
        assert client._engine is not None
        client._engine.submit(boom)  # a command without an outcome: nothing to report it to

        with pytest.raises(EngineFailed):
            await asyncio.wait_for(client.send("q", "x"), 5)
        async with asyncio.timeout(5):
            while peer.open_connections or proton_threads():
                await asyncio.sleep(0.05)
        await client.aclose()


async def test_refused_send_fails_only_that_address() -> None:
    with fake_peer(refuse=frozenset({"missing"})) as peer:
        async with Client([url_of(peer)]) as client:
            with pytest.raises(LinkRefused, match="amqp:not-found"):
                await asyncio.wait_for(client.send("missing", "x"), 5)

            await asyncio.wait_for(client.send("present", "y"), 5)
            assert client.is_connected

    assert peer.received == ["y"]


async def test_refused_subscribe_raises_instead_of_waiting_forever() -> None:
    with fake_peer(refuse=frozenset({"missing"})) as peer:
        async with Client([url_of(peer)]) as client:
            with pytest.raises(LinkRefused):
                async with asyncio.timeout(5), client.subscribe("missing"):
                    pass
            async with asyncio.timeout(5), client.subscribe("present"):
                pass
            assert client.is_connected
