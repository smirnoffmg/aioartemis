import asyncio
import threading
import traceback

import pytest

from aioartemis import AuthenticationFailed, Client, ClientClosed, ConnectionLost, DeliveryLost
from conftest import CREDENTIALS, Node

# These tests start Artemis in Docker.
pytestmark = pytest.mark.integration


async def test_roundtrip(queue_name: str, node_b: Node) -> None:
    async with Client([node_b.url], **CREDENTIALS) as client, client.subscribe(queue_name) as sub:
        await client.send(queue_name, "hello", properties={"k": "v"})
        msg = await asyncio.wait_for(sub.get(), 5)
        await msg.accept()

    assert msg.body == "hello"
    assert msg.properties == {"k": "v"}
    assert msg.message_id, "consumers need an id to discard duplicates"


async def test_prefetch_limits_unacked_messages(queue_name: str, node_b: Node) -> None:
    async with (
        Client([node_b.url], **CREDENTIALS) as client,
        client.subscribe(queue_name, prefetch=2) as sub,
    ):
        for i in range(4):
            await client.send(queue_name, i)

        first = await asyncio.wait_for(sub.get(), 5)
        await asyncio.wait_for(sub.get(), 5)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(sub.get(), 1)

        await first.accept()
        third = await asyncio.wait_for(sub.get(), 5)

    assert third.body == 2


async def test_failover_to_next_node(queue_name: str, node_a: Node, node_b: Node) -> None:
    async with (
        Client([node_a.url, node_b.url], heartbeat=2.0, shuffle=False, **CREDENTIALS) as client,
        client.subscribe(queue_name) as sub,
    ):
        await client.send(queue_name, "before")
        before = await asyncio.wait_for(sub.get(), 5)
        await before.accept()
        assert client.connected_url == node_a.url

        # A graceful shutdown, unlike a kill, leaves the transport half-closed
        # until the heartbeat expires: a send in that window must wait.
        node_a.stop()
        await asyncio.wait_for(client.send(queue_name, "after"), 30)
        after = await asyncio.wait_for(sub.get(), 10)
        await after.accept()

        assert client.connected_url == node_b.url
    assert after.body == "after"


async def test_ack_of_message_from_lost_connection_raises(
    queue_name: str, node_a: Node, node_b: Node
) -> None:
    async with (
        Client([node_a.url, node_b.url], heartbeat=2.0, shuffle=False, **CREDENTIALS) as client,
        client.subscribe(queue_name) as sub,
    ):
        await client.send(queue_name, "orphan")
        orphan = await asyncio.wait_for(sub.get(), 5)

        node_a.kill()
        await asyncio.wait_for(client.send(queue_name, "probe"), 30)

        with pytest.raises(DeliveryLost):
            await orphan.accept()


async def test_send_during_outage_waits_for_reconnect(queue_name: str, node_a: Node, node_b: Node) -> None:
    async with (
        Client([node_a.url, node_b.url], heartbeat=2.0, shuffle=False, **CREDENTIALS) as client,
        client.subscribe(queue_name) as sub,
    ):
        await client.send(queue_name, "warm-up")
        await (await asyncio.wait_for(sub.get(), 5)).accept()

        node_b.pause()
        try:
            node_a.kill()
            async with asyncio.timeout(10):
                while client.is_connected:
                    await asyncio.sleep(0.05)

            send = asyncio.create_task(client.send(queue_name, "queued"))
            await asyncio.sleep(1)
            assert not send.done(), "nothing to send to while every node is down"
        finally:
            node_b.unpause()

        await asyncio.wait_for(send, 30)
        msg = await asyncio.wait_for(sub.get(), 10)
        await msg.accept()

        assert client.connected_url == node_b.url
    assert msg.body == "queued"


async def test_message_sent_before_any_subscriber_is_kept(queue_name: str, node_b: Node) -> None:
    async with Client([node_b.url], **CREDENTIALS) as client:
        await client.send(queue_name, "early")
        async with client.subscribe(queue_name) as sub:
            msg = await asyncio.wait_for(sub.get(), 5)
            await msg.accept()
    assert msg.body == "early"


async def test_subscriptions_on_one_address_compete_for_messages(queue_name: str, node_b: Node) -> None:
    async with (
        asyncio.timeout(15),  # a second link that never attaches would hang in subscribe()
        Client([node_b.url], **CREDENTIALS) as client,
        client.subscribe(queue_name) as first,
        client.subscribe(queue_name) as second,
    ):
        for i in range(4):
            await client.send(queue_name, i)
        async with asyncio.timeout(5):
            while first.qsize() + second.qsize() < 4:
                await asyncio.sleep(0.05)
        await asyncio.sleep(0.5)  # a duplicate, if any, would arrive meanwhile

        bodies: dict[str, list[int]] = {}
        for name, sub in (("first", first), ("second", second)):
            bodies[name] = []
            while not sub.empty():
                msg = await sub.get()
                await msg.accept()
                bodies[name].append(msg.body)

    assert sorted(bodies["first"] + bodies["second"]) == [0, 1, 2, 3]
    assert bodies["first"]
    assert bodies["second"], "the second link must be live, not shadowed by a name clash"


async def test_nacked_message_counts_as_failed_delivery(queue_name: str, node_b: Node) -> None:
    async with Client([node_b.url], **CREDENTIALS) as client, client.subscribe(queue_name) as sub:
        await client.send(queue_name, "poison")
        first = await asyncio.wait_for(sub.get(), 5)
        await first.nack()
        again = await asyncio.wait_for(sub.get(), 5)
        await again.accept()

    assert first.delivery_count == 0
    assert again.delivery_count == 1, "the broker can only dead-letter what it counts"


async def test_released_message_comes_back_without_penalty(queue_name: str, node_b: Node) -> None:
    async with Client([node_b.url], **CREDENTIALS) as client, client.subscribe(queue_name) as sub:
        await client.send(queue_name, "not now")
        first = await asyncio.wait_for(sub.get(), 5)
        await first.release()
        again = await asyncio.wait_for(sub.get(), 5)
        await again.accept()

    assert again.delivery_count == 0


async def test_settle_errors_carry_no_proton_thread_frames(queue_name: str, node_b: Node) -> None:
    async with Client([node_b.url], **CREDENTIALS) as client, client.subscribe(queue_name) as sub:
        await client.send(queue_name, "once")
        msg = await asyncio.wait_for(sub.get(), 5)
        await msg.accept()
        with pytest.raises(DeliveryLost, match="already settled") as error:
            await msg.accept()

    # A frame from the engine would keep proton objects alive on this thread.
    frames = [frame.f_code.co_filename for frame, _ in traceback.walk_tb(error.value.__traceback__)]
    assert not any(name.endswith("_engine.py") for name in frames)


async def test_node_refusing_credentials_during_failover_is_skipped(
    queue_name: str, node_a: Node, misconfigured_node: Node
) -> None:
    urls = [node_a.url, misconfigured_node.url]
    async with Client(urls, heartbeat=1.0, open_timeout=2.0, shuffle=False, **CREDENTIALS) as client:
        node_a.pause()
        try:
            async with asyncio.timeout(10):
                while client.is_connected:
                    await asyncio.sleep(0.05)
            await asyncio.sleep(2)  # long enough to have been refused by the other node
        finally:
            node_a.unpause()

        await asyncio.wait_for(client.send(queue_name, "back"), 30)
        assert client.connected_url == node_a.url


async def test_in_flight_send_fails_with_its_message_id(queue_name: str, node_a: Node) -> None:
    async with Client([node_a.url], heartbeat=1.0, **CREDENTIALS) as client:
        await client.send(queue_name, "warm-up")
        node_a.pause()
        try:
            with pytest.raises(ConnectionLost) as lost:
                await asyncio.wait_for(client.send(queue_name, "x", message_id="order-42"), 10)
        finally:
            node_a.unpause()

    assert lost.value.message_id == "order-42"


async def test_send_returns_message_id_for_safe_retries(queue_name: str, node_b: Node) -> None:
    async with Client([node_b.url], **CREDENTIALS) as client:
        generated = await client.send(queue_name, "a")
        given = await client.send(queue_name, "b", message_id="m-1")
    assert generated
    assert given == "m-1"


async def test_unencodable_body_fails_fast_and_client_survives(queue_name: str, node_b: Node) -> None:
    async with Client([node_b.url], **CREDENTIALS) as client:
        with pytest.raises(TypeError):
            await asyncio.wait_for(client.send(queue_name, object()), 5)
        await asyncio.wait_for(client.send(queue_name, "fine"), 5)


async def test_send_after_close_raises(queue_name: str, node_b: Node) -> None:
    client = Client([node_b.url], **CREDENTIALS)
    async with client:
        pass
    with pytest.raises(ClientClosed):
        await asyncio.wait_for(client.send(queue_name, "late"), 5)


async def test_wrong_password_fails_fast(node_b: Node) -> None:
    started = asyncio.get_running_loop().time()
    with pytest.raises(AuthenticationFailed):
        async with Client([node_b.url], user="artemis", password="wrong", connect_timeout=30):
            pass
    assert asyncio.get_running_loop().time() - started < 10


async def test_close_is_bounded_when_node_stops_answering(node_a: Node) -> None:
    client = Client([node_a.url], close_timeout=2.0, **CREDENTIALS)
    await client.__aenter__()
    node_a.pause()
    await asyncio.sleep(1)  # the freeze is not instant; without this the node still answers close
    try:
        started = asyncio.get_running_loop().time()
        await client.__aexit__(None, None, None)
        elapsed = asyncio.get_running_loop().time() - started
        # Checked before unpausing: a thawed node answers the close and lets the thread go.
        leaked = [t for t in threading.enumerate() if t.name == "aioartemis" and t.is_alive()]
    finally:
        node_a.unpause()

    assert elapsed < 3.0, "close must not wait for an answer that never comes"
    assert leaked == []


async def test_messages_from_a_lost_connection_are_not_handed_out(
    queue_name: str, node_a: Node, node_b: Node
) -> None:
    async with (
        Client([node_a.url, node_b.url], heartbeat=2.0, shuffle=False, **CREDENTIALS) as client,
        client.subscribe(queue_name, prefetch=5) as sub,
    ):
        for i in range(3):
            await client.send(queue_name, f"stale-{i}")
        async with asyncio.timeout(5):
            while sub.qsize() < 3:
                await asyncio.sleep(0.05)

        node_a.kill()
        await asyncio.wait_for(client.send(queue_name, "fresh"), 30)
        msg = await asyncio.wait_for(sub.get(), 10)
        await msg.accept()

    assert msg.body == "fresh"


async def test_rejected_message_goes_to_the_dead_letter_queue(queue_name: str, node_b: Node) -> None:
    async with (
        Client([node_b.url], **CREDENTIALS) as client,
        client.subscribe(queue_name) as sub,
        client.subscribe("DLQ") as dead_letters,
    ):
        message_id = await client.send(queue_name, "unprocessable")
        msg = await asyncio.wait_for(sub.get(), 5)
        await msg.reject()

        async with asyncio.timeout(5):
            while True:
                dead = await dead_letters.get()
                await dead.accept()
                if dead.message_id == message_id:
                    break
        assert sub.empty(), "a rejected message is not redelivered to the queue"


async def test_message_nacked_too_often_goes_to_the_dead_letter_queue(queue_name: str, node_b: Node) -> None:
    async with (
        asyncio.timeout(30),
        Client([node_b.url], **CREDENTIALS) as client,
        client.subscribe(queue_name) as sub,
        client.subscribe("DLQ") as dead_letters,
    ):
        message_id = await client.send(queue_name, "poison")
        attempts = 0
        while True:
            try:
                msg = await asyncio.wait_for(sub.get(), 2)
            except TimeoutError:
                break  # no longer redelivered
            attempts += 1
            await msg.nack()

        while (dead := await dead_letters.get()).message_id != message_id:
            await dead.accept()
        await dead.accept()

    assert attempts == 10, "Artemis' default max-delivery-attempts"


async def test_binary_values_arrive_intact(queue_name: str, node_b: Node) -> None:
    expected = [f"payload-{i}".encode() for i in range(20)]
    async with (
        Client([node_b.url], **CREDENTIALS) as client,
        client.subscribe(queue_name, prefetch=20) as sub,
    ):
        for body in expected:
            await client.send(queue_name, body, properties={"raw": b"\x00\x01"})
        # Read only after all of them arrived: by then proton has freed and reused
        # the buffers of the first ones.
        received = [await asyncio.wait_for(sub.get(), 5) for _ in expected]
        for msg in received:
            await msg.accept()

    assert [msg.body for msg in received] == expected
    assert all(type(msg.body) is bytes for msg in received)
    assert all(msg.properties == {"raw": b"\x00\x01"} for msg in received)


async def test_broker_drops_a_resend_carrying_the_same_duplicate_id(queue_name: str, node_b: Node) -> None:
    async with Client([node_b.url], **CREDENTIALS) as client, client.subscribe(queue_name) as sub:
        for body in ("first", "resent"):
            await client.send(queue_name, body, properties={"_AMQ_DUPL_ID": "order-42"})
        first = await asyncio.wait_for(sub.get(), 5)
        await first.accept()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(sub.get(), 1)

    assert first.body == "first"


async def test_refused_first_login_is_retried_within_the_auth_retry_window(node_b: Node) -> None:
    started = asyncio.get_running_loop().time()
    with pytest.raises(AuthenticationFailed):
        async with Client(
            [node_b.url], user="artemis", password="wrong", auth_retry_window=2.0, connect_timeout=30
        ):
            pass
    assert asyncio.get_running_loop().time() - started >= 2.0
