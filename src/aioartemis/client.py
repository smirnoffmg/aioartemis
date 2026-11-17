from __future__ import annotations

import asyncio
import itertools
import logging
import threading
import uuid
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from typing import Any

from proton import SSL, Message
from proton.reactor import Container

from ._engine import Engine, EngineConfig, PendingSend, Received, Routing, Settlement, _Subscription
from .errors import ArtemisError, ClientClosed, EngineFailed

log = logging.getLogger("aioartemis")

_THREAD_NAME = "aioartemis"


class _FutureOutcome:
    """Bridges a result from the proton thread into an asyncio future."""

    def __init__(self, client: Client) -> None:
        self._loop = client._running_loop()
        self.future: asyncio.Future[Any] = self._loop.create_future()
        client._outstanding.add(self.future)
        self.future.add_done_callback(client._outstanding.discard)

    def ok(self, value: Any = None) -> None:
        _call_soon(self._loop, self._set, value, None)

    def fail(self, exc: BaseException) -> None:
        _call_soon(self._loop, self._set, None, exc)

    def _set(self, value: Any, exc: BaseException | None) -> None:
        if self.future.done():
            return
        if exc is None:
            self.future.set_result(value)
        else:
            self.future.set_exception(exc)


@dataclass(frozen=True)
class IncomingMessage:
    """A received message. It stays unsettled, counting against the subscription's
    `prefetch`, until one of `accept`, `reject`, `nack` or `release` is called.

    Each of those settles the message locally and queues the outcome for the
    broker; it returns without waiting for the broker to see it. If the connection
    drops in between, the broker redelivers the message, so processing must
    tolerate duplicates even after a successful `accept`.

    Attributes:
        body: The decoded AMQP body (`str`, `bytes`, `dict`, `list`, numbers, ...).
        properties: The application properties, empty if there are none.
        message_id: The AMQP message id. Messages sent by this library always
            carry one; messages from other producers may have `None`.
        delivery_count: How many earlier deliveries of this message failed:
            0 on the first delivery, incremented by `nack` but not by `release`.

    All four methods raise `DeliveryLost` if the message was already settled or
    came over a connection or subscription that is gone.
    """

    body: Any
    properties: dict[str, Any]
    message_id: Any
    delivery_count: int
    _client: Client
    _received: Received

    async def accept(self) -> None:
        """The message was processed; the broker removes it from the queue."""
        await self._client._settle(self._received, "accept")

    async def reject(self) -> None:
        """The message can never be processed. Artemis moves it straight to the
        address's dead-letter address, ignoring `max-delivery-attempts`, or drops
        it when none is configured. For a failure that may pass, use `nack`."""
        await self._client._settle(self._received, "reject")

    async def nack(self) -> None:
        """Processing failed: redeliver the message, counting a failed attempt, so
        the broker's `max-delivery-attempts` and dead-letter address apply."""
        await self._client._settle(self._received, "nack")

    async def release(self) -> None:
        """Hand the message back unprocessed (e.g. on shutdown) without counting
        an attempt. A message released over and over is never dead-lettered."""
        await self._client._settle(self._received, "release")


class Subscription:
    """Messages received for one `Client.subscribe` call.

    Iterate with `async for`, or call `get`. Iteration never ends by itself: leave
    it with `break`, by cancelling, or through the error the subscription fails
    with: `LinkRefused` if the broker detaches it, `ClientClosed` when the client
    closes, `EngineFailed` if the engine dies.

    Messages that arrived over a connection that has since dropped are never
    handed out: they can no longer be settled and the broker redelivers them.
    """

    def __init__(self, client: Client) -> None:
        self._client = client
        self._messages: deque[IncomingMessage] = deque()
        self._error: BaseException | None = None
        self._ready = asyncio.Event()

    async def get(self) -> IncomingMessage:
        """Wait for the next message.

        Raises:
            LinkRefused: The broker detached the subscription.
            ClientClosed: The client was closed.
            EngineFailed: The engine thread died.
        """
        while True:
            self._drop_stale()
            if self._messages:
                return self._messages.popleft()
            if self._error is not None:
                raise self._error
            self._ready.clear()
            await self._ready.wait()

    def qsize(self) -> int:
        """Number of messages ready to be returned by `get` without waiting."""
        self._drop_stale()
        return len(self._messages)

    def empty(self) -> bool:
        """Whether `get` would have to wait."""
        return self.qsize() == 0

    def _push(self, message: IncomingMessage) -> None:
        self._messages.append(message)
        self._ready.set()

    def _fail(self, error: BaseException) -> None:
        if self._error is None:
            self._error = error
        self._ready.set()

    def _drop_stale(self) -> None:
        live = self._client._live_epoch
        while self._messages and self._messages[0]._received.epoch != live:
            self._messages.popleft()

    def __aiter__(self) -> AsyncIterator[IncomingMessage]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[IncomingMessage]:
        while True:
            yield await self.get()


class Client:
    """asyncio AMQP 1.0 client for ActiveMQ Artemis with failover across nodes.

    Use it once, as an async context manager: entering connects, leaving closes.
    The client belongs to the event loop it was entered in, and runs the AMQP
    engine on a thread of its own with a single connection to one node at a time.
    Delivery is at-least-once; see the Delivery guarantees page.

    Args:
        urls: Every node to use, as `amqp://host:port` (or one load balancer).
        user: User name for SASL authentication.
        password: Password for SASL authentication. Over `amqp://` it is sent in
            clear text.
        heartbeat: AMQP idle timeout in seconds: a connection that carries no
            traffic for this long is considered dead. `None` disables it.
        connect_timeout: Seconds `async with Client(...)` waits for the first
            connection before raising `TimeoutError`.
        open_timeout: Seconds a single node may take to open a connection before
            the client gives up on it and tries the next one.
        close_timeout: Seconds closing waits for the broker to confirm before
            dropping the connection.
        reconnect_base: Seconds of the first backoff step between passes over
            the nodes.
        reconnect_max: Cap of the backoff, in seconds. A connection must also
            stay up this long before the backoff resets.
        shuffle: Shuffle `urls` once per client, to spread clients over nodes.

    Raises:
        ValueError: `urls` is empty.
        RuntimeError: An `amqps://` URL was given, but python-qpid-proton was built
            without OpenSSL.
    """

    def __init__(
        self,
        urls: list[str],
        *,
        user: str | None = None,
        password: str | None = None,
        heartbeat: float | None = 10.0,
        connect_timeout: float = 30.0,
        open_timeout: float = 10.0,
        close_timeout: float = 5.0,
        reconnect_base: float = 0.1,
        reconnect_max: float = 10.0,
        shuffle: bool = True,
    ) -> None:
        if not urls:
            raise ValueError("at least one url is required")
        if any(url.startswith("amqps://") for url in urls) and not SSL.present():
            # python-qpid-proton builds without TLS, silently, when the OpenSSL headers
            # are missing; amqps:// would otherwise fail later on the engine thread.
            raise RuntimeError(
                "amqps:// needs TLS, but python-qpid-proton was built without OpenSSL. Install "
                "pkg-config and the OpenSSL headers (libssl-dev on Debian/Ubuntu, openssl-dev on "
                "Alpine), then rebuild it: pip install --force-reinstall --no-cache-dir python-qpid-proton"
            )
        self._config = EngineConfig(
            urls=list(urls),
            user=user,
            password=password,
            heartbeat=heartbeat,
            open_timeout=open_timeout,
            close_timeout=close_timeout,
            reconnect_base=reconnect_base,
            reconnect_max=reconnect_max,
        )
        self._shuffle = shuffle
        self._connect_timeout = connect_timeout
        self._loop: asyncio.AbstractEventLoop | None = None
        self._engine: Engine | None = None
        self._thread: threading.Thread | None = None
        self._connected = asyncio.Event()
        self._failure: BaseException | None = None
        self._failed = asyncio.Event()
        self._closed = False
        self._closing = asyncio.Event()
        self._last_error: str | None = None
        self._live_epoch: int | None = None
        self._outstanding: set[asyncio.Future[Any]] = set()
        self._subscriptions: dict[int, Subscription] = {}
        self._subscription_ids = itertools.count()
        self._connected_url: str | None = None

    @property
    def is_connected(self) -> bool:
        """Whether a connection to a node is open right now."""
        return self._connected.is_set()

    @property
    def connected_url(self) -> str | None:
        """URL of the node of the current connection, or of the last one while
        reconnecting; `None` before the first connection."""
        return self._connected_url

    @property
    def last_error(self) -> str | None:
        """Why the last connection attempt failed or the last connection dropped."""
        return self._last_error

    async def __aenter__(self) -> Client:
        """Start the engine and wait for the first connection.

        Raises:
            TimeoutError: No node could be reached within `connect_timeout`; the
                message includes the last error.
            AuthenticationFailed: The credentials were refused.
            ClientClosed: `aclose` was called while connecting.
            RuntimeError: The client was already entered once.
        """
        if self._thread is not None:
            raise RuntimeError("a Client can be entered only once")
        self._loop = asyncio.get_running_loop()
        engine = self._engine = Engine(self._config, _Listener(self), shuffle=self._shuffle)
        self._thread = threading.Thread(target=self._run, args=(engine,), name=_THREAD_NAME, daemon=True)
        self._thread.start()
        try:
            await self._wait_until_connected()
        except BaseException:
            # Covers cancellation too: __aexit__ never runs when __aenter__ fails,
            # and the proton thread would otherwise reconnect forever.
            await asyncio.shield(self.aclose())
            raise
        return self

    async def _wait_until_connected(self) -> None:
        try:
            async with asyncio.timeout(self._connect_timeout):
                # A loop, not a single wait: the first connection may open and
                # drop again before this task gets to look at it.
                while not self.is_connected:
                    if self._failure is not None:
                        raise self._failure
                    if self._closing.is_set():
                        raise ClientClosed("client closed while connecting")
                    await _first_of(self._connected, self._failed, self._closing)
        except TimeoutError as exc:
            reason = f": {self._last_error}" if self._last_error else ""
            raise TimeoutError(
                f"could not connect to any of {self._config.urls} in {self._connect_timeout}s{reason}"
            ) from exc

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close the client; leaving `async with` does this. Idempotent.

        Sends still waiting to be transmitted fail with `ClientClosed`, sends
        awaiting the broker's answer with `ConnectionLost`, and subscriptions
        with `ClientClosed`. Waits at most `close_timeout` plus two seconds.
        """
        if self._closed:
            return
        self._closed = True
        self._closing.set()
        engine, thread = self._engine, self._thread
        if engine is None or thread is None:
            return
        with suppress(ClientClosed):
            engine.submit(engine.shutdown)
        await asyncio.to_thread(thread.join, self._config.close_timeout + 2)
        if thread.is_alive():
            log.warning("proton thread did not exit within the close timeout")
        self._fail_outstanding(ClientClosed("client closed"))

    async def send(
        self,
        address: str,
        body: Any,
        *,
        properties: dict[str, Any] | None = None,
        message_id: Any = None,
        durable: bool = True,
        routing: Routing = "queue",
    ) -> Any:
        """Send a message and wait until the broker has accepted it.

        While the client is reconnecting the message waits in a local queue, which
        is not bounded; cancel the call (e.g. with `asyncio.timeout`) to give up on
        it. A message already handed to the broker cannot be recalled by
        cancelling.

        Args:
            address: The Artemis address to send to.
            body: The AMQP body: `str`, `bytes`, `dict`, `list`, numbers, ...
            properties: AMQP application properties.
            message_id: Id for consumers to deduplicate by; a UUID by default.
                Pass the same id when retrying after `ConnectionLost`.
            durable: Ask the broker to persist the message.
            routing: `"queue"` (anycast) or `"topic"` (multicast). Decides how
                Artemis auto-creates the address.

        Returns:
            The message id.

        Raises:
            ConnectionLost: The outcome is unknown; retry with the same id.
            SendRejected: The broker refused the message.
            SendReleased: The broker did not take the message; safe to resend.
            LinkRefused: The broker refused the link for this address.
            ClientClosed: The client was closed.
            AuthenticationFailed: The client failed to log in.
            EngineFailed: The engine thread died.
            TypeError: AMQP cannot encode the body.
        """
        engine = self._usable_engine()
        message_id = message_id if message_id is not None else str(uuid.uuid4())
        message = Message(body=body, properties=properties, id=message_id, durable=durable)
        try:
            encoded = message.encode()
        except Exception as exc:
            raise TypeError(f"cannot encode message body {body!r}: {exc}") from exc

        outcome = _FutureOutcome(self)
        item = PendingSend(encoded, message_id, outcome)
        engine.submit(lambda: engine.send(address, routing, item), outcome)
        try:
            return await outcome.future
        except asyncio.CancelledError:
            item.abandoned = True
            with suppress(ArtemisError):
                engine.submit(lambda: engine.forget(address, routing, item))
            raise

    @asynccontextmanager
    async def subscribe(
        self, address: str, *, prefetch: int = 10, routing: Routing = "queue"
    ) -> AsyncGenerator[Subscription, None]:
        """Receive messages from an address; use as
        `async with client.subscribe(...) as subscription`.

        Entering waits until the broker attaches the link, which while the client
        is reconnecting lasts until it is back; bound it with `asyncio.timeout` if
        that matters. The subscription is re-attached after every failover, but
        not after the broker detaches it. Leaving closes it; messages not yet
        settled are redelivered by the broker.

        With `routing="topic"` the broker keeps a queue for the subscription only
        while it is attached: messages published while the client is
        reconnecting are not delivered to it.

        Args:
            address: The Artemis address (queue) to receive from.
            prefetch: How many messages may be delivered but not yet settled.
            routing: `"queue"` (anycast, consumers compete) or `"topic"`
                (multicast, each subscription gets every message).

        Raises:
            LinkRefused: The broker refused the subscription.
            ClientClosed: The client was closed.
            ValueError: `prefetch` is below 1.
        """
        if prefetch < 1:
            raise ValueError("prefetch must be at least 1")
        engine = self._usable_engine()
        subscription = Subscription(self)
        subscription_id = next(self._subscription_ids)
        loop = self._running_loop()

        def deliver(received: Received) -> None:
            _call_soon(loop, self._deliver, subscription, received)

        def fail(exc: BaseException) -> None:
            _call_soon(loop, subscription._fail, exc)

        opened = _FutureOutcome(self)
        # Built on the proton thread: it will hold a proton Link, and the last
        # reference to a proton object must not be dropped on this thread.
        engine.submit(
            lambda: engine.subscribe(
                _Subscription(subscription_id, address, routing, prefetch, deliver, fail, opened)
            ),
            opened,
        )
        self._subscriptions[subscription_id] = subscription
        try:
            await opened.future
            yield subscription
        finally:
            del self._subscriptions[subscription_id]
            with suppress(ArtemisError):
                engine.submit(lambda: engine.unsubscribe(subscription_id))

    # -- internals ----------------------------------------------------------------

    def _deliver(self, subscription: Subscription, received: Received) -> None:
        subscription._push(
            IncomingMessage(
                body=received.body,
                properties=received.properties,
                message_id=received.message_id,
                delivery_count=received.delivery_count,
                _client=self,
                _received=received,
            )
        )

    async def _settle(self, received: Received, how: Settlement) -> None:
        engine = self._usable_engine()
        outcome = _FutureOutcome(self)
        engine.submit(
            lambda: engine.settle_received(
                received.subscription_id, received.delivery_id, received.epoch, how, outcome
            ),
            outcome,
        )
        await outcome.future

    def _usable_engine(self) -> Engine:
        if self._failure is not None:
            raise self._failure
        if self._closed:
            raise ClientClosed("client closed")
        if self._engine is None:
            raise RuntimeError("use the Client as an async context manager first")
        return self._engine

    def _running_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            raise RuntimeError("use the Client as an async context manager first")
        return self._loop

    def _run(self, engine: Engine) -> None:
        loop = self._running_loop()
        container = Container(engine)
        try:
            container.run()
        except BaseException as exc:
            log.exception("proton thread died")
            _call_soon(loop, self._mark_failed, EngineFailed(f"proton thread died: {exc!r}"))
            engine.crash_cleanup(container)
        finally:
            engine.release_fds()
            _call_soon(loop, self._engine_exited)

    def _engine_exited(self) -> None:
        self._connected.clear()
        self._live_epoch = None
        error = self._failure or ClientClosed("client closed")
        self._fail_outstanding(error)
        for subscription in self._subscriptions.values():
            subscription._fail(error)

    def _fail_outstanding(self, error: BaseException) -> None:
        for future in list(self._outstanding):
            if not future.done():
                future.set_exception(error)

    def _mark_connected(self, url: str, epoch: int) -> None:
        self._connected_url = url
        self._live_epoch = epoch
        self._connected.set()

    def _mark_disconnected(self) -> None:
        self._live_epoch = None
        self._connected.clear()

    def _mark_failed(self, exc: BaseException) -> None:
        self._failure = exc
        self._failed.set()

    def _record_error(self, reason: str) -> None:
        self._last_error = reason


class _Listener:
    def __init__(self, client: Client) -> None:
        self._client = client
        self._loop = client._running_loop()

    def connected(self, url: str, epoch: int) -> None:
        _call_soon(self._loop, self._client._mark_connected, url, epoch)

    def disconnected(self) -> None:
        _call_soon(self._loop, self._client._mark_disconnected)

    def fatal(self, exc: BaseException) -> None:
        _call_soon(self._loop, self._client._mark_failed, exc)

    def error(self, reason: str) -> None:
        _call_soon(self._loop, self._client._record_error, reason)


async def _first_of(*events: asyncio.Event) -> None:
    waiters = [asyncio.ensure_future(event.wait()) for event in events]
    try:
        await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for waiter in waiters:
            waiter.cancel()


def _call_soon(loop: asyncio.AbstractEventLoop, callback: Any, *args: Any) -> None:
    # The loop may already be closed if the application exited without closing us.
    with suppress(RuntimeError):
        loop.call_soon_threadsafe(callback, *args)
