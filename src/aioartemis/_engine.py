"""Proton side of the client: everything here runs on the container's own thread.

No proton object leaves this thread: the asyncio side only ever holds plain
data and integer ids, because proton-c is not thread-safe and even dropping
the last reference to a wrapper (``pn_decref``) touches its state.
"""

from __future__ import annotations

import itertools
import logging
import os
import queue
import random
import threading
import uuid
from collections import deque
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from proton import Condition, Connection, Data, Delivery, Event, Handler, Link, Message, symbol
from proton._reactor import Task
from proton.handlers import MessagingHandler
from proton.reactor import ApplicationEvent, Container, EventInjector, LinkOption

from ._policy import Attempts, after_failed_attempt, after_lost_connection, backoff_delay, credit_to_grant
from .errors import (
    AuthenticationFailed,
    ClientClosed,
    ConnectionLost,
    DeliveryLost,
    LinkRefused,
    SendRejected,
    SendReleased,
)

log = logging.getLogger("aioartemis")

Routing = Literal["queue", "topic"]
Settlement = Literal["accept", "reject", "release", "nack"]

_FATAL_CONDITIONS = frozenset({"amqp:unauthorized-access"})


class Outcome(Protocol):
    """Settles one asyncio future from the proton thread."""

    def ok(self, value: Any = None) -> None: ...
    def fail(self, exc: BaseException) -> None: ...


@dataclass(frozen=True)
class Received:
    body: Any
    properties: dict[str, Any]
    message_id: Any
    delivery_count: int
    subscription_id: int
    delivery_id: int
    epoch: int


@dataclass(frozen=True)
class EngineConfig:
    urls: list[str]
    user: str | None
    password: str | None
    heartbeat: float | None
    open_timeout: float
    close_timeout: float
    reconnect_base: float
    reconnect_max: float


class Listener(Protocol):
    """How the engine reports to the asyncio side; called on the proton thread."""

    def connected(self, url: str, epoch: int) -> None: ...
    def disconnected(self) -> None: ...
    def fatal(self, exc: BaseException) -> None: ...
    def error(self, reason: str) -> None: ...


@dataclass
class PendingSend:
    encoded: bytes
    message_id: Any
    outcome: Outcome
    # Written by the asyncio thread on cancellation; a plain attribute store is atomic.
    abandoned: bool = False


@dataclass
class _Sender:
    address: str
    routing: Routing
    link: Link | None = None
    pending: deque[PendingSend] = field(default_factory=deque)
    in_flight: dict[str, PendingSend] = field(default_factory=dict)
    # True once the broker has granted credit on the current link; credit seen
    # before that may be stale, and a transfer sent on it is lost unsettled.
    sendable: bool = False


@dataclass
class _Subscription:
    id: int
    address: str
    routing: Routing
    prefetch: int
    deliver: Callable[[Received], None]
    fail: Callable[[BaseException], None]
    opened: Outcome | None
    link: Link | None = None
    deliveries: dict[int, Delivery] = field(default_factory=dict)


class _Routing(LinkOption):
    """Without an explicit capability Artemis auto-creates a MULTICAST address,
    turning a "queue" into a topic: messages sent before a subscriber exist are
    dropped and every subscriber gets every message."""

    def __init__(self, routing: Routing) -> None:
        self._routing = routing

    def apply(self, link: Link) -> None:
        terminus = link.target if link.is_sender else link.source
        terminus.capabilities.put_array(False, Data.SYMBOL)
        terminus.capabilities.enter()
        terminus.capabilities.put_symbol(symbol(self._routing))
        terminus.capabilities.exit()


class _Timer(Handler):
    def __init__(self, callback: Callable[[], None]) -> None:
        self._callback = callback

    def on_timer_task(self, event: Event) -> None:
        self._callback()


_RUN_COMMANDS = "run_commands"


class Engine(MessagingHandler):
    def __init__(self, config: EngineConfig, listener: Listener, *, shuffle: bool) -> None:
        super().__init__(prefetch=0, auto_accept=False)
        self._config = config
        self._listener = listener
        self._urls = list(config.urls)
        if shuffle:
            random.shuffle(self._urls)
        self._injector = EventInjector()
        self._injector_lock = threading.Lock()
        self._injector_released = False
        self._commands: queue.SimpleQueue[tuple[Callable[[], None], Outcome | None]] = queue.SimpleQueue()
        self._container: Container | None = None

        self._connection: Connection | None = None
        self._connected_url: str | None = None
        self._attempts = Attempts(tuple(self._urls))
        self._stable = False
        self._open_timer: Task | None = None
        self._stable_timer: Task | None = None
        self._retry_timer: Task | None = None
        self._wake_pending = False
        self._online = False
        self._epoch = 0
        self._closing = False
        self._fatal: BaseException | None = None

        self._senders: dict[tuple[str, Routing], _Sender] = {}
        self._subscriptions: dict[int, _Subscription] = {}
        self._by_link: dict[Link, _Sender | _Subscription] = {}
        self._delivery_ids = itertools.count()

    # -- any thread ---------------------------------------------------------------

    def submit(self, command: Callable[[], None], outcome: Outcome | None = None) -> None:
        # The lock keeps a late submit from writing into a pipe fd that
        # release_fds has closed and the OS may have handed to another file.
        with self._injector_lock:
            if self._injector_released:
                raise ClientClosed("client closed")
            self._commands.put((command, outcome))
            # The pipe is blocking and this runs on the event loop: write one
            # wake-up per batch, not one per command, or a burst of sends
            # stalls the loop whenever the proton thread is busy.
            if not self._wake_pending:
                self._wake_pending = True
                self._injector.trigger(ApplicationEvent(_RUN_COMMANDS))

    def release_fds(self) -> None:
        """EventInjector.close() leaves its pipe open; call once the container has exited."""
        with self._injector_lock:
            self._injector_released = True
            for fd in self._injector.pipe:
                os.close(fd)

    # -- commands -----------------------------------------------------------------

    def send(self, address: str, routing: Routing, item: PendingSend) -> None:
        if self._refuse_when_closing(item.outcome):
            return
        sender = self._senders.get((address, routing))
        if sender is None:
            sender = self._senders[(address, routing)] = _Sender(address, routing)
            if self._online:
                self._attach_sender(sender)
        sender.pending.append(item)
        self._flush(sender)

    def forget(self, address: str, routing: Routing, item: PendingSend) -> None:
        sender = self._senders.get((address, routing))
        if sender is not None and item in sender.pending:
            sender.pending.remove(item)

    def subscribe(self, subscription: _Subscription) -> None:
        if subscription.opened is not None and self._refuse_when_closing(subscription.opened):
            return
        self._subscriptions[subscription.id] = subscription
        if self._online:
            self._attach_subscription(subscription)

    def unsubscribe(self, subscription_id: int) -> None:
        subscription = self._subscriptions.pop(subscription_id, None)
        if subscription is None:
            return
        if subscription.opened is not None:
            subscription.opened.fail(ClientClosed("unsubscribed before the link opened"))
        if subscription.link is not None:
            self._by_link.pop(subscription.link, None)
            subscription.link.close()

    def settle_received(
        self, subscription_id: int, delivery_id: int, epoch: int, how: Settlement, outcome: Outcome
    ) -> None:
        subscription = self._subscriptions.get(subscription_id)
        if subscription is None or epoch != self._epoch or not self._online:
            raise DeliveryLost("the connection or subscription this message came over is gone")
        delivery = subscription.deliveries.pop(delivery_id, None)
        if delivery is None:
            raise DeliveryLost("the message was already settled")
        if how == "accept":
            delivery.update(Delivery.ACCEPTED)
        elif how == "reject":
            delivery.update(Delivery.REJECTED)
        elif how == "release":
            delivery.update(Delivery.RELEASED)
        else:
            # MODIFIED with delivery-failed: unlike RELEASED, Artemis counts it as a
            # delivery attempt, so max-delivery-attempts and the DLQ still apply.
            delivery.local.failed = True
            delivery.update(Delivery.MODIFIED)
        delivery.settle()
        self._top_up(subscription)
        outcome.ok()

    def shutdown(self) -> None:
        if self._closing:
            return
        self._closing = True
        self._fail_everything(ClientClosed("client closed"))
        connection = self._connection
        if self._online and connection is not None:
            connection.close()
            self._schedule(self._config.close_timeout, self._stop)
        else:
            self._stop()

    # -- proton callbacks ---------------------------------------------------------

    def on_start(self, event: Event) -> None:
        self._container = event.container
        event.container.selectable(self._injector)
        self._connect()

    def on_run_commands(self, event: Event) -> None:
        with self._injector_lock:
            self._wake_pending = False
        while True:
            try:
                command, outcome = self._commands.get_nowait()
            except queue.Empty:
                return
            try:
                command()
            except Exception as exc:
                if outcome is None:
                    raise
                outcome.fail(_detached(exc))

    def on_connection_opened(self, event: Event) -> None:
        if not _same(event.connection, self._connection):
            return
        self._cancel(self._open_timer)
        self._online = True
        self._epoch += 1
        self._connected_url = self._attempts.url
        self._stable = False
        self._stable_timer = self._schedule(self._config.reconnect_max, self._mark_stable)
        for sender in self._senders.values():
            self._attach_sender(sender)
        for subscription in self._subscriptions.values():
            self._attach_subscription(subscription)
        self._listener.connected(self._connected_url, self._epoch)

    def on_disconnected(self, event: Event) -> None:
        self._lost(event.connection, "transport closed")

    def on_connection_error(self, event: Event) -> None:
        self._lost(event.connection, f"closed by broker: {_describe(_remote_condition(event.connection))}")

    def on_connection_closing(self, event: Event) -> None:
        self._lost(event.connection, "closed by broker")

    def on_connection_closed(self, event: Event) -> None:
        if self._closing:
            self._stop()

    def on_session_error(self, event: Event) -> None:
        self._lost(event.connection, "session closed by broker")

    def on_session_closing(self, event: Event) -> None:
        self._lost(event.connection, "session closed by broker")

    def on_transport_error(self, event: Event) -> None:
        # Proton usually reports the transport closed before its error, so the
        # condition is read in _lost; overriding also stops MessagingHandler
        # from closing the connection itself on an authentication failure.
        pass

    def on_link_opened(self, event: Event) -> None:
        link = event.link
        owner = self._by_link.get(link) if link is not None else None
        if not isinstance(owner, _Subscription) or link is None:
            return
        if link.remote_source.address is None:
            return  # refused: Artemis follows up with a detach carrying the reason
        if owner.opened is not None:
            owner.opened.ok(owner.id)
            owner.opened = None
        self._top_up(owner)

    def on_link_error(self, event: Event) -> None:
        self._link_gone(event.link, LinkRefused(_describe(_remote_condition(event.link))))

    def on_link_closing(self, event: Event) -> None:
        self._link_gone(event.link, LinkRefused("link detached by broker"))

    def on_sendable(self, event: Event) -> None:
        owner = self._by_link.get(event.link) if event.link is not None else None
        if isinstance(owner, _Sender):
            owner.sendable = True
            self._flush(owner)

    def on_accepted(self, event: Event) -> None:
        self._resolve(event.delivery, None)

    def on_rejected(self, event: Event) -> None:
        delivery = event.delivery
        # Only the rejected variant of RemoteDisposition carries a condition.
        condition = getattr(delivery.remote, "condition", None) if delivery is not None else None
        self._resolve(delivery, SendRejected(_describe(condition) if condition else "rejected"))

    def on_released(self, event: Event) -> None:
        self._resolve(event.delivery, SendReleased("broker released the message"))

    def on_message(self, event: Event) -> None:
        owner = self._by_link.get(event.link) if event.link is not None else None
        delivery = event.delivery
        if not isinstance(owner, _Subscription) or delivery is None:
            return
        delivery_id = next(self._delivery_ids)
        owner.deliveries[delivery_id] = delivery
        message: Message = event.message
        owner.deliver(
            Received(
                body=message.body,
                properties=dict(message.properties or {}),
                message_id=message.id,
                delivery_count=message.delivery_count,
                subscription_id=owner.id,
                delivery_id=delivery_id,
                epoch=self._epoch,
            )
        )

    # -- connection lifecycle -----------------------------------------------------

    def _connect(self) -> None:
        assert self._container is not None
        url = self._attempts.url
        connection = self._container.connect(
            url=url,
            reconnect=False,
            user=self._config.user,
            password=self._config.password,
            heartbeat=self._config.heartbeat,
        )
        self._connection = connection
        self._open_timer = self._schedule(
            self._config.open_timeout, lambda: self._lost(connection, "open timed out")
        )

    def _lost(self, connection: Connection | None, reason: str) -> None:
        """Every way of losing a connection ends up here; events for connections
        we have already given up on are ignored."""
        if connection is None or not _same(connection, self._connection):
            return
        self._connection = None
        self._cancel(self._open_timer)
        self._cancel(self._stable_timer)
        condition = connection.transport.condition if connection.transport is not None else None
        _abort(connection)
        was_online, self._online = self._online, False
        self._drop_links(reason)
        if was_online:
            self._listener.disconnected()
        if condition is not None:
            reason = f"{reason}: {_describe(condition)}"
        if condition is not None and condition.name in _FATAL_CONDITIONS:
            if self._epoch == 0:
                # Wrong credentials before we ever got in: retrying cannot help.
                self._fatal = AuthenticationFailed(_describe(condition))
                self._fail_everything(self._fatal)
                self._listener.fatal(self._fatal)
            else:
                # Once we have logged in, a node refusing us is a misconfigured node,
                # not broken credentials, and is failed over like any other.
                log.warning("%s refused the credentials: %s", self._attempts.url, _describe(condition))
        if self._closing or self._fatal is not None:
            self._stop()
            return
        self._listener.error(f"{self._attempts.url}: {reason}")
        log.info("connection to %s lost: %s", self._attempts.url, reason)
        self._schedule_reconnect(was_online)

    def _schedule_reconnect(self, was_online: bool) -> None:
        if was_online and self._connected_url is not None:
            self._attempts = after_lost_connection(
                self._urls, self._connected_url, self._attempts, stable=self._stable
            )
        else:
            self._attempts = after_failed_attempt(self._attempts)
        delay = backoff_delay(
            self._attempts.round,
            base=self._config.reconnect_base,
            cap=self._config.reconnect_max,
            rand=random.random,
        )
        self._retry_timer = self._schedule(delay, self._connect)

    def _mark_stable(self) -> None:
        self._stable = True

    def _drop_links(self, reason: str) -> None:
        for sender in self._senders.values():
            sender.link = None
            for item in sender.in_flight.values():
                item.outcome.fail(ConnectionLost(f"outcome unknown: {reason}", item.message_id))
            sender.in_flight.clear()
        for subscription in self._subscriptions.values():
            subscription.link = None
            subscription.deliveries.clear()
        self._by_link.clear()

    def _stop(self) -> None:
        self._cancel(self._open_timer)
        self._cancel(self._stable_timer)
        self._cancel(self._retry_timer)
        if self._connection is not None:
            _abort(self._connection)
            self._connection = None
        self._injector.close()
        assert self._container is not None
        self._container.stop()

    def crash_cleanup(self, container: Container) -> None:
        """After an exception escaped the event loop: close the socket instead of
        leaving the broker holding the connection and its unacked messages.

        The engine is unhooked first so the bug that crashed it cannot fire
        again; the remaining events then only reach proton's IO handler, which
        closes the sockets."""
        container.handler = Handler()
        with suppress(Exception):
            if self._connection is not None:
                _abort(self._connection)
            self._injector.close()
            container.stop()
            while container.process():
                pass

    # -- links --------------------------------------------------------------------

    def _attach_sender(self, sender: _Sender) -> None:
        assert self._container is not None
        assert self._connection is not None
        link = self._container.create_sender(
            self._connection,
            sender.address,
            name=_link_name(sender.address),
            options=_Routing(sender.routing),
        )
        sender.link, sender.sendable = link, False
        self._by_link[link] = sender

    def _attach_subscription(self, subscription: _Subscription) -> None:
        assert self._container is not None
        assert self._connection is not None
        link = self._container.create_receiver(
            self._connection,
            subscription.address,
            name=_link_name(subscription.address),
            options=_Routing(subscription.routing),
        )
        subscription.link = link
        self._by_link[link] = subscription

    def _link_gone(self, link: Link | None, error: LinkRefused) -> None:
        owner = self._by_link.pop(link, None) if link is not None else None
        if isinstance(owner, _Sender):
            self._senders.pop((owner.address, owner.routing), None)
            for item in owner.pending:
                item.outcome.fail(error)
            for item in owner.in_flight.values():
                item.outcome.fail(ConnectionLost(f"outcome unknown: {error}", item.message_id))
        elif isinstance(owner, _Subscription):
            self._subscriptions.pop(owner.id, None)
            if owner.opened is not None:
                owner.opened.fail(error)
            else:
                owner.fail(error)

    def _flush(self, sender: _Sender) -> None:
        link = sender.link
        while link is not None and sender.sendable and sender.pending and link.credit > 0:
            item = sender.pending.popleft()
            if item.abandoned:
                continue
            delivery = link.delivery(link.delivery_tag())
            link.stream(item.encoded)
            link.advance()
            sender.in_flight[delivery.tag] = item

    def _resolve(self, delivery: Delivery | None, error: Exception | None) -> None:
        if delivery is None:
            return
        owner = self._by_link.get(delivery.link)
        item = owner.in_flight.pop(delivery.tag, None) if isinstance(owner, _Sender) else None
        if item is None:
            return
        if error is None:
            item.outcome.ok(item.message_id)
        else:
            item.outcome.fail(error)

    def _top_up(self, subscription: _Subscription) -> None:
        link = subscription.link
        if link is None:
            return
        grant = credit_to_grant(subscription.prefetch, link.credit, len(subscription.deliveries))
        if grant:
            link.flow(grant)

    # -- helpers ------------------------------------------------------------------

    def _refuse_when_closing(self, outcome: Outcome) -> bool:
        if self._fatal is not None:
            outcome.fail(self._fatal)
            return True
        if self._closing:
            outcome.fail(ClientClosed("client closed"))
            return True
        return False

    def _fail_everything(self, error: BaseException) -> None:
        for sender in self._senders.values():
            for item in sender.pending:
                item.outcome.fail(error)
            for item in sender.in_flight.values():
                item.outcome.fail(ConnectionLost(f"outcome unknown: {error}", item.message_id))
            sender.pending.clear()
            sender.in_flight.clear()
        for subscription in self._subscriptions.values():
            if subscription.opened is not None:
                subscription.opened.fail(error)
            else:
                subscription.fail(error)
        self._subscriptions.clear()

    def _schedule(self, delay: float, callback: Callable[[], None]) -> Task:
        assert self._container is not None
        return self._container.schedule(delay, _Timer(callback))

    @staticmethod
    def _cancel(task: Task | None) -> None:
        if task is not None:
            task.cancel()


def _detached(exc: BaseException) -> BaseException:
    # The traceback holds this thread's frames and, through their locals,
    # proton objects; the asyncio side must not end up owning those.
    exc.__context__ = None
    exc.__cause__ = None
    return exc.with_traceback(None)


def _same(a: Connection | None, b: Connection | None) -> bool:
    # Proton hands out a fresh wrapper per event, so identity never matches;
    # wrappers compare equal when they wrap the same C object.
    return a is not None and b is not None and a == b


def _link_name(address: str) -> str:
    # Proton's default name is container-id + address, so a second link to the
    # same address on one connection collides with the first and never attaches.
    return f"{address}-{uuid.uuid4().hex}"


def _abort(connection: Connection) -> None:
    connection.close()
    transport = connection.transport
    if transport is not None:
        transport.close_head()
        transport.close_tail()


def _remote_condition(endpoint: Connection | Link | None) -> Condition | None:
    return endpoint.remote_condition if endpoint is not None else None


def _describe(condition: Condition | None) -> str:
    if condition is None:
        return "no reason given"
    return f"{condition.name}: {condition.description}" if condition.description else str(condition.name)
