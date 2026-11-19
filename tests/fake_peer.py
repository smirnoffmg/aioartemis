"""An in-process AMQP peer for broker behaviour Artemis cannot be made to show on demand."""

from __future__ import annotations

import socket
import threading
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from proton import Condition, Connection, Event, Handler
from proton.handlers import MessagingHandler
from proton.reactor import ApplicationEvent, Container, EventInjector


class _Timer(Handler):
    def __init__(self, callback: Any) -> None:
        self._callback = callback

    def on_timer_task(self, event: Event) -> None:
        self._callback()


class FakePeer(MessagingHandler):
    """Accepts everything, except what a test asks it to break.

    ``close_with`` closes connections with that error condition ``close_after``
    seconds after they open: only the first one, or every one when
    ``close_every`` is set (a node that keeps dropping clients). ``refuse``
    lists addresses whose links are refused the way Artemis does it: an
    attach without a terminus, then a detach carrying the error. ``credit``
    is how many messages a client may send before it waits; 0 holds every
    send in the client's backlog.
    """

    def __init__(
        self,
        url: str,
        *,
        close_with: str | None = None,
        close_after: float = 0.3,
        close_every: bool = False,
        refuse: frozenset[str] = frozenset(),
        credit: int = 10,
    ) -> None:
        super().__init__(prefetch=credit)
        self._injector = EventInjector()
        self.url = url
        self.connections = 0
        self.open_connections: set[Connection] = set()
        self.received: list[Any] = []
        self._close_with = close_with
        self._close_after = close_after
        self._close_every = close_every
        self._refuse = refuse
        self._container: Container | None = None

    def on_start(self, event: Event) -> None:
        self._container = event.container
        event.container.selectable(self._injector)
        self._acceptor = event.container.listen(self.url)

    def on_connection_opened(self, event: Event) -> None:
        self.connections += 1
        connection = event.connection
        assert connection is not None
        self.open_connections.add(connection)
        if self._close_with and (self._close_every or self.connections == 1):
            condition = self._close_with

            def close() -> None:
                connection.condition = Condition(condition, "closed by fake peer")
                connection.close()

            if self._close_after == 0:
                close()  # the close goes out in the same batch as the open
            else:
                event.container.schedule(self._close_after, _Timer(close))

    def on_transport_closed(self, event: Event) -> None:
        if event.connection is not None:
            self.open_connections.discard(event.connection)

    def on_link_opening(self, event: Event) -> None:
        link = event.link
        assert link is not None
        if _address(link) not in self._refuse:
            link.source.copy(link.remote_source)
            link.target.copy(link.remote_target)

    def on_link_opened(self, event: Event) -> None:
        # A refused link is answered without a terminus (it was not copied
        # above), as Artemis does; the detach then carries the reason.
        link = event.link
        assert link is not None
        if _address(link) in self._refuse:
            link.condition = Condition("amqp:not-found", f"no such address: {_address(link)}")
            link.close()

    def on_message(self, event: Event) -> None:
        self.received.append(event.message.body)

    def stop(self) -> None:
        """Safe to call from any thread."""
        self._injector.trigger(ApplicationEvent("stop_peer"))

    def on_stop_peer(self, event: Event) -> None:
        self._acceptor.close()
        self._injector.close()
        assert self._container is not None
        self._container.stop()


def _address(link: Any) -> str | None:
    terminus = link.remote_source if link.is_sender else link.remote_target
    return terminus.address  # type: ignore[no-any-return]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@contextmanager
def fake_peer(**behaviour: Any) -> Generator[FakePeer, None, None]:
    address = f"127.0.0.1:{free_port()}"
    peer = FakePeer(address, **behaviour)
    container = Container(peer)
    thread = threading.Thread(target=container.run, daemon=True)
    thread.start()
    try:
        yield peer
    finally:
        peer.stop()
        thread.join(5)


def url_of(peer: FakePeer) -> str:
    return f"amqp://{peer.url}"
