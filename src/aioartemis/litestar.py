"""Litestar integration: install with `pip install "aioartemis[litestar]"`."""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from typing import Any

from litestar import Litestar, Request, Response
from litestar.config.app import AppConfig
from litestar.datastructures import State
from litestar.di import Provide
from litestar.exceptions import ImproperlyConfiguredException
from litestar.plugins import InitPlugin
from litestar.status_codes import HTTP_503_SERVICE_UNAVAILABLE
from litestar.types import ExceptionHandlersMap

from .client import Client
from .errors import ArtemisError, ClientClosed, ConnectionLost, SendBacklogFull, SendReleased

__all__ = ["ArtemisPlugin"]

log = logging.getLogger(__name__)

# The plugin owns the client, which it closes only on shutdown: there,
# ClientClosed means "this instance is stopping", and another one can serve.
_TRANSIENT: tuple[type[ArtemisError], ...] = (ConnectionLost, SendReleased, SendBacklogFull, ClientClosed)


class ArtemisPlugin(InitPlugin):
    """Run a `Client` for the application's lifetime and inject it into handlers.

    The client connects on startup, so the application does not start until a
    node is reachable, up to the client's `connect_timeout`; it closes on
    shutdown. Handlers receive it as the dependency named `key`, and it is at
    `app.state.<key>` for lifespans and background tasks. The plugin's lifespan
    goes first in the list, so lifespans the application or earlier plugins
    declare run inside it: they have the client on startup and while they shut
    down. `on_startup` hooks have it too; `on_shutdown` hooks run after the
    lifespans have exited and find it closed.

    Errors that a retry can cure, raised from handlers, become
    `503 Service Unavailable` with a `Retry-After` header: `ConnectionLost`,
    `SendReleased`, `SendBacklogFull`, and `ClientClosed` while the application
    shuts down. A `ConnectionLost` response carries the `message_id` to retry
    the send with, which helps only when the HTTP caller chose that id. The
    response does not reveal broker addresses; the cause is logged as a warning
    on the `aioartemis.litestar` logger. Other broker errors (a refused message
    or link, refused credentials, a dead engine) are not retryable and are left
    to the application, which answers `500` by default.

    The plugin does not register a handler for an error the application already
    handles at the application level, under that class or one of its broker
    error base classes such as `ArtemisError`. Catch-all handlers, for
    `Exception` or `500`, do not count. Handlers on routers and controllers are
    not seen: pass `exception_handlers=False` to handle broker errors there.

    Args:
        client_factory: Returns a new, not yet entered `Client`; called on every
            startup, since a client can be entered only once.
        key: Name of the dependency and of the attribute in `app.state`.
        retry_after: Seconds in the `Retry-After` header of a `503` response.
        exception_handlers: Whether to register the `503` handlers at all.

    Raises:
        ImproperlyConfiguredException: The application already has a dependency
            named `key`.

    Example:
        ```python
        app = Litestar(
            route_handlers=[...],
            plugins=[ArtemisPlugin(lambda: Client(urls, user="app", password="secret"))],
        )
        ```
    """

    def __init__(
        self,
        client_factory: Callable[[], Client],
        *,
        key: str = "artemis",
        retry_after: int = 1,
        exception_handlers: bool = True,
    ) -> None:
        self._client_factory = client_factory
        self._key = key
        self._retry_after = retry_after
        self._exception_handlers = exception_handlers

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        if self._key in app_config.dependencies:
            raise ImproperlyConfiguredException(
                f"a dependency named {self._key!r} already exists; give ArtemisPlugin another key"
            )
        app_config.lifespan.insert(0, self._lifespan)
        app_config.dependencies[self._key] = Provide(self._provide, sync_to_thread=False)
        if self._exception_handlers:
            for error in _TRANSIENT:
                if not _handled(app_config.exception_handlers, error):
                    app_config.exception_handlers[error] = self._unavailable
        return app_config

    @asynccontextmanager
    async def _lifespan(self, app: Litestar) -> AsyncGenerator[None, None]:
        async with self._client_factory() as client:
            app.state[self._key] = client
            yield

    def _provide(self, state: State) -> Client:
        client: Client = state[self._key]
        return client

    def _unavailable(self, request: Request[Any, Any, Any], exc: ArtemisError) -> Response[dict[str, Any]]:
        log.warning("%s %s: %s: %s", request.method, request.url.path, type(exc).__name__, exc)
        body: dict[str, Any] = {"detail": "the message broker is unavailable; retry"}
        if isinstance(exc, ConnectionLost) and exc.message_id is not None:
            body["detail"] = "the message broker may or may not have the message; retry with its message_id"
            body["message_id"] = exc.message_id
        return Response(
            body,
            status_code=HTTP_503_SERVICE_UNAVAILABLE,
            headers={"Retry-After": str(self._retry_after)},
        )


def _handled(handlers: ExceptionHandlersMap, error: type[ArtemisError]) -> bool:
    return any(cls in handlers for cls in error.__mro__ if issubclass(cls, ArtemisError))
