"""Exceptions raised by aioartemis.

Every exception here derives from `ArtemisError`. A few failures use built-in
exceptions instead, so `except ArtemisError` does not catch them: `TimeoutError`
when no node can be reached at start-up, `TypeError` for a body AMQP cannot
encode, `ValueError` for invalid arguments and `RuntimeError` for misuse of the
client's lifecycle.
"""

from typing import Any


class ArtemisError(Exception):
    """Base class of the errors this library raises."""


class ConnectionLost(ArtemisError):
    """The outcome of a send is unknown: the connection dropped (or the client was
    closed) after the message was handed to the broker but before the broker
    settled it. It may or may not have been delivered.

    Retry with the same `message_id` so that consumers can discard the duplicate.

    Attributes:
        message_id: The id of the message whose outcome is unknown.
    """

    def __init__(self, message: str, message_id: Any = None) -> None:
        super().__init__(message)
        self.message_id = message_id


class SendRejected(ArtemisError):
    """The broker refused the message; the reason is in the error text. Resending
    it unchanged will most likely fail the same way."""


class SendReleased(ArtemisError):
    """The broker did not process the message and did not deliver it; it is safe
    to send it again."""


class SendBacklogFull(ArtemisError):
    """`max_pending_sends` sends are already waiting for the broker, typically
    because no node is reachable; this one was not queued, so nothing was sent.

    Send it again later, or shed the load.
    """


class LinkRefused(ArtemisError):
    """The broker refused or detached the link for an address, for example because
    the address does not exist or access is denied.

    Pending sends to the address fail with it, and the next `send` to the address
    tries a new link. A subscription fails with it, either on entering or later
    from `Subscription.get`, and is not re-attached.
    """


class DeliveryLost(ArtemisError):
    """A received message can no longer be settled: it was already settled, or the
    connection or subscription it came over is gone, in which case the broker
    redelivers it."""


class AuthenticationFailed(ArtemisError):
    """The credentials were refused, and kept being refused for `auth_retry_window`
    if one is set: before the client ever logged in, or after it by every node.

    Raised by `async with Client(...)` and, after a login, by every pending and
    later call and by subscriptions; the client cannot be used afterwards. While
    some node has not refused the credentials since the last login, a refusing
    node is treated as a failed node instead and the client fails over past it.
    """


class ClientClosed(ArtemisError):
    """The client has been closed: raised by calls made after `Client.aclose`, by
    sends still waiting to be transmitted and by subscriptions when it closes,
    and by `async with Client(...)` when `aclose` interrupts the connecting."""


class EngineFailed(ArtemisError):
    """The thread running the AMQP engine died of an unexpected error; the client
    cannot be used any more. The error is logged on the `aioartemis` logger."""
