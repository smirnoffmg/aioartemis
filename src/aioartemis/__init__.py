"""asyncio AMQP 1.0 client for ActiveMQ Artemis with failover across nodes."""

from .client import Client, IncomingMessage, Subscription
from .errors import (
    ArtemisError,
    AuthenticationFailed,
    ClientClosed,
    ConnectionLost,
    DeliveryLost,
    EngineFailed,
    LinkRefused,
    SendRejected,
    SendReleased,
)

__version__ = "0.1.1"

__all__ = [
    "ArtemisError",
    "AuthenticationFailed",
    "Client",
    "ClientClosed",
    "ConnectionLost",
    "DeliveryLost",
    "EngineFailed",
    "IncomingMessage",
    "LinkRefused",
    "SendRejected",
    "SendReleased",
    "Subscription",
    "__version__",
]
