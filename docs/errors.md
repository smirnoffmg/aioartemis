# Errors

Every exception the library defines derives from `ArtemisError`. A few failures use built-in
exceptions instead, so `except ArtemisError` does not catch them; they are listed at the end.

## Library errors

| Exception | Raised by | Means | What to do |
| --- | --- | --- | --- |
| `ConnectionLost` | `send` | The connection dropped, or the client closed, after the message was handed to the broker and before it answered. The message may or may not have arrived. | Retry with `message_id=error.message_id`; consumers deduplicate. |
| `SendRejected` | `send` | The broker refused the message; the reason is in the error text. | Fix the cause; resending unchanged fails again. |
| `SendReleased` | `send` | The broker did not take the message. | Safe to send again. |
| `LinkRefused` | `send`, `subscribe`, `Subscription.get` | The broker refused or detached the link for the address, e.g. it does not exist or access is denied. | For `send`: the next call tries a new link. A subscription is not re-attached: subscribe again once the cause is fixed. |
| `DeliveryLost` | `accept`, `nack`, `release`, `reject` | The message was already settled, or the connection or subscription it came over is gone. | Nothing: the broker redelivers it. |
| `AuthenticationFailed` | `async with Client(...)` | The credentials were refused before the client ever logged in. | Fix the credentials; the client cannot be used. |
| `ClientClosed` | any call after `aclose`; pending sends and subscriptions when the client closes; `async with Client(...)` interrupted by `aclose` | The client is closed. | Use a new client. |
| `EngineFailed` | any call | The engine thread died of an unexpected error, logged on the `aioartemis` logger. | Use a new client and report the bug. |

## Built-in exceptions

| Exception | Raised by | Means |
| --- | --- | --- |
| `TimeoutError` | `async with Client(...)` | No node could be reached within `connect_timeout`; the message includes the last error. |
| `TypeError` | `send` | AMQP cannot encode the body. |
| `ValueError` | `Client(...)`, `subscribe` | `urls` is empty, or `prefetch` is below 1. |
| `RuntimeError` | `async with Client(...)`, any call | The client was entered twice, or used before being entered. |
| `RuntimeError` | `Client(...)` | An `amqps://` URL was given, but python-qpid-proton was built without OpenSSL; see [Installation](index.md#installation). |

`Client.last_error` tells why the last connection attempt failed or the last connection dropped, also
while the client is reconnecting.
