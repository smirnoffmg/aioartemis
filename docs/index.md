# aioartemis

asyncio-native AMQP 1.0 client for [ActiveMQ Artemis](https://activemq.apache.org/components/artemis/),
built for running behind load balancers and failing over between nodes.

It wraps [Qpid Proton](https://qpid.apache.org/proton/), the reference AMQP 1.0 implementation, and runs
its event loop on a thread of its own, so heartbeats keep flowing even while your event loop is busy.

## Installation

```bash
pip install aioartemis
```

Python 3.12 or newer is required. aioartemis itself is pure Python, but its AMQP engine,
`python-qpid-proton`, is published only as a source distribution: installing it compiles C code and
links it against the system's OpenSSL. Install the build dependencies first:

| System | Build dependencies |
| --- | --- |
| Debian, Ubuntu | `apt-get install build-essential pkg-config libssl-dev` |
| Alpine | `apk add build-base pkgconf openssl-dev` |
| macOS (Homebrew) | `brew install pkg-config openssl@3` |

Elsewhere you need a C compiler, `pkg-config` and the OpenSSL development headers; a Python installed
by the system package manager may also need its development headers (`python3-dev`). At run time only
the OpenSSL library is needed (`libssl3`, or `libssl3t64` on newer Debian and Ubuntu), so a Docker image
can build in one stage and run in a slimmer one.

!!! warning "Without the OpenSSL headers, TLS is silently left out"
    If `pkg-config` cannot find OpenSSL, `python-qpid-proton` still installs, but without TLS:
    `amqp://` works, `amqps://` does not, and `Client` refuses `amqps://` URLs with a `RuntimeError`.
    Check what you got:

    ```bash
    python -c "import proton; print(proton.SSL.present())"
    ```

    To fix it, install the dependencies above and rebuild without the build cache, which would otherwise
    hand back the wheel built without TLS:

    ```bash
    pip install --force-reinstall --no-cache-dir python-qpid-proton
    # or, with uv:
    uv cache clean python-qpid-proton && uv sync --reinstall-package python-qpid-proton
    ```

## Sending and receiving

```python
import asyncio
from contextlib import suppress

from aioartemis import Client, DeliveryLost


async def main() -> None:
    urls = ["amqp://artemis-1:5672", "amqp://artemis-2:5672"]
    async with Client(urls, user="app", password="secret") as client:
        await client.send("orders", {"id": 42})

        async with client.subscribe("orders", prefetch=10) as orders:
            async for message in orders:
                print(message.body)
                # The connection dropped since the message arrived: the broker
                # redelivers it, so there is nothing left to settle.
                with suppress(DeliveryLost):
                    await message.accept()


asyncio.run(main())
```

`send` returns once the broker has accepted the message. Every received message stays unsettled until
you call one of its four settling methods:

| Method | Use it when | The broker then |
| --- | --- | --- |
| `accept()` | the message was processed | removes it |
| `nack()` | processing failed, it may work later | redelivers it, counting a failed attempt |
| `release()` | you did not process it, e.g. on shutdown | redelivers it, counting nothing |
| `reject()` | the message can never be processed | moves it to the dead-letter address at once |

All four raise `DeliveryLost` when the connection the message came over has dropped since. The broker
then redelivers the message, so a consumer loop should carry on, as above, instead of letting the error
end the subscription.

## Litestar

```bash
pip install "aioartemis[litestar]"
```

`ArtemisPlugin` runs a client for the application's lifetime and injects it into handlers:

```python
from litestar import Litestar, post
from litestar.di import NamedDependency

from aioartemis import Client
from aioartemis.litestar import ArtemisPlugin


@post("/orders")
async def create_order(data: dict[str, str], artemis: NamedDependency[Client]) -> None:
    await artemis.send("orders", data, message_id=data["id"])


app = Litestar(
    route_handlers=[create_order],
    plugins=[ArtemisPlugin(lambda: Client(["amqp://artemis-1:5672"], user="app", password="secret"))],
)
```

The application does not start until a node is reachable. The client is also at `app.state.artemis`, for
your own lifespans and `on_startup` hooks; the plugin's lifespan goes first, so lifespans declared by the
application or by earlier plugins have the client on startup and while they shut down. `on_shutdown`
hooks run after the lifespans and find it closed.

| Raised from a handler | Response |
| --- | --- |
| `ConnectionLost`, `SendReleased`, `SendBacklogFull`, `ClientClosed` (during shutdown) | `503`, `Retry-After: 1`; for `ConnectionLost`, the `message_id` to retry with |
| `SendRejected`, `LinkRefused`, `AuthenticationFailed`, `EngineFailed` | left to the application: `500` by default |

A retry can cure the first group, not the second, which is a bug or misconfiguration. The `503` body names
no broker address; the cause is logged as a warning on the `aioartemis.litestar` logger. Litestar logs
unhandled exceptions only in debug mode unless `log_exceptions="always"` is set in its logging config, so
set it to see the causes of `500`s. The `message_id` helps only a caller that chose it, as the order id in
the example below does.

The plugin skips an error the application already handles at the application level, by its class or a
base class such as `ArtemisError`; handlers for `Exception` or `500` do not count. It cannot see handlers
on routers and controllers: to handle broker errors there, pass `exception_handlers=False`. A dependency
already named `artemis` is an error; pass another `key`.

[litestar_plugin.py](https://github.com/smirnoffmg/aioartemis/tree/main/examples/litestar_plugin.py) is
a complete application, and [litestar_app.py](https://github.com/smirnoffmg/aioartemis/tree/main/examples/litestar_app.py)
the same one wired by hand.

## Next

- [Delivery guarantees](guarantees.md): duplicates, ordering and what each call promises.
- [Failover and load balancers](failover.md): how the client moves between nodes.
- [Errors](errors.md): what every exception means and what to do about it.
- [API reference](api.md): every option and method.
- [Examples](https://github.com/smirnoffmg/aioartemis/tree/main/examples): runnable scripts for retries,
  idempotent consumers, graceful shutdown, failover and topics.

The client logs on the `aioartemis` logger: every lost connection at `INFO`; at `WARNING` a node that
refuses the credentials after a successful login, and an engine thread that did not stop on close; a
crashed engine thread at `ERROR`.
