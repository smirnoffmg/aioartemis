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

from aioartemis import Client


async def main() -> None:
    urls = ["amqp://artemis-1:5672", "amqp://artemis-2:5672"]
    async with Client(urls, user="app", password="secret") as client:
        await client.send("orders", {"id": 42})

        async with client.subscribe("orders", prefetch=10) as orders:
            async for message in orders:
                print(message.body)
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
