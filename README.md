# aioartemis

[![CI](https://github.com/smirnoffmg/aioartemis/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/smirnoffmg/aioartemis/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/aioartemis)](https://pypi.org/project/aioartemis/)
[![Python](https://img.shields.io/pypi/pyversions/aioartemis)](https://pypi.org/project/aioartemis/)

asyncio-native AMQP 1.0 client for [ActiveMQ Artemis](https://activemq.apache.org/components/artemis/),
built for running behind load balancers and failing over between nodes.

- Reconnects to the next node on any connection loss, with jittered exponential backoff, and re-attaches
  senders and subscriptions.
- At-least-once delivery with explicit outcomes: ambiguous sends raise `ConnectionLost` with the
  `message_id` to retry with; `accept`, `nack`, `release` and `reject` map to what Artemis does.
- Built on [Qpid Proton](https://qpid.apache.org/proton/), whose event loop runs on a thread of its own,
  so heartbeats keep flowing while your event loop is busy.

## Installation

```bash
pip install aioartemis
```

Python 3.12 or newer. aioartemis is pure Python, but its AMQP engine, `python-qpid-proton`, is published
only as a source distribution and compiles against the system's OpenSSL, so install the build
dependencies first, e.g. on Debian/Ubuntu:

```bash
apt-get install build-essential pkg-config libssl-dev
```

Without the OpenSSL headers it still installs, silently without TLS. See
[Installation](https://smirnoffmg.dev/aioartemis/#installation) for other systems and how to check.

## Usage

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

For Litestar, `pip install "aioartemis[litestar]"` adds `ArtemisPlugin`, which runs the client for the
application's lifetime, injects it into handlers and answers broker errors a retry can cure with `503`; see
[Litestar](https://smirnoffmg.dev/aioartemis/#litestar).

## Documentation

<https://smirnoffmg.dev/aioartemis/>

- [Delivery guarantees](https://smirnoffmg.dev/aioartemis/guarantees/): duplicates, ordering,
  settling messages
- [Failover and load balancers](https://smirnoffmg.dev/aioartemis/failover/): reconnects, backoff,
  limitations
- [Errors](https://smirnoffmg.dev/aioartemis/errors/): what every exception means
- [API reference](https://smirnoffmg.dev/aioartemis/api/)

## Examples

[examples/](https://github.com/smirnoffmg/aioartemis/tree/main/examples) has runnable scripts for retrying ambiguous sends, an idempotent consumer, graceful
shutdown, failover, topics and Litestar applications, with a Docker Compose file for two local nodes.

## License

Apache License 2.0, see [LICENSE](https://github.com/smirnoffmg/aioartemis/blob/main/LICENSE).

## Contributing

See [CONTRIBUTING.md](https://github.com/smirnoffmg/aioartemis/blob/main/CONTRIBUTING.md).
