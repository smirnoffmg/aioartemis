# aioartemis

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
[Installation](https://smirnoffmg.github.io/aioartemis/#installation) for other systems and how to check.

## Usage

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

## Documentation

<https://smirnoffmg.github.io/aioartemis/>

- [Delivery guarantees](https://smirnoffmg.github.io/aioartemis/guarantees/): duplicates, ordering,
  settling messages
- [Failover and load balancers](https://smirnoffmg.github.io/aioartemis/failover/): reconnects, backoff,
  limitations
- [Errors](https://smirnoffmg.github.io/aioartemis/errors/): what every exception means
- [API reference](https://smirnoffmg.github.io/aioartemis/api/)

## Examples

[examples/](examples/) has runnable scripts for retrying ambiguous sends, an idempotent consumer, graceful
shutdown, failover and topics, with a Docker Compose file for two local nodes.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
