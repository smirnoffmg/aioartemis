"""Watch the client fail over between nodes.

Start both nodes from docker-compose.yml, run this script, then stop the node
it is connected to:

    docker compose -f examples/docker-compose.yml stop artemis-1

The client moves to the other node; the `aioartemis` logger reports why. Start
the node again and stop the other one to see it move back. Ctrl+C to quit.
"""

import asyncio
import logging
import os
import signal
import uuid
from contextlib import suppress

from aioartemis import Client, ConnectionLost

URLS = os.environ.get("ARTEMIS_URLS", "amqp://localhost:5672,amqp://localhost:5673").split(",")
USER = os.environ.get("ARTEMIS_USER", "artemis")
PASSWORD = os.environ.get("ARTEMIS_PASSWORD", "artemis")


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    # A short heartbeat makes a vanished node noticed within seconds.
    async with Client(URLS, user=USER, password=PASSWORD, heartbeat=2.0, shuffle=False) as client:
        n = 0
        while not stop.is_set():
            message_id = str(uuid.uuid4())
            while True:
                try:
                    await client.send("examples.heartbeat", n, message_id=message_id)
                    break
                except ConnectionLost:
                    print("outcome unknown, retrying", message_id)
            print(f"sent {n} via {client.connected_url}")
            n += 1
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=1)


if __name__ == "__main__":
    asyncio.run(main())
