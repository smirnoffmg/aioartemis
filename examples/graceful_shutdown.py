"""Stop a consumer on Ctrl+C or SIGTERM without losing or penalising work.

The message being processed when the signal arrives is `release()`d: it goes
back to the queue without counting as a failed delivery. Messages the client
had prefetched but not handed out yet need nothing: closing the subscription
returns them to the broker.
"""

import asyncio
import os
import signal

from aioartemis import Client, Subscription

URLS = os.environ.get("ARTEMIS_URLS", "amqp://localhost:5672,amqp://localhost:5673").split(",")
USER = os.environ.get("ARTEMIS_USER", "artemis")
PASSWORD = os.environ.get("ARTEMIS_PASSWORD", "artemis")

ADDRESS = "examples.jobs"


async def consume(jobs: Subscription) -> None:
    async for message in jobs:
        print("working on", message.body)
        try:
            await asyncio.sleep(1)  # the actual work
        except asyncio.CancelledError:
            print("interrupted, releasing", message.body)
            await message.release()
            raise
        await message.accept()
        print("done", message.body)


async def main() -> None:
    async with Client(URLS, user=USER, password=PASSWORD) as client:
        for n in range(10):
            await client.send(ADDRESS, f"job-{n}")

        async with client.subscribe(ADDRESS, prefetch=5) as jobs:
            consumer = asyncio.create_task(consume(jobs))
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, consumer.cancel)
            print("consuming; press Ctrl+C to stop")
            try:
                await consumer
            except asyncio.CancelledError:
                print("stopped")


if __name__ == "__main__":
    asyncio.run(main())
