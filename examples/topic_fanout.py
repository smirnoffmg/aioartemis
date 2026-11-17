"""Publish to a topic: every subscription receives every message.

With `routing="topic"` the broker keeps a queue per subscription only while it
is attached, so subscribe before publishing; messages published while a
subscriber is disconnected are not delivered to it.
"""

import asyncio
import os

from aioartemis import Client

URLS = os.environ.get("ARTEMIS_URLS", "amqp://localhost:5672,amqp://localhost:5673").split(",")
USER = os.environ.get("ARTEMIS_USER", "artemis")
PASSWORD = os.environ.get("ARTEMIS_PASSWORD", "artemis")


async def main() -> None:
    async with (
        Client(URLS, user=USER, password=PASSWORD) as client,
        client.subscribe("examples.prices", routing="topic") as billing,
        client.subscribe("examples.prices", routing="topic") as analytics,
    ):
        for price in (100, 101, 99):
            await client.send("examples.prices", price, routing="topic")

        for name, subscription in (("billing", billing), ("analytics", analytics)):
            for _ in range(3):
                message = await subscription.get()
                await message.accept()
                print(name, "got", message.body)


if __name__ == "__main__":
    asyncio.run(main())
