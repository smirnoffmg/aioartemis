"""Send a few messages to a queue and consume them."""

import asyncio
import os

from aioartemis import Client

URLS = os.environ.get("ARTEMIS_URLS", "amqp://localhost:5672,amqp://localhost:5673").split(",")
USER = os.environ.get("ARTEMIS_USER", "artemis")
PASSWORD = os.environ.get("ARTEMIS_PASSWORD", "artemis")


async def main() -> None:
    async with Client(URLS, user=USER, password=PASSWORD) as client:
        print("connected to", client.connected_url)

        for n in range(3):
            message_id = await client.send("examples.orders", {"order": n}, properties={"source": "example"})
            print("sent", n, "as", message_id)

        async with client.subscribe("examples.orders", prefetch=10) as orders:
            for _ in range(3):
                message = await orders.get()
                print("received", message.body, message.properties)
                await message.accept()


if __name__ == "__main__":
    asyncio.run(main())
