"""Retry a send whose outcome is unknown, without creating an undetectable duplicate.

`ConnectionLost` means the broker may or may not have the message. Sending it
again is right, but only with the same `message_id`, so that consumers can
recognise the second copy. `SendReleased` means the broker did not take the
message at all, so retrying it is plainly safe. `SendRejected` is not retried:
the broker refused the message and would refuse it again.
"""

import asyncio
import os
import uuid
from typing import Any

from aioartemis import Client, ConnectionLost, SendReleased

URLS = os.environ.get("ARTEMIS_URLS", "amqp://localhost:5672,amqp://localhost:5673").split(",")
USER = os.environ.get("ARTEMIS_USER", "artemis")
PASSWORD = os.environ.get("ARTEMIS_PASSWORD", "artemis")


async def send_reliably(client: Client, address: str, body: Any, *, attempts: int = 5) -> Any:
    message_id = str(uuid.uuid4())
    for attempt in range(1, attempts + 1):
        try:
            # While no node is reachable a send waits; bound how long one attempt may wait.
            async with asyncio.timeout(30):
                return await client.send(address, body, message_id=message_id)
        except (ConnectionLost, SendReleased) as error:
            if attempt == attempts:
                raise
            print(f"attempt {attempt} failed ({error}); retrying {message_id}")
            await asyncio.sleep(0.5 * attempt)
    raise AssertionError("unreachable")


async def main() -> None:
    async with Client(URLS, user=USER, password=PASSWORD) as client:
        message_id = await send_reliably(client, "examples.payments", {"amount": 100})
        print("delivered", message_id)

        async with client.subscribe("examples.payments") as payments:
            message = await payments.get()
            await message.accept()
            print("consumed", message.message_id)


if __name__ == "__main__":
    asyncio.run(main())
