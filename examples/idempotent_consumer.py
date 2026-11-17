"""A consumer that applies every message exactly once, on top of at-least-once delivery.

The id of each processed message is stored in the same database transaction as
the effect of processing it, so a redelivered or duplicated message is detected
and skipped. The example also shows the settling methods:

- a temporary failure is `nack()`ed and comes back with a higher `delivery_count`;
- a message that can never be processed is `reject()`ed and lands in the DLQ;
- everything else is `accept()`ed once its effect is committed.
"""

import asyncio
import os
import sqlite3
from typing import Any

from aioartemis import Client, IncomingMessage

URLS = os.environ.get("ARTEMIS_URLS", "amqp://localhost:5672,amqp://localhost:5673").split(",")
USER = os.environ.get("ARTEMIS_USER", "artemis")
PASSWORD = os.environ.get("ARTEMIS_PASSWORD", "artemis")

ADDRESS = "examples.deposits"


class InvalidMessage(Exception):
    """Can never be processed."""


class TemporaryFailure(Exception):
    """May succeed on a later delivery."""


def open_database() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE processed (message_id TEXT PRIMARY KEY)")
    db.execute("CREATE TABLE balance (account TEXT PRIMARY KEY, amount INTEGER NOT NULL)")
    return db


def apply(db: sqlite3.Connection, message: IncomingMessage) -> bool:
    """Apply a deposit once. Returns False for a message seen before."""
    body: Any = message.body
    if not isinstance(body, dict) or "account" not in body or "amount" not in body:
        raise InvalidMessage(f"not a deposit: {body!r}")
    if body.get("flaky") and message.delivery_count == 0:
        raise TemporaryFailure("downstream service unavailable")

    with db:  # one transaction: the effect and the record that it happened
        try:
            db.execute("INSERT INTO processed (message_id) VALUES (?)", (str(message.message_id),))
        except sqlite3.IntegrityError:
            return False
        db.execute(
            "INSERT INTO balance (account, amount) VALUES (?, ?) "
            "ON CONFLICT (account) DO UPDATE SET amount = amount + excluded.amount",
            (body["account"], body["amount"]),
        )
    return True


async def produce(client: Client) -> None:
    await client.send(ADDRESS, {"account": "alice", "amount": 10}, message_id="deposit-1")
    # A retried send after ConnectionLost: same id, so it is a duplicate, not a second deposit.
    await client.send(ADDRESS, {"account": "alice", "amount": 10}, message_id="deposit-1")
    await client.send(ADDRESS, {"account": "bob", "amount": 5, "flaky": True}, message_id="deposit-2")
    await client.send(ADDRESS, "garbage", message_id="deposit-3")


async def consume(client: Client, db: sqlite3.Connection) -> None:
    async with client.subscribe(ADDRESS) as deposits:
        while True:
            try:
                message = await asyncio.wait_for(deposits.get(), timeout=3)
            except TimeoutError:
                return  # nothing left to do
            try:
                applied = apply(db, message)
            except InvalidMessage as error:
                print("reject", message.message_id, error)
                await message.reject()
            except TemporaryFailure as error:
                print("nack", message.message_id, error, "delivery_count", message.delivery_count)
                await message.nack()
            else:
                print("accept", message.message_id, "applied" if applied else "duplicate, skipped")
                await message.accept()


async def main() -> None:
    db = open_database()
    async with Client(URLS, user=USER, password=PASSWORD) as client:
        await produce(client)
        await consume(client, db)
    print("balances:", dict(db.execute("SELECT account, amount FROM balance ORDER BY account")))


if __name__ == "__main__":
    asyncio.run(main())
