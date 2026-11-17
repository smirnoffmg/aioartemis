# Delivery guarantees

aioartemis delivers **at least once**. A message is never silently lost by the client, but it can arrive
more than once and out of order. Write consumers so that processing the same message twice is harmless.

## Where duplicates come from

1. **Redelivery of unsettled messages.** A message that was delivered but not settled when the
   connection dropped, the subscription closed or the consumer crashed is delivered again, possibly to
   another consumer.
2. **`accept()` does not wait for the broker.** It settles the message locally and queues the outcome;
   if the connection drops before the broker sees it, the broker redelivers the message. A successful
   `accept()` is not proof that the broker will not deliver the message again.
3. **Retried sends.** When the connection drops after a message was handed to the broker but before the
   broker answered, `send()` raises `ConnectionLost`: the message may or may not have arrived. If you
   send it again, it may arrive twice.

## Deduplicating

Every message sent with aioartemis carries a `message_id`, a UUID unless you pass your own. `send()`
returns it and `ConnectionLost.message_id` carries it, so a retry can reuse the same id:

```python
from aioartemis import ConnectionLost

try:
    await client.send("orders", order, message_id=order["id"])
except ConnectionLost as lost:
    await client.send("orders", order, message_id=lost.message_id)
```

On the consumer side, record the ids of processed messages in the same transaction as the effect of
processing them, and skip a message whose id is already recorded. Messages from other producers may have
no `message_id` (`None`); deduplicate those by a business key instead.

## Ordering

Order is not guaranteed. A redelivered message arrives after messages that were sent later, and
competing consumers of one queue interleave.

## Sending

`send()` returns once the broker has **accepted** the message. If the broker refuses it, `send()` raises
`SendRejected`; if the broker does not take it, `SendReleased`. See [Errors](errors.md).

While no node is reachable, sends wait in a local queue and go out once the client has reconnected. The
queue is not bounded; cancel the call, e.g. with `asyncio.timeout`, to give up on a message. Cancelling
does not recall a message that has already been handed to the broker.

## Settling received messages

A received message stays unsettled until you call one of:

| Method | Meaning | Effect in Artemis | `delivery_count` on redelivery |
| --- | --- | --- | --- |
| `accept()` | processed | removed from the queue | — |
| `nack()` | processing failed, may succeed later | redelivered; after `max-delivery-attempts` failures moved to the dead-letter address | incremented |
| `release()` | not processed, e.g. shutting down | redelivered | unchanged |
| `reject()` | can never be processed | moved to the dead-letter address at once (dropped if none is configured) | — |

`delivery_count` is the number of earlier deliveries that failed. Use `nack()` for failures: a message
that is only ever `release()`d is redelivered forever and never reaches the dead-letter address.

At most `prefetch` messages per subscription are delivered but not yet settled; the broker sends more as
you settle them.

## Queues and topics

By default an address is a **queue** (`routing="queue"`, anycast): messages sent before any consumer
exists are kept, and the consumers of one address share its messages, each message going to one of them.

With `routing="topic"` (multicast) each subscription gets every message, but the broker keeps a queue
for a subscription only while it is attached: messages published while the client is disconnected or
failing over are not delivered to it.

Routing matters most when Artemis creates the address on first use: without it, Artemis' default would
make every new address a topic.

## Messages from a lost connection

When a connection drops, messages that arrived over it but have not been handed to your code are
discarded: they can no longer be settled, and the broker redelivers them. Settling a message you did get
from a connection that has since dropped raises `DeliveryLost`.
