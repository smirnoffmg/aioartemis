# Examples

Each script is self-contained. Start two local Artemis nodes and run one:

```bash
docker compose -f examples/docker-compose.yml up -d
uv run python examples/send_and_receive.py
```

In this repository `uv run` has every dependency. Elsewhere, install the released package; the Litestar
examples also need the extra and an ASGI server, and `litestar_plugin.py` needs aioartemis 0.3.0 or newer:

```bash
pip install aioartemis                          # every other example
pip install "aioartemis[litestar]>=0.3" uvicorn # the Litestar examples
```

| Script | Shows |
| --- | --- |
| [send_and_receive.py](send_and_receive.py) | Sending to a queue and consuming with `accept()` |
| [retry_ambiguous_send.py](retry_ambiguous_send.py) | Retrying after `ConnectionLost` with the same `message_id` |
| [idempotent_consumer.py](idempotent_consumer.py) | Deduplicating by `message_id` in the same transaction as the effect; `nack()` for a temporary failure, `reject()` for a message that can never be processed |
| [graceful_shutdown.py](graceful_shutdown.py) | Stopping on Ctrl+C or SIGTERM and `release()`ing the message in progress |
| [failover.py](failover.py) | Moving between nodes while you stop and start them |
| [topic_fanout.py](topic_fanout.py) | `routing="topic"`: every subscription gets every message |
| [litestar_app.py](litestar_app.py) | A Litestar app wired by hand: the client in the lifespan and injected into handlers, a background consumer, a readiness probe; broker errors a retry can cure as `503` with `Retry-After`, the rest as `500` |
| [litestar_plugin.py](litestar_plugin.py) | The same app with `ArtemisPlugin` from `aioartemis[litestar]` doing the wiring; see [Litestar](https://smirnoffmg.dev/aioartemis/#litestar) |

The scripts connect to `amqp://localhost:5672,amqp://localhost:5673` as `artemis`/`artemis`; override with
`ARTEMIS_URLS` (comma-separated), `ARTEMIS_USER` and `ARTEMIS_PASSWORD`.

The two nodes are not clustered, so a message is only visible on the node it was sent to; each script
sends and receives through one client, which keeps them on the same node. The Artemis web console is at
<http://localhost:8161> and <http://localhost:8162>.

Stop the nodes with `docker compose -f examples/docker-compose.yml down`.
