# Failover and load balancers

Give `Client` the URL of every node, or the single URL of a load balancer in front of them. The client
keeps one connection to one node at a time.

## When the client reconnects

The client treats any of these as a lost connection and moves on to another node:

- the TCP connection drops, or no traffic arrives for `heartbeat` seconds;
- the broker closes the connection, whatever the reason it gives;
- a node does not finish opening the connection within `open_timeout` seconds.

After reconnecting it re-attaches every sender and every subscription. A subscription that the broker
itself detached, for example because the address was deleted, has already failed with `LinkRefused` and
is not re-attached.

## In which order and how fast

- With `shuffle=True` (the default) every client shuffles `urls` once, so clients spread over the nodes.
- After losing a connection the client starts with the node **after** the one it lost.
- It tries the nodes one by one. Each full pass over all nodes that fails increases the backoff round,
  and before every attempt the client waits a random time between 0 and
  `min(reconnect_max, reconnect_base × 2^round)` seconds. The randomness (full jitter) keeps clients
  that lost the same node from reconnecting in lockstep.
- The backoff resets only after a connection stayed up for `reconnect_max` seconds. A node that accepts
  connections and drops them at once (a redirect, a node still starting) keeps clients backing off
  instead of being hammered.

## Credentials

If the credentials are refused before the client has ever logged in, entering `async with Client(...)`
raises `AuthenticationFailed`, even when other nodes would have accepted them: retrying the same
credentials cannot help. After a successful login, a node that refuses the credentials is treated as a
misconfigured node: the client logs a warning and fails over past it, trying it again in later passes.

## Behind a load balancer

- Keep `heartbeat` well below the load balancer's idle timeout, so that idle connections are not cut.
  The heartbeat is also what detects a node that disappeared without closing the connection, so a dead
  node is noticed after about `heartbeat` seconds.
- A producer and a consumer of the same queue may be connected to different nodes. Configure message
  redistribution in the Artemis cluster, or messages stay on the node they were sent to.

## Limitations

- `amqps://` URLs use proton's default TLS settings; the CA, a client certificate and SNI cannot be
  configured yet. Over plain `amqp://` the password is sent in clear text.
- Host names are resolved on the client's engine thread, which blocks it, timers included, until the
  resolver answers.
- A sender link is kept for every address the client has sent to, for as long as the client lives.
