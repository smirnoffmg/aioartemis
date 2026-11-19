# Failover and load balancers

Give `Client` the URL of every node, or the single URL of a load balancer in front of them. The client
keeps one connection to one node at a time.

## When the client reconnects

The client treats any of these as a lost connection and moves on to another node:

- the TCP connection drops, or no traffic arrives for `heartbeat` seconds;
- the broker closes the connection, whatever the reason it gives;
- a node does not finish opening the connection within `open_timeout` seconds.

A node whose host name does not resolve, or whose port refuses connections, is a failed attempt like
any other: the client records it in `last_error` and tries the next node.

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
credentials cannot help.

After a successful login, a node that refuses the credentials is treated as a misconfigured node: the
client logs a warning and fails over past it, trying it again in later passes. Once **every** node has
refused them since the last login, the credentials were revoked or rotated: the client fails with
`AuthenticationFailed`, which pending and later sends raise and subscriptions end with, instead of
retrying for ever. A node that cannot be reached does not count as a refusal.

A broker that authenticates against LDAP or another backend may refuse valid credentials while that
backend restarts. To ride this out, pass `auth_retry_window`: refused credentials are retried like an
unreachable node for that many seconds, counted from the start before the first login and from the
first refusal after it, and only then raise `AuthenticationFailed`.

Artemis caches a successful login for `security-invalidation-interval` (10 seconds by default), so a
node may keep accepting revoked credentials for that long.

## Behind a load balancer

- Keep `heartbeat` well below the load balancer's idle timeout, so that idle connections are not cut.
  The heartbeat is also what detects a node that disappeared without closing the connection, so a dead
  node is noticed after about `heartbeat` seconds.
- A producer and a consumer of the same queue may be connected to different nodes. Configure message
  redistribution in the Artemis cluster, or messages stay on the node they were sent to.

## Limitations

- `amqps://` URLs verify the broker's certificate chain against OpenSSL's default trust store (which
  honours `SSL_CERT_FILE` and `SSL_CERT_DIR`) and its host name against the URL; the host name is also
  sent as SNI. A client certificate and a CA file passed in code cannot be configured yet. Over plain
  `amqp://` the password is sent in clear text.
- Host names are resolved on the client's engine thread, which blocks it, timers included, until the
  resolver answers.
- A sender link is kept for every address the client has sent to, for as long as the client lives.
