from collections.abc import Callable
from dataclasses import dataclass, replace


def credit_to_grant(prefetch: int, credit: int, unacked: int) -> int:
    return max(0, prefetch - credit - unacked)


def failover_order(urls: list[str], start: int) -> list[str]:
    start %= len(urls)
    return urls[start:] + urls[:start]


def backoff_delay(round_: int, *, base: float, cap: float, rand: Callable[[], float]) -> float:
    """Full jitter, including the first round: clients that lost the same node
    spread their retries out instead of stampeding the next one in lockstep."""
    return rand() * min(cap, base * 2.0**round_)


@dataclass(frozen=True)
class Attempts:
    """Where the reconnect loop is: which node to try next and how many full
    passes over the nodes have failed (the backoff round)."""

    order: tuple[str, ...]
    index: int = 0
    round: int = 0

    @property
    def url(self) -> str:
        return self.order[self.index % len(self.order)]


def after_failed_attempt(attempts: Attempts) -> Attempts:
    index = attempts.index + 1
    wrapped = index % len(attempts.order) == 0
    return replace(attempts, index=index, round=attempts.round + wrapped)


def after_lost_connection(urls: list[str], connected: str, attempts: Attempts, *, stable: bool) -> Attempts:
    """Start with the node after the one that was lost. Only a connection that
    stayed up for a while resets the backoff: one that opens and drops at once
    (a redirect, a node still starting) must keep backing off, or every client
    reconnects to it in a tight loop."""
    order = tuple(failover_order(urls, urls.index(connected) + 1))
    return Attempts(order, index=0, round=0 if stable else attempts.round + 1)
