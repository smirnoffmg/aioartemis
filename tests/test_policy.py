import pytest

from aioartemis._policy import (
    Attempts,
    after_failed_attempt,
    after_lost_connection,
    backoff_delay,
    credit_to_grant,
    failover_order,
    refused_login_is_fatal,
)


@pytest.mark.parametrize(
    ("prefetch", "credit", "unacked", "expected"),
    [
        (10, 0, 0, 10),
        (10, 3, 2, 5),
        (10, 10, 0, 0),
        (10, 9, 5, 0),
    ],
)
def test_credit_to_grant(prefetch: int, credit: int, unacked: int, expected: int) -> None:
    assert credit_to_grant(prefetch, credit, unacked) == expected


def test_failover_order_starts_after_the_node_that_failed() -> None:
    assert failover_order(["a", "b", "c"], start=1) == ["b", "c", "a"]
    assert failover_order(["a", "b", "c"], start=3) == ["a", "b", "c"]


def test_first_round_is_only_lightly_jittered() -> None:
    assert backoff_delay(0, base=0.1, cap=10.0, rand=lambda: 1.0) == pytest.approx(0.1)
    assert backoff_delay(0, base=0.1, cap=10.0, rand=lambda: 0.0) == 0.0


@pytest.mark.parametrize(("round_", "expected"), [(1, 0.2), (2, 0.4), (5, 3.2), (20, 10.0)])
def test_backoff_grows_exponentially_up_to_cap(round_: int, expected: float) -> None:
    assert backoff_delay(round_, base=0.1, cap=10.0, rand=lambda: 1.0) == pytest.approx(expected)


def test_backoff_is_jittered_so_clients_do_not_reconnect_in_lockstep() -> None:
    assert backoff_delay(5, base=0.1, cap=10.0, rand=lambda: 0.25) == pytest.approx(0.8)


def test_failed_attempts_walk_the_nodes_then_start_a_new_round() -> None:
    attempts = Attempts(("a", "b"))
    assert attempts.url == "a"

    attempts = after_failed_attempt(attempts)
    assert (attempts.url, attempts.round) == ("b", 0)

    attempts = after_failed_attempt(attempts)
    assert (attempts.url, attempts.round) == ("a", 1)


def test_losing_a_stable_connection_retries_the_next_node_at_once() -> None:
    attempts = Attempts(("a", "b", "c"), index=4, round=3)
    after = after_lost_connection(["a", "b", "c"], "b", attempts, stable=True)
    assert after == Attempts(("c", "a", "b"), index=0, round=0)


def test_losing_a_connection_right_after_it_opened_keeps_backing_off() -> None:
    attempts = Attempts(("a",), index=0, round=3)
    after = after_lost_connection(["a"], "a", attempts, stable=False)
    assert after.round == 4, "a node that drops every client must not be hammered"


def test_backoff_never_overflows_however_long_every_node_stays_down() -> None:
    assert backoff_delay(10_000, base=0.1, cap=10.0, rand=lambda: 1.0) == pytest.approx(10.0)


def test_refused_login_after_the_first_connection_is_failed_over_while_a_node_may_accept() -> None:
    assert not refused_login_is_fatal(
        ever_connected=True, refused_by_every_node=False, elapsed=1000.0, retry_window=None
    )


def test_refused_login_by_every_node_after_the_first_connection_is_fatal() -> None:
    # Revoked or rotated credentials: retrying them forever would hang every send.
    assert refused_login_is_fatal(
        ever_connected=True, refused_by_every_node=True, elapsed=0.0, retry_window=None
    )


@pytest.mark.parametrize(("elapsed", "fatal"), [(29.9, False), (30.0, True)])
def test_refusal_by_every_node_is_ridden_out_within_the_window(elapsed: float, fatal: bool) -> None:
    assert (
        refused_login_is_fatal(
            ever_connected=True, refused_by_every_node=True, elapsed=elapsed, retry_window=30.0
        )
        is fatal
    )


def test_refused_first_login_is_fatal_at_once_by_default() -> None:
    assert refused_login_is_fatal(
        ever_connected=False, refused_by_every_node=False, elapsed=0.0, retry_window=None
    )


@pytest.mark.parametrize(("elapsed", "fatal"), [(0.0, False), (29.9, False), (30.0, True), (31.0, True)])
def test_refused_first_login_is_retried_within_the_window(elapsed: float, fatal: bool) -> None:
    assert (
        refused_login_is_fatal(
            ever_connected=False, refused_by_every_node=False, elapsed=elapsed, retry_window=30.0
        )
        is fatal
    )
