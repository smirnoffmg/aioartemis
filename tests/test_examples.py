"""Run every example against a real node, so that they cannot rot."""

import asyncio
import importlib.util
import os
import signal
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
from litestar.testing import AsyncTestClient

from conftest import CREDENTIALS, Node

pytestmark = pytest.mark.integration

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


def run(script: str, node: Node, *, interrupt_after: float | None = None) -> str:
    env = {
        **os.environ,
        "ARTEMIS_URLS": node.url,
        "ARTEMIS_USER": CREDENTIALS["user"],
        "ARTEMIS_PASSWORD": CREDENTIALS["password"],
    }
    process = subprocess.Popen(
        [sys.executable, str(EXAMPLES / script)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        if interrupt_after is None:
            output, _ = process.communicate(timeout=60)
        else:
            try:
                output, _ = process.communicate(timeout=interrupt_after)
            except subprocess.TimeoutExpired:
                process.send_signal(signal.SIGINT)
                output, _ = process.communicate(timeout=30)
    finally:
        process.kill()
    assert process.returncode == 0, output
    return output


def test_send_and_receive(node_b: Node) -> None:
    assert run("send_and_receive.py", node_b).count("received") == 3


def test_retry_ambiguous_send(node_b: Node) -> None:
    assert "consumed" in run("retry_ambiguous_send.py", node_b)


def test_idempotent_consumer(node_b: Node) -> None:
    output = run("idempotent_consumer.py", node_b)
    assert "duplicate, skipped" in output
    assert "reject deposit-3" in output
    assert "nack deposit-2" in output
    assert "balances: {'alice': 10, 'bob': 5}" in output


def test_graceful_shutdown(node_b: Node) -> None:
    output = run("graceful_shutdown.py", node_b, interrupt_after=2.5)
    assert "interrupted, releasing" in output
    assert "stopped" in output


def test_failover(node_b: Node) -> None:
    assert f"via {node_b.url}" in run("failover.py", node_b, interrupt_after=2.5)


def test_topic_fanout(node_b: Node) -> None:
    output = run("topic_fanout.py", node_b)
    assert output.count("billing got") == 3
    assert output.count("analytics got") == 3


async def test_litestar_app(node_b: Node, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ARTEMIS_URLS", node_b.url)
    spec = importlib.util.spec_from_file_location("litestar_app", EXAMPLES / "litestar_app.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)  # Litestar resolves type hints through it
    spec.loader.exec_module(module)

    async with AsyncTestClient(app=module.app) as http:
        health = await http.get("/health")
        assert health.status_code == 200, health.text

        order = {"id": f"o-{uuid.uuid4().hex}", "item": "book", "quantity": 2}
        created = await http.post("/orders", json=order)
        assert created.status_code == 202, created.text
        assert created.json() == {"message_id": order["id"]}

        async with asyncio.timeout(10):
            while (fetched := await http.get(f"/orders/{order['id']}")).status_code == 404:
                await asyncio.sleep(0.1)
        assert fetched.json() == order
