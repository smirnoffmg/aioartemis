import asyncio
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import TypedDict

import pytest
from testcontainers.core.container import DockerContainer

from aioartemis import Client

IMAGE = "apache/activemq-artemis:2.44.0"
AMQP_PORT = 5672


class Credentials(TypedDict):
    user: str
    password: str


CREDENTIALS: Credentials = {"user": "artemis", "password": "artemis"}


@dataclass(frozen=True)
class Node:
    url: str
    stop: Callable[[], None]
    kill: Callable[[], None]
    pause: Callable[[], None]
    unpause: Callable[[], None]
    revoke_credentials: Callable[[], None]


async def wait_until_up(url: str, credentials: Credentials) -> None:
    async with asyncio.timeout(60):
        while True:
            try:
                async with Client([url], connect_timeout=2.0, **credentials):
                    return
            except TimeoutError:
                continue


def start_node(credentials: Credentials = CREDENTIALS) -> Iterator[Node]:
    container = (
        DockerContainer(IMAGE)
        .with_env("ARTEMIS_USER", credentials["user"])
        .with_env("ARTEMIS_PASSWORD", credentials["password"])
        .with_exposed_ports(AMQP_PORT)
    )
    container.start()
    killed = False

    def kill() -> None:
        nonlocal killed
        if not killed:
            killed = True
            container.stop()

    try:
        host = container.get_container_host_ip()
        url = f"amqp://{host}:{container.get_exposed_port(AMQP_PORT)}"
        asyncio.run(wait_until_up(url, credentials))
        wrapped = container.get_wrapped_container()

        def revoke_credentials() -> None:
            # login.config has reload=true: the broker rereads the file on the next login.
            users = "/var/lib/artemis-instance/etc/artemis-users.properties"
            revoked = f"echo '{credentials['user']} = revoked' > {users}"
            exit_code, output = container.exec(["sh", "-c", revoked])
            assert exit_code == 0, output

        yield Node(
            url=url,
            stop=wrapped.stop,
            kill=kill,
            pause=wrapped.pause,
            unpause=wrapped.unpause,
            revoke_credentials=revoke_credentials,
        )
    finally:
        kill()


@pytest.fixture(scope="session")
def node_b() -> Iterator[Node]:
    yield from start_node()


@pytest.fixture
def node_a() -> Iterator[Node]:
    """A fresh node per test, since failover tests kill it."""
    yield from start_node()


@pytest.fixture
def misconfigured_node() -> Iterator[Node]:
    """A node that refuses the credentials every other node accepts."""
    yield from start_node({"user": "artemis", "password": "something-else"})


@pytest.fixture
def queue_name() -> str:
    return f"test.{uuid.uuid4().hex}"
