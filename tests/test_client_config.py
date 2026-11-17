import proton
import pytest

from aioartemis import Client


def test_amqps_without_tls_support_fails_with_instructions(monkeypatch: pytest.MonkeyPatch) -> None:
    # python-qpid-proton silently builds without TLS when the OpenSSL headers are
    # missing; amqps:// would then only fail later, on the engine thread.
    monkeypatch.setattr(proton.SSL, "present", staticmethod(lambda: False))
    with pytest.raises(RuntimeError, match="built without OpenSSL"):
        Client(["amqp://a:5672", "amqps://b:5671"])


def test_plain_amqp_does_not_need_tls_support(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(proton.SSL, "present", staticmethod(lambda: False))
    Client(["amqp://a:5672"])
