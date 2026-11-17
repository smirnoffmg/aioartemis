from typing import Any

from proton import Message

from aioartemis._engine import owned


def decoded(**fields: Any) -> Message:
    message = Message()
    message.decode(Message(**fields).encode())
    return message


def test_binary_values_are_copied_out_of_proton_buffers() -> None:
    message = decoded(body={"k": b"v", "l": [b"x"]}, properties={"p": b"bin"}, id=b"id")

    assert owned(message.body) == {"k": b"v", "l": [b"x"]}
    assert type(owned(message.body)["k"]) is bytes
    assert type(owned(message.body)["l"][0]) is bytes
    assert type(owned(message.properties)["p"]) is bytes
    assert type(owned(message.id)) is bytes


def test_binary_body_is_copied() -> None:
    message = decoded(body=b"raw")
    body = owned(message.body)
    assert type(body) is bytes
    assert body == b"raw"


def test_other_values_pass_through() -> None:
    assert owned(decoded(body={"n": 1, "s": "x"}).body) == {"n": 1, "s": "x"}
