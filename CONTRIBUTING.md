# Contributing

## Setup

```bash
uv sync
pre-commit install
```

The hooks run ruff, mypy, pyright, import-linter and the fast tests on every commit.

## Checks

- `uv run ruff check .` and `uv run ruff format --check .`
- `uv run mypy`: strict, over `src` and `tests`
- `uv run pyright`: a second type checker with a newer typeshed; fails on deprecated APIs
- `uv run lint-imports`: keeps the proton engine (`_engine.py`) free of asyncio, since it runs on its
  own thread
- `uv run pytest -m "not integration"`: fast tests, including broker faults simulated by an in-process
  AMQP peer (`tests/fake_peer.py`)
- `uv run pytest`: everything; the `integration` tests start ActiveMQ Artemis nodes in Docker through
  testcontainers
- `uv run pytest tests/test_examples.py`: runs every script in `examples/` against a node, so they
  keep working; a new example needs a test there
- `uv run mkdocs serve`: the documentation site; `uv run mkdocs build --strict` must pass

## Design rules

- No proton object may be created, used or released on the asyncio thread: proton-c is not
  thread-safe, and even dropping the last reference to a wrapper touches its state. The asyncio side
  holds plain data and integer ids only; commands go to the engine through `Engine.submit`.
- Every way of losing a connection ends in `Engine._lost`.
- Reconnect arithmetic lives in pure functions in `_policy.py`, tested in `tests/test_policy.py`.

## Releasing

Publishing a GitHub Release builds the package and uploads it to PyPI through trusted publishing
([workflow](.github/workflows/publish.yml)). Configure the trusted publisher on PyPI once: workflow
`Publish to PyPI`, environment `pypi`.

The documentation is deployed to GitHub Pages on every push to `main`; in the repository settings, set
**Pages → Source** to **GitHub Actions**.
