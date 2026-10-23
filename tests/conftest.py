import pytest

from unhinged import Engine


@pytest.fixture
def engine(tmp_path):
    eng = Engine(str(tmp_path / "test.db"))
    try:
        yield eng
    finally:
        eng.close()


@pytest.fixture
def anyio_backend():
    return "asyncio"


def pytest_collection_modifyitems(items):
    """Run ``async def`` tests on anyio's asyncio backend without a per-test marker."""
    import inspect

    for item in items:
        if inspect.iscoroutinefunction(getattr(item, "function", None)):
            item.add_marker(pytest.mark.anyio)
