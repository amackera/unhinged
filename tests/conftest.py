import pytest

from unhinged import Engine


@pytest.fixture
def engine(tmp_path):
    eng = Engine(str(tmp_path / "test.db"))
    try:
        yield eng
    finally:
        eng.close()
