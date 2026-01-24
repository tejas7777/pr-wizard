import pytest
import fakeredis

from tests.settings import get_test_settings


@pytest.fixture
def redis_client():
    client = fakeredis.FakeRedis()
    yield client
    client.flushall()


@pytest.fixture
def test_settings():
    return get_test_settings()
