import pytest
import pytest_asyncio
import fakeredis.aioredis as fake_aioredis
from fluxmesh.storage.broker import FluxBroker
from fluxmesh.api.routes import set_broker


@pytest_asyncio.fixture
async def broker():
    # Fresh broker with isolated fake redis
    b = FluxBroker(use_fakeredis=True)
    b.redis = fake_aioredis.FakeRedis(decode_responses=True)
    set_broker(b)
    yield b
    await b.close()
