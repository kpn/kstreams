from collections import namedtuple
from unittest.mock import AsyncMock

import pytest

from kstreams import ConsumerRecord, middleware

Middleware = namedtuple("Middleware", ["middleware", "call"])


@pytest.fixture
def dlq_middleware():
    dlq = AsyncMock()

    class DLQMiddleware(middleware.BaseMiddleware):
        async def __call__(self, cr: ConsumerRecord):
            try:
                return await self.next_call(cr)
            except ValueError:
                await dlq(cr.value)

    return Middleware(middleware=DLQMiddleware, call=dlq)


@pytest.fixture
def elastic_middleware():
    save_to_elastic = AsyncMock()

    class ElasticMiddleware(middleware.BaseMiddleware):
        async def __call__(self, cr: ConsumerRecord):
            await save_to_elastic(cr.value)
            return await self.next_call(cr)

    return Middleware(middleware=ElasticMiddleware, call=save_to_elastic)


@pytest.fixture
def s3_middleware():
    backup_to_s3 = AsyncMock()

    class S3Middleware(middleware.BaseMiddleware):
        async def __call__(self, cr: ConsumerRecord):
            await backup_to_s3(cr.value)
            return await self.next_call(cr)

    return Middleware(middleware=S3Middleware, call=backup_to_s3)
