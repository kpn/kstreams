import asyncio
import logging

import aiorun

from .schemas import country_schema, deployment_schema
from .streaming.streams import stream_engine

logger = logging.getLogger(__name__)

deployment_topic = "local--deployment"
country_topic = "local--country"


async def produce():
    for _ in range(2):
        metadata = await stream_engine.send(
            deployment_topic,
            value={
                "image": "confluentinc/cp-kafka",
                "replicas": 1,
                "port": 8080,
            },
            serializer_kwargs={
                "subject": "deployment",
                "schema": deployment_schema,
            },
        )
        logger.info(f"Event produced on topic {deployment_topic}. Metadata: {metadata}")

        metadata = await stream_engine.send(
            country_topic,
            value={
                "country": "Netherlands",
            },
            serializer_kwargs={
                "subject": "country",
                "schema": country_schema,
            },
        )
        logger.info(f"Event produced on topic {country_topic}. Metadata: {metadata}")

        await asyncio.sleep(3)


async def start():
    await stream_engine.start()
    await produce()


async def shutdown(loop: asyncio.AbstractEventLoop):
    await stream_engine.stop()


def main():
    logging.basicConfig(level=logging.INFO)
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=shutdown)
