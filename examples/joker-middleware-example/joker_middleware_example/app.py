import asyncio
import logging

import aiorun

import kstreams

logger = logging.getLogger(__name__)


class JokerBlokerMiddleware(kstreams.middleware.BaseMiddleware):
    async def __call__(self, cr: kstreams.ConsumerRecord):
        try:
            return await self.next_call(cr)
        except ValueError:
            logger.info(
                "Blocking the Joker 🤡⃤ 🤡⃤ 🤡⃤. so we can continue processing events... \n"
            )


middlewares = [kstreams.middleware.Middleware(JokerBlokerMiddleware)]
stream_engine = kstreams.create_engine(title="my-stream-engine")


@stream_engine.stream(topics=["local--hello-world"], middlewares=middlewares)
async def consume(cr: kstreams.ConsumerRecord):
    logger.info(f"Event consumed: headers: {cr.headers}, payload: {cr}")
    if cr.value == b"joker":
        raise ValueError("Stream crashed 🤡 🤡 🤡 🤡 🤡 🤡 🤡 🤡")


async def start():
    await stream_engine.start()


async def shutdown(loop: asyncio.AbstractEventLoop):
    await stream_engine.stop()


async def stop(loop: asyncio.AbstractEventLoop):
    await stream_engine.stop()


def main():
    logging.basicConfig(level=logging.INFO)
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=shutdown)
