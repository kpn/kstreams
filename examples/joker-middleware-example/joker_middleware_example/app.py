import aiorun

import kstreams


class JokerBlokerMiddleware(kstreams.middleware.BaseMiddleware):
    async def __call__(self, cr: kstreams.ConsumerRecord):
        try:
            return await self.next_call(cr)
        except ValueError:
            print(
                "Blocking the Joker 🤡⃤ 🤡⃤ 🤡⃤. so we can continue processing events... \n"
            )


middlewares = [kstreams.middleware.Middleware(JokerBlokerMiddleware)]
stream_engine = kstreams.create_engine(title="my-stream-engine")


@stream_engine.stream(topics=["local--hello-world"], middlewares=middlewares)
async def consume(cr: kstreams.ConsumerRecord):
    print(f"Event consumed: headers: {cr.headers}, payload: {cr}")
    if cr.value == b"joker":
        raise ValueError("Stream crashed 🤡 🤡 🤡 🤡 🤡 🤡 🤡 🤡")


async def start():
    await stream_engine.start()


async def shutdown(loop):
    await stream_engine.stop()


def main():
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=shutdown)
