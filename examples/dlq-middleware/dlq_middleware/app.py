import aiorun

from kstreams import ConsumerRecord, create_engine, middleware


class DLQMiddleware(middleware.BaseMiddleware):
    def __init__(self, *, topic: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.topic = topic

    async def __call__(self, cr: ConsumerRecord):
        try:
            return await self.next_call(cr)
        except ValueError as exc:
            print(f"\n Event crashed because {str(exc)} \n")
            print(f"\n Producing event {cr.value} to DLQ topic {self.topic} \n")
            await self.send(self.topic, key=cr.key, value=cr.value)


middlewares = [
    middleware.Middleware(DLQMiddleware, topic="kstreams--dlq-topic"),
]
stream_engine = create_engine(title="my-stream-engine")


@stream_engine.stream(topics=["local--hello-world"], middlewares=middlewares)
async def consume(cr: ConsumerRecord):
    print(f"Event consumed: headers: {cr.headers}, payload: {cr}")
    if cr.value == b"joker":
        raise ValueError("🤡 🤡 🤡 🤡 🤡 🤡 🤡 🤡")
    print("Event has been proccesses")


async def start():
    await stream_engine.start()


async def shutdown(loop):
    await stream_engine.stop()


def main():
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=shutdown)
