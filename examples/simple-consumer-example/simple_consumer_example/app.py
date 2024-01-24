import aiorun

import kstreams

stream_engine = kstreams.create_engine(title="my-stream-engine")


@stream_engine.stream(topics=["local--hello-world"], group_id="example-group")
async def consume(cr: kstreams.ConsumerRecord):
    print(f"Event consumed: headers: {cr.headers}, payload: {cr}")


async def start():
    await stream_engine.start()


async def shutdown(loop):
    await stream_engine.stop()


def main():
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=shutdown)
