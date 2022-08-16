import signal

import aiorun

import kstreams

stream_engine = kstreams.create_engine(title="my-stream-engine")


@stream_engine.stream(topics=["local--hello-world"], group_id="example-group")
async def consume(stream):
    print("Consumer started")
    try:
        async for cr in stream:
            print(f"Event consumed: headers: {cr.headers}, payload: {cr}")
    finally:
        # Terminate the program if something fails. (aiorun will cath this signal and properly shutdown this program.)
        signal.alarm(signal.SIGTERM)


async def start():
    await stream_engine.start()


async def shutdown(loop):
    await stream_engine.stop()


def main():
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=shutdown)
