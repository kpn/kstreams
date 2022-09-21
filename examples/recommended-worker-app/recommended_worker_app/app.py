import aiorun

from .resources import stream_engine
from .streams import consume

stream_engine.add_stream(consume)


async def start():
    await stream_engine.start()


async def shutdown(loop):
    await stream_engine.stop()


def main():
    print("Starting application...")
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=shutdown)
