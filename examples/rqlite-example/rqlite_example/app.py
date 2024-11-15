import asyncio
import logging

import aiorun

from .models import User
from .resources import Base, Session, db_engine, stream_engine
from .streams import consume

stream_engine.add_stream(consume)


@stream_engine.after_startup
async def load_data():
    user = User(
        name="spongebob",
        fullname="Spongebob Squarepants",
    )

    with Session() as session:
        session.add(user)
        session.commit()


async def start():
    Base.metadata.create_all(db_engine)
    await stream_engine.start()


async def stop(loop: asyncio.AbstractEventLoop):
    await stream_engine.stop()


def main():
    logging.basicConfig(level=logging.INFO)
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=stop)
