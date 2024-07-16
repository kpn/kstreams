import asyncio
import logging

import aiorun

import kstreams

logger = logging.getLogger(__name__)

stream_engine = kstreams.create_engine(title="my-stream-engine")

customer_invoice_topic = "local--customer-invoice"
customer_profile_topic = "local--customer-profile"
invoice_event = b"invoice-1"
profile_event = b"profile-1"


@stream_engine.stream(
    topics="^local--customer-.*$",
    subscribe_by_pattern=True,
    group_id="topics-by-pattern-group",
)
async def stream(cr: kstreams.ConsumerRecord):
    if cr.topic == customer_invoice_topic:
        assert cr.value == invoice_event
    elif cr.topic == customer_profile_topic:
        assert cr.value == profile_event
    else:
        raise ValueError(f"Invalid topic {cr.topic}")

    logger.info(f"Event {cr.value} consumed from topic {cr.topic}")


@stream_engine.after_startup
async def produce():
    for event_number in range(0, 2):
        await stream_engine.send(customer_invoice_topic, value=invoice_event)
        await stream_engine.send(customer_profile_topic, value=profile_event)


async def start():
    await stream_engine.start()


async def stop(loop: asyncio.AbstractEventLoop):
    await stream_engine.stop()


def main():
    logging.basicConfig(level=logging.INFO)
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=stop)
