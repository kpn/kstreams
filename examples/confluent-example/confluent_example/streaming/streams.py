from kstreams import ConsumerRecord

from .engine import stream_engine

deployment_topic = "local--deployment"
country_topic = "local--country"


@stream_engine.stream(deployment_topic)
async def deployment_stream(cr: ConsumerRecord):
    print(f"Event consumed on topic {deployment_topic}. The user is {cr.value}")


@stream_engine.stream(country_topic)
async def country_stream(cr: ConsumerRecord):
    print(f"Event consumed on topic {country_topic}. The Address is {cr.value}")
