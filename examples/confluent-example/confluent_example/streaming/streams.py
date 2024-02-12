from confluent_example.middlewares import ConfluentMiddlewareDeserializer
from kstreams import ConsumerRecord, middleware

from .engine import schema_registry_client, stream_engine

deployment_topic = "local--deployment"
country_topic = "local--country"

middlewares = [
    middleware.Middleware(
        ConfluentMiddlewareDeserializer, schema_registry_client=schema_registry_client
    )
]


@stream_engine.stream(deployment_topic, middlewares=middlewares)
async def deployment_stream(cr: ConsumerRecord):
    print(f"Event consumed on topic {deployment_topic}. The user is {cr.value}")


@stream_engine.stream(country_topic, middlewares=middlewares)
async def country_stream(cr: ConsumerRecord):
    print(f"Event consumed on topic {country_topic}. The Address is {cr.value}")
