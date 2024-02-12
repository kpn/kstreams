from schema_registry.client import AsyncSchemaRegistryClient

from confluent_example.serializers import AvroSerializer
from kstreams import create_engine

schema_registry_client = AsyncSchemaRegistryClient("http://localhost:8081")

stream_engine = create_engine(
    title="my-stream-engine",
    serializer=AvroSerializer(schema_registry_client),
)
