# Confluent example

This example is run with [python-schema-registry-client](https://github.com/marcosschroh/python-schema-registry-client) and `kstreams`

## Requirements

python 3.8+, poetry, docker-compose

## Installation

```bash
poetry install
```

## Usage

1. Start the kafka cluster. Inside the folder `confluent-example` run: `docker-compose up`
2. Inside the `confluent-example` folder exeute `poetry run app`

## Description

### Services

The `docker-compose.yaml` contains 3 servises: `kafka`, `zookeeper` and `schema-registry-server`. The `schema-registry-server`
will store the `schemas`.

### Schemas

For this example we have two `avro schemas` that are needed for the `serialize/deserialize` process:

```python
DEPLOYMENT_AVRO_SCHEMA = {
    "type": "record",
    "namespace": "deployment",
    "name": "Deployment",
    "fields": [
        {"name": "image", "type": "string"},
        {"name": "replicas", "type": "int"},
        {"name": "port", "type": "int"},
    ],
}

COUNTRY_AVRO_SCHEMA = {
    "type": "record",
    "namespace": "country",
    "name": "Country",
    "fields": [{"name": "country", "type": "string"}],
}
```

### Serializers

We need to define custom `serializers` so the `engine` and `streams` will handle the payload properly.
In this case we use `AsyncAvroMessageSerializer` that already contains the methods to `encode/decode` events.
Because to `serialize` events we need to know the `subject` and the `schema`, we make use of `serializer_kwargs`
to provide them.

```python
# serializers.py
from kstreams import ConsumerRecord
from schema_registry.serializers import AsyncAvroMessageSerializer


class AvroSerializer(AsyncAvroMessageSerializer):

    async def serialize(self, payload: Dict, serializer_kwargs: Dict[str, str], **kwargs) -> bytes:
        """
        Serialize a payload to avro-binary using the schema and the subject
        """
        schema = serializer_kwargs["schema"]  # GET THE SCHEMA
        subject = serializer_kwargs["subject"]  # GET THE SUBJECT
        event = await self.encode_record_with_schema(subject, schema, payload)

        return event


class AvroDeserializer(AsyncAvroMessageSerializer):
    async def deserialize(
        self, consumer_record: ConsumerRecord, **kwargs
    ) -> ConsumerRecord:
        """
        Deserialize the event to a dict
        """
        data = await self.decode_message(consumer_record.value)
        consumer_record.value = data
        return consumer_record
```

Then, we inject the `serializers` in the `engine`:


```python
# engine.py
from confluent_example import serializers

from schema_registry.client import AsyncSchemaRegistryClient


client = AsyncSchemaRegistryClient("http://localhost:8081")

stream_engine = create_engine(
    title="my-stream-engine",
    serializer=serializers.AvroSerializer(client),
    deserializer=serializers.AvroDeserializer(client)
)
```

### Application

The application consist in a producer which will `produce` deployment and country events:

```python
from .streaming.streams import stream_engine
from .schemas import deployment_schema, country_schema


await stream_engine.send(
    "local--deployment",
    value={
        "image": "confluentinc/cp-kafka",
        "replicas": 1,
        "port": 8080,
    },
    serializer_kwargs={  # Using the serializer_kwargs
        "subject": "deployment",
        "schema": deployment_schema,
    },

)

await stream_engine.send(
    "local--country",
    value={
        "country": "Netherlands",
    },
    serializer_kwargs={  # Using the serializer_kwargs
        "subject": "country",
        "schema": country_schema,
    },
)
```

And there are two `streams` that will consume events from `local--deployment` and `local--country` topics:

```python
from .engine import stream_engine
from kstreams import ConsumerRecord

deployment_topic = "local--deployment"
country_topic = "local--country"


@stream_engine.stream(deployment_topic)
async def deployment_stream(cr: ConsumerRecord):
    print(f"Event consumed on topic {deployment_topic}. The user is {cr.value}")


@stream_engine.stream(country_topic)
async def country_stream(cr: ConsumerRecord):
    print(f"Event consumed on topic {country_topic}. The Address is {cr.value}")
```

## Note

If you plan on using this example, pay attention to the `pyproject.toml` dependencies, where
`kstreams` is pointing to the parent folder. You will have to set the latest version.
