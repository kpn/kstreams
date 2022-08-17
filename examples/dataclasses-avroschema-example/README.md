## Example with dataclasses-avroschema

[dataclasses-avroschema](https://github.com/marcosschroh/dataclasses-avroschema) example with `kstreams`

### Requirements

python 3.8+, poetry, docker-compose

### Installation

```bash
poetry install
```

## Usage

1. Start the kafka cluster: From `kstreams` project root execute `./scripts/cluster/start/`
2. Inside the `dataclasses-avroschema-example` folder exeute `poetry run app`

## Description

In this example we want to stream instances of `models.User` and `models.Address` using `avro-schemas`:

```python
# models.py
@dataclass
class User(AvroModel):
    "An User"
    name: str
    age: int
    pets: typing.List[str]
    accounts: typing.Dict[str, int]
    country: str = "Argentina"
    address: str = None

    class Meta:
        namespace = "User.v1"
        aliases = ["user-v1", "super user",]


@dataclass
class Address(AvroModel):
    "An Address"
    street: str
    street_number: int

    class Meta:
        namespace = "Address.v1"
        aliases = ["address-v1",]
```

To accomplish it, we need to define custom `serializers` for our `models`:

```python
# serializers.py
class AvroSerializer:
    async def serialize(self, instance: AvroModel, **kwargs) -> bytes:
        """
        Serialize an AvroModel to avro-binary
        """
        return instance.serialize()


class AvroDeserializer:
    def __init__(self, *, model: AvroModel) -> None:
        self.model = model

    async def deserialize(
        self, consumer_record: aiokafka.structs.ConsumerRecord, **kwargs
    ) -> aiokafka.structs.ConsumerRecord:
        """
        Deserialize a payload to an AvroModel
        """
        data = self.model.deserialize(consumer_record.value)
        consumer_record.value = data
        return consumer_record

```

Then, we inject the `serializers` in the `engine` and `streams` and we are ready to go


```python
# app.py
stream_engine = create_engine(
    title="my-stream-engine",
    serializer=serializers.AvroSerializer(),
)


@stream_engine.stream(
    user_topic, deserializer=serializers.AvroDeserializer(model=User)
)
async def user_stream(stream: Stream):
    async for cr in stream:
        print(f"Event consumed on topic {user_topic}. The user is {cr.value}")


@stream_engine.stream(
    address_topic, deserializer=serializers.AvroDeserializer(model=Address)
)
async def address_stream(stream: Stream):
    async for cr in stream:
        print(f"Event consumed on topic {address_topic}. The Address is {cr.value}")
```

## Note

If you plan on using this example, pay attention to the `pyproject.toml` dependencies, where
`kstreams` is pointing to the parent folder. You will have to set the latest version.
