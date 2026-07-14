# Memory Backend Example

Simple example with `kstreams` using the in-memory backend.

## Requirements

python 3.14+, poetry

### Installation

```bash
poetry install
```

## Usage

Inside this folder execute:

```bash
poetry run app
```

The app starts a stream engine configured with the `InMemory` backend, publishes 5 events with the same payload (i.e. `'{"message": "Hello world!"}'`) to the `local--kstreams-test` topic, consumes them with the `example-group` consumer group, and sends each consumed value to `local--hello-world`.

You should see logs similar to the following:

```bash
INFO:memory_backend.app:Event consumed: ConsumerRecord(topic='local--kstreams-test', partition=0, offset=0, timestamp=..., timestamp_type=0, key='1', value=b'{"message": "Hello world!"}', checksum=None, serialized_key_size=1, serialized_value_size=27, headers=())

INFO:memory_backend.app:Event consumed: ConsumerRecord(topic='local--kstreams-test', partition=0, offset=1, timestamp=..., timestamp_type=0, key='1', value=b'{"message": "Hello world!"}', checksum=None, serialized_key_size=1, serialized_value_size=27, headers=())

INFO:memory_backend.app:Event consumed: ConsumerRecord(topic='local--kstreams-test', partition=0, offset=2, timestamp=..., timestamp_type=0, key='1', value=b'{"message": "Hello world!"}', checksum=None, serialized_key_size=1, serialized_value_size=27, headers=())

INFO:memory_backend.app:Event consumed: ConsumerRecord(topic='local--kstreams-test', partition=0, offset=3, timestamp=..., timestamp_type=0, key='1', value=b'{"message": "Hello world!"}', checksum=None, serialized_key_size=1, serialized_value_size=27, headers=())

INFO:memory_backend.app:Event consumed: ConsumerRecord(topic='local--kstreams-test', partition=0, offset=4, timestamp=..., timestamp_type=0, key='1', value=b'{"message": "Hello world!"}', checksum=None, serialized_key_size=1, serialized_value_size=27, headers=())
```

## Note

If you plan on using this example, pay attention to the `pyproject.toml` dependencies, where `kstreams` is pointing to the parent folder. You will have to set the latest version.
