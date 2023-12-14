# Simple Example

Simple example with `kstreams`

## Requirements

python 3.8+, poetry, docker-compose

### Installation

```bash
poetry install
```

## Usage

1. Start the kafka cluster: From `kstreams` project root execute `./scripts/cluster/start`
2. Inside this folder execute `poetry run app`

The app should publish 5 events with the same payload (i.e. `'{"message": "Hello world!"}'`) to the `local--kstreams-test` topic.
The `ConsumerRecord` will then be consumed and printed by the consumer.

You should see something similar to the following logs:

```bash
❯ me@me-pc simple-example % poetry run app
Topic local--kstreams-test is not available during auto-create initialization
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
ConsumerRecord(topic='local--kstreams-test', partition=0, offset=1, timestamp=1702563120133, timestamp_type=0, key='1', value=b'{"message": "Hello world!"}', checksum=None, serialized_key_size=1, serialized_value_size=27, headers=())
ConsumerRecord(topic='local--kstreams-test', partition=0, offset=2, timestamp=1702563122143, timestamp_type=0, key='1', value=b'{"message": "Hello world!"}', checksum=None, serialized_key_size=1, serialized_value_size=27, headers=())
ConsumerRecord(topic='local--kstreams-test', partition=0, offset=3, timestamp=1702563124159, timestamp_type=0, key='1', value=b'{"message": "Hello world!"}', checksum=None, serialized_key_size=1, serialized_value_size=27, headers=())
ConsumerRecord(topic='local--kstreams-test', partition=0, offset=4, timestamp=1702563126172, timestamp_type=0, key='1', value=b'{"message": "Hello world!"}', checksum=None, serialized_key_size=1, serialized_value_size=27, headers=())
ConsumerRecord(topic='local--kstreams-test', partition=0, offset=4, timestamp=1702563128185, timestamp_type=0, key='1', value=b'{"message": "Hello world!"}', checksum=None, serialized_key_size=1, serialized_value_size=27, headers=())
```

## Note

If you plan on using this example, pay attention to the `pyproject.toml` dependencies, where
`kstreams` is pointing to the parent folder. You will have to set the latest version.
