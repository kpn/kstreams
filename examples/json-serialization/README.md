# Json serialization Example

Simple example with `kstreams`

## Requirements

python 3.10+, poetry, docker-compose

### Installation

```bash
poetry install
```

## Usage

1. Start the kafka cluster: From `kstreams` project root execute `./scripts/cluster/start`
2. Inside this folder execute `poetry run app`

raw_topic = "local--kstreams"
json_topic = "local--kstreams-json"


- The app should publish 5 `raw` events with the same payload (i.e. `'{"message": "Hello world!"}'`) to the `local--kstreams` topic
- The app should publish 5 `json` events with the same payload (i.e. `'{"message": "Hello world!"}'`) to the `local--kstreams-json`
- There are 2 Streams that will consume the events sent in the previous steps.

You should see something similar to the following logs:

```bash
❯ me@me-pc json-serialization % poetry run app
INFO:json_serialization.app:Event consumed: headers: (('content-type', b'application/json'),), value: {'message': 'Hello world!'}
INFO:json_serialization.app:Message sent: RecordMetadata(topic='local--kstreams-json', partition=0, topic_partition=TopicPartition(topic='local--kstreams-json', partition=0), offset=2, timestamp=1764162461728, timestamp_type=0, log_start_offset=0)
INFO:json_serialization.app:Event consumed: headers: (('content-type', b'application/json'),), value: {'message': 'Hello world!'}
INFO:json_serialization.app:Message sent: RecordMetadata(topic='local--kstreams-json', partition=0, topic_partition=TopicPartition(topic='local--kstreams-json', partition=0), offset=3, timestamp=1764162464741, timestamp_type=0, log_start_offset=0)
INFO:json_serialization.app:Event consumed: headers: (('content-type', b'application/json'),), value: {'message': 'Hello world!'}
INFO:json_serialization.app:Message sent: RecordMetadata(topic='local--kstreams-json', partition=0, topic_partition=TopicPartition(topic='local--kstreams-json', partition=0), offset=4, timestamp=1764162467745, timestamp_type=0, log_start_offset=0)
INFO:json_serialization.app:Event consumed: headers: (('content-type', b'application/json'),), value: {'message': 'Hello world!'}
INFO:json_serialization.app:Message sent: RecordMetadata(topic='local--kstreams', partition=0, topic_partition=TopicPartition(topic='local--kstreams', partition=0), offset=0, timestamp=1764162470757, timestamp_type=0, log_start_offset=0)
INFO:json_serialization.app:Event consumed: headers: (), value: b'Hello world!'
```

## Note

If you plan on using this example, pay attention to the `pyproject.toml` dependencies, where
`kstreams` is pointing to the parent folder. You will have to set the latest version.
