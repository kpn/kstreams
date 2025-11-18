# Consume Multiple Topics Example

Consume multiple topics example with `kstreams`

## Requirements

python 3.10+, poetry, docker-compose

### Installation

```bash
poetry install
```

## Usage

1. Start the kafka cluster: From `kstreams` project root execute `./scripts/cluster/start`
2. Inside this folder execute `poetry run app`

The app should publish 5 events with the same payload (i.e. `'{"message": "Hello world from topic {topic}!"}'`) to two topics: `local--kstreams-2` and `local--hello-world`.
The `ConsumerRecord` will then be consumed and printed by the consumer.

You should see something similar to the following logs:

```bash
❯ me@me-pc simple-example % poetry run app
Topic local--kstreams-2 is not available during auto-create initialization
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Topic local--kstreams-2 is not available during auto-create initialization
Topic local--kstreams-2 is not available during auto-create initialization
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Message sent: RecordMetadata(topic='local--kstreams-2', partition=0, topic_partition=TopicPartition(topic='local--kstreams-2', partition=0), offset=0, timestamp=1703694932136, timestamp_type=0, log_start_offset=0)
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Message sent: RecordMetadata(topic='local--hello-world', partition=0, topic_partition=TopicPartition(topic='local--hello-world', partition=0), offset=0, timestamp=1703694934271, timestamp_type=0, log_start_offset=0)
Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
Message sent: RecordMetadata(topic='local--kstreams-2', partition=0, topic_partition=TopicPartition(topic='local--kstreams-2', partition=0), offset=1, timestamp=1703694935284, timestamp_type=0, log_start_offset=0)
Event consumed from topic: local--kstreams-2, headers: (), payload: b'{"message": "Hello world from topic local--kstreams-2!"}'
Message sent: RecordMetadata(topic='local--hello-world', partition=0, topic_partition=TopicPartition(topic='local--hello-world', partition=0), offset=1, timestamp=1703694936320, timestamp_type=0, log_start_offset=0)
Event consumed from topic: local--hello-world, headers: (), payload: b'{"message": "Hello world from topic local--hello-world!"}'
Message sent: RecordMetadata(topic='local--kstreams-2', partition=0, topic_partition=TopicPartition(topic='local--kstreams-2', partition=0), offset=2, timestamp=1703694937336, timestamp_type=0, log_start_offset=0)
Event consumed from topic: local--kstreams-2, headers: (), payload: b'{"message": "Hello world from topic local--kstreams-2!"}'
Message sent: RecordMetadata(topic='local--hello-world', partition=0, topic_partition=TopicPartition(topic='local--hello-world', partition=0), offset=2, timestamp=1703694938349, timestamp_type=0, log_start_offset=0)
Event consumed from topic: local--hello-world, headers: (), payload: b'{"message": "Hello world from topic local--hello-world!"}'
Message sent: RecordMetadata(topic='local--kstreams-2', partition=0, topic_partition=TopicPartition(topic='local--kstreams-2', partition=0), offset=3, timestamp=1703694939361, timestamp_type=0, log_start_offset=0)
Event consumed from topic: local--kstreams-2, headers: (), payload: b'{"message": "Hello world from topic local--kstreams-2!"}'
Message sent: RecordMetadata(topic='local--hello-world', partition=0, topic_partition=TopicPartition(topic='local--hello-world', partition=0), offset=3, timestamp=1703694940376, timestamp_type=0, log_start_offset=0)
Event consumed from topic: local--hello-world, headers: (), payload: b'{"message": "Hello world from topic local--hello-world!"}'
Message sent: RecordMetadata(topic='local--kstreams-2', partition=0, topic_partition=TopicPartition(topic='local--kstreams-2', partition=0), offset=4, timestamp=1703694941386, timestamp_type=0, log_start_offset=0)
Event consumed from topic: local--kstreams-2, headers: (), payload: b'{"message": "Hello world from topic local--kstreams-2!"}'
Message sent: RecordMetadata(topic='local--hello-world', partition=0, topic_partition=TopicPartition(topic='local--hello-world', partition=0), offset=4, timestamp=1703694942404, timestamp_type=0, log_start_offset=0)
Event consumed from topic: local--hello-world, headers: (), payload: b'{"message": "Hello world from topic local--hello-world!"}
```

## Note

If you plan on using this example, pay attention to the `pyproject.toml` dependencies, where
`kstreams` is pointing to the parent folder. You will have to set the latest version.
