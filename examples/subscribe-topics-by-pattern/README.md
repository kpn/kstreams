# Subscribe topics by pattern

In the following example we have a `Stream` that will consume from topics that match the regular expression `^local--customer-.*$`, for example
`local--customer-invoice` and `local--customer-profile`.

## Requirements

python 3.10+, poetry, docker-compose

### Installation

```bash
poetry install
```

## Usage

1. Start the kafka cluster: From `kstreams` project root execute `./scripts/cluster/start`
2. Inside this folder execute `poetry run app`

The app publishes events to the topics `local--customer-invoice` and `local--customer-profile`, then the events are consumed by the `stream` that has subscribed them using the pattern `^local--customer-.*$`.

You should see something similar to the following logs:

```bash
❯ poetry run app

INFO:aiokafka.consumer.consumer:Subscribed to topic pattern: re.compile('^local--customer-.*$')
INFO:kstreams.prometheus.monitor:Starting Prometheus Monitoring started...
INFO:aiokafka.consumer.subscription_state:Updating subscribed topics to: frozenset({'local--customer-profile', 'local--customer-invoice'})
INFO:aiokafka.consumer.group_coordinator:Discovered coordinator 1 for group topics-by-pattern-group
INFO:aiokafka.consumer.group_coordinator:Revoking previously assigned partitions set() for group topics-by-pattern-group
INFO:aiokafka.consumer.group_coordinator:(Re-)joining group topics-by-pattern-group
INFO:aiokafka.consumer.group_coordinator:Joined group 'topics-by-pattern-group' (generation 7) with member_id aiokafka-0.11.0-d4e8d901-666d-4286-8c6c-621a12b7216f
INFO:aiokafka.consumer.group_coordinator:Elected group leader -- performing partition assignments using roundrobin
INFO:aiokafka.consumer.group_coordinator:Successfully synced group topics-by-pattern-group with generation 7
INFO:aiokafka.consumer.group_coordinator:Setting newly assigned partitions {TopicPartition(topic='local--customer-profile', partition=0), TopicPartition(topic='local--customer-invoice', partition=0)} for group topics-by-pattern-group
INFO:subscribe_topics_by_pattern.app:Event b'profile-1' consumed from topic local--customer-profile
INFO:subscribe_topics_by_pattern.app:Event b'profile-1' consumed from topic local--customer-profile
INFO:subscribe_topics_by_pattern.app:Event b'invoice-1' consumed from topic local--customer-invoice
INFO:subscribe_topics_by_pattern.app:Event b'invoice-1' consumed from topic local--customer-invoice
```

## Note

If you plan on using this example, pay attention to the `pyproject.toml` dependencies, where
`kstreams` is pointing to the parent folder. You will have to set the latest version.
