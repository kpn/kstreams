# Send Many Example

Send many example with `kstreams`. When send_many is used, the partition must be supplied in advance as is is not possible to send the batch to many partitions.

## Requirements

python 3.14+, poetry, docker-compose

### Installation

```bash
poetry install
```

## Usage

1. Start the kafka cluster: From `kstreams` project root execute `./scripts/cluster/start`
2. Inside this folder execute `poetry run app`

The app publishes 5 events in batch to the `local--kstreams-send-many` topic, partition `0`. The payload is `b'Hello world {event_id}!`
The `ConsumerRecord` will then be consumed and printed by the consumer.

You should see something similar to the following logs:

```bash
❯ me@me-pc send-many % poetry run app
INFO:aiokafka.consumer.subscription_state:Updating subscribed topics to: frozenset({'local--kstreams-send-many'})
INFO:aiokafka.consumer.consumer:Subscribed to topic(s): {'local--kstreams-send-many'}
INFO:kstreams.prometheus.monitor:Starting Prometheus Monitoring started...
Batch send metadata: RecordMetadata(topic='local--kstreams-send-many', partition=0, topic_partition=TopicPartition(topic='local--kstreams-send-many', partition=0), offset=15, timestamp=-1, timestamp_type=0, log_start_offset=0)
INFO:aiokafka.consumer.group_coordinator:Discovered coordinator 1 for group send-many-group
INFO:aiokafka.consumer.group_coordinator:Revoking previously assigned partitions set() for group send-many-group
INFO:aiokafka.consumer.group_coordinator:(Re-)joining group send-many-group
INFO:aiokafka.consumer.group_coordinator:Joined group 'send-many-group' (generation 5) with member_id aiokafka-0.12.0-67e91929-bc94-4199-83bc-0ae7f2aa71f5
INFO:aiokafka.consumer.group_coordinator:Elected group leader -- performing partition assignments using roundrobin
INFO:aiokafka.consumer.group_coordinator:Successfully synced group send-many-group with generation 5
INFO:aiokafka.consumer.group_coordinator:Setting newly assigned partitions {TopicPartition(topic='local--kstreams-send-many', partition=0)} for group send-many-group
INFO:send_many.app:Event consumed: ConsumerRecord(topic='local--kstreams-send-many', partition=0, offset=15, timestamp=1763475963001, timestamp_type=0, key='0', value=b'Hello world 0!', checksum=None, serialized_key_size=1, serialized_value_size=14, headers=()) 

INFO:send_many.app:Event consumed: ConsumerRecord(topic='local--kstreams-send-many', partition=0, offset=16, timestamp=1763475963001, timestamp_type=0, key='1', value=b'Hello world 1!', checksum=None, serialized_key_size=1, serialized_value_size=14, headers=()) 

INFO:send_many.app:Event consumed: ConsumerRecord(topic='local--kstreams-send-many', partition=0, offset=17, timestamp=1763475963001, timestamp_type=0, key='2', value=b'Hello world 2!', checksum=None, serialized_key_size=1, serialized_value_size=14, headers=()) 

INFO:send_many.app:Event consumed: ConsumerRecord(topic='local--kstreams-send-many', partition=0, offset=18, timestamp=1763475963001, timestamp_type=0, key='3', value=b'Hello world 3!', checksum=None, serialized_key_size=1, serialized_value_size=14, headers=()) 

INFO:send_many.app:Event consumed: ConsumerRecord(topic='local--kstreams-send-many', partition=0, offset=19, timestamp=1763475963001, timestamp_type=0, key='4', value=b'Hello world 4!', checksum=None, serialized_key_size=1, serialized_value_size=14, headers=()) 
```

## Note

If you plan on using this example, pay attention to the `pyproject.toml` dependencies, where
`kstreams` is pointing to the parent folder. You will have to set the latest version.
