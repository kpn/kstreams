# Stream with Manual Commit

`Stream` example with a `manual commit` (`enable_auto_commit=False`)

## Description

This example demostrate that using the `ManualCommitRebalanceListener` rebalance listener with `manual commit` will trigger a `commit` before the `Stream` partitions
are `revoked`. This behaviour will avoid the error `CommitFailedError` and *duplicate* message delivery after a rebalance.

The `Stream` process in batches and commits every 5 events. If a rebalance is triggered before the commit is called and the partition to `commit` was revoked
then the error `CommitFailedError` should be raised if `ManualCommitRebalanceListener` is not used.

## Requirements

python 3.10+, poetry, docker-compose

## Installation

```bash
poetry install
```

## Usage

1. Start the kafka cluster: From `kstreams` project root execute `./scripts/cluster/start`
2. Inside this folder execute `poetry run app`
3. From `kstreams` project root, you can use the `./scripts/cluster/events/send` to send events to the kafka cluster. A prompt will open. Enter messages to send. The command is:

```bash
./scripts/cluster/events/send "local--hello-world"
> kstreams  # Type this!!!
```

Then, on the consume side, you should see something similar to the following logs:

```bash
❯ me@me-pc stream-with-manual-commit % poetry run app

INFO:aiokafka.consumer.subscription_state:Updating subscribed topics to: frozenset({'local--hello-world'})
INFO:aiokafka.consumer.consumer:Subscribed to topic(s): {'local--hello-world'}
INFO:aiokafka.consumer.group_coordinator:Discovered coordinator 1 for group example-group
INFO:aiokafka.consumer.group_coordinator:Revoking previously assigned partitions set() for group example-group
INFO:aiokafka.consumer.group_coordinator:(Re-)joining group example-group
INFO:aiokafka.consumer.group_coordinator:Joined group 'example-group' (generation 34) with member_id aiokafka-0.8.0-f5fac56e-71b7-41cf-9308-3363c8f82fd2
INFO:aiokafka.consumer.group_coordinator:Elected group leader -- performing partition assignments using roundrobin
INFO:aiokafka.consumer.group_coordinator:Successfully synced group example-group with generation 34
INFO:aiokafka.consumer.group_coordinator:Setting newly assigned partitions {TopicPartition(topic='local--hello-world', partition=0)} for group example-group

Event consumed: headers: (), payload: ConsumerRecord(topic='local--hello-world', partition=0, offset=21, timestamp=1677506271687, timestamp_type=0, key=None, value=b'kstream', checksum=None, serialized_key_size=-1, serialized_value_size=7, headers=())
```

Then if you run the same program in a different terminal you should see that the `commit` is called for you before the partitions are revoked.

```bash
INFO:kstreams.rebalance_listener: Manual commit enabled for stream <kstreams.streams.Stream object at 0x1073e7a50>. Performing `commit` before revoking partitions
```

## Note

If you plan on using this example, pay attention to the `pyproject.toml` dependencies, where
`kstreams` is pointing to the parent folder. You will have to set the latest version.
