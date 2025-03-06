# Kstreams Transaction example

This example shows how to use `kafka transactions` with `kstreams`. For this purpose we have to setup a local kafka with 3 `brokers` and `kafka-ui` to explore
the `topics offsets` and `groups lag`.

## Requirements

`python 3.9+`, `poetry`, `docker-compose`

### Usage

First run the local cluster:

```bash
./scripts/cluster/start
```

Second, you need to install the project dependencies dependencies. In a different terminal execute:

```bash
poetry install
```

Then we can run the project

```bash
poetry run app
```

You should see something similar to the following logs:

```bash
kstreams/examples/transactions via 🐳 colima is 📦 v0.1.0 via 🐍 v3.12.4 
❯ poetry run app

02/14/2025 04:40:40 PM Updating subscribed topics to: frozenset({'local--kstreams-transactional'})
02/14/2025 04:40:40 PM Subscribed to topic(s): {'local--kstreams-transactional'}
02/14/2025 04:40:40 PM Updating subscribed topics to: frozenset({'local--kstreams-json'})
02/14/2025 04:40:40 PM Subscribed to topic(s): {'local--kstreams-json'}
02/14/2025 04:40:40 PM Starting Prometheus Monitoring started...
02/14/2025 04:40:40 PM Discovered coordinator 1 for group my-group-json-data
02/14/2025 04:40:40 PM Revoking previously assigned partitions set() for group my-group-json-data
02/14/2025 04:40:40 PM (Re-)joining group my-group-json-data
02/14/2025 04:40:40 PM Discovered coordinator 2 for group my-group-raw-data
02/14/2025 04:40:40 PM Revoking previously assigned partitions set() for group my-group-raw-data
02/14/2025 04:40:40 PM (Re-)joining group my-group-raw-data
02/14/2025 04:40:40 PM Joined group 'my-group-json-data' (generation 23) with member_id aiokafka-0.12.0-1d2163ac-810d-4028-be96-0741e0589752
02/14/2025 04:40:40 PM Elected group leader -- performing partition assignments using roundrobin
02/14/2025 04:40:40 PM Joined group 'my-group-raw-data' (generation 23) with member_id aiokafka-0.12.0-fce394e0-f09e-4bfb-8133-eef590c52d7a
02/14/2025 04:40:40 PM Elected group leader -- performing partition assignments using roundrobin
02/14/2025 04:40:40 PM Successfully synced group my-group-json-data with generation 23
02/14/2025 04:40:40 PM Setting newly assigned partitions {TopicPartition(topic='local--kstreams-json', partition=0)} for group my-group-json-data
02/14/2025 04:40:40 PM Successfully synced group my-group-raw-data with generation 23
02/14/2025 04:40:40 PM Setting newly assigned partitions {TopicPartition(topic='local--kstreams-transactional', partition=0)} for group my-group-raw-data
02/14/2025 04:40:43 PM Message sent to topic local--kstreams-json 


02/14/2025 04:40:43 PM Json Event consumed with offset: 25, value: {'message': 'Hello world!'}


02/14/2025 04:40:43 PM Discovered coordinator 1 for transactional id my-transaction-id-b213d4a1-7ee8-43dd-a121-8d5305391997
02/14/2025 04:40:43 PM Discovered coordinator 1 for group id my-group-json-data
02/14/2025 04:40:43 PM Commiting offsets {TopicPartition(topic='local--kstreams-json', partition=0): 26} for group my-group-json-data
02/14/2025 04:40:43 PM Message sent inside transaction with metadata: RecordMetadata(topic='local--kstreams-transactional', partition=0, topic_partition=TopicPartition(topic='local--kstreams-transactional', partition=0), offset=50, timestamp=1739547643468, timestamp_type=0, log_start_offset=0)
Error in transaction: None
02/14/2025 04:40:43 PM Transaction with id my-transaction-id-b213d4a1-7ee8-43dd-a121-8d5305391997 committed
02/14/2025 04:40:43 PM Event consumed from topic local--kstreams-transactional with value: b'Transaction id my-transaction-id-b213d4a1-7ee8-43dd-a121-8d5305391997 from argument in coroutine \n' 


02/14/2025 04:40:46 PM Message sent to topic local--kstreams-json 


02/14/2025 04:40:46 PM Json Event consumed with offset: 26, value: {'message': 'Hello world!'}


02/14/2025 04:40:46 PM Discovered coordinator 3 for transactional id my-transaction-id-26cd219e-cb2c-4c0f-93b1-34682c5a8a43
02/14/2025 04:40:46 PM Discovered coordinator 1 for group id my-group-json-data
02/14/2025 04:40:46 PM Commiting offsets {TopicPartition(topic='local--kstreams-json', partition=0): 27} for group my-group-json-data
02/14/2025 04:40:46 PM Message sent inside transaction with metadata: RecordMetadata(topic='local--kstreams-transactional', partition=0, topic_partition=TopicPartition(topic='local--kstreams-transactional', partition=0), offset=52, timestamp=1739547646464, timestamp_type=0, log_start_offset=0)
Error in transaction: None
02/14/2025 04:40:46 PM Transaction with id my-transaction-id-26cd219e-cb2c-4c0f-93b1-34682c5a8a43 committed
02/14/2025 04:40:46 PM Event consumed from topic local--kstreams-transactional with value: b'Transaction id my-transaction-id-26cd219e-cb2c-4c0f-93b1-34682c5a8a43 from argument in coroutine \n' 


02/14/2025 04:40:49 PM Message sent to topic local--kstreams-json 


02/14/2025 04:40:49 PM Json Event consumed with offset: 27, value: {'message': 'Hello world!'}


02/14/2025 04:40:49 PM Discovered coordinator 2 for transactional id my-transaction-id-e2df5b6a-2fd9-4c14-ac98-d719ad627eb6
02/14/2025 04:40:49 PM Discovered coordinator 1 for group id my-group-json-data
02/14/2025 04:40:49 PM Commiting offsets {TopicPartition(topic='local--kstreams-json', partition=0): 28} for group my-group-json-data
02/14/2025 04:40:49 PM Message sent inside transaction with metadata: RecordMetadata(topic='local--kstreams-transactional', partition=0, topic_partition=TopicPartition(topic='local--kstreams-transactional', partition=0), offset=54, timestamp=1739547649480, timestamp_type=0, log_start_offset=0)
Error in transaction: None
02/14/2025 04:40:49 PM Transaction with id my-transaction-id-e2df5b6a-2fd9-4c14-ac98-d719ad627eb6 committed
02/14/2025 04:40:49 PM Event consumed from topic local--kstreams-transactional with value: b'Transaction id my-transaction-id-e2df5b6a-2fd9-4c14-ac98-d719ad627eb6 from argument in coroutine \n' 
```

## Note

If you plan on using this example, pay attention to the `pyproject.toml` dependencies, where
`kstreams` is pointing to the parent folder. You will have to set the latest version.
