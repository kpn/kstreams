# Send benchmark

Benchmark of `send many` vs `send by one` with `kstreams`.
## Requirements

python 3.14+, poetry, docker-compose

### Installation

```bash
poetry install
```

## Usage

1. Start the kafka cluster: From `kstreams` project root execute `./scripts/cluster/start`
2. Inside this folder execute `poetry run app`

- The app publishes 200 events in batch to the `local--kstreams-send-many` topic, to partition `0`. The payload is `b'Hello world {event_id}!`
- The app publishes 200 events (one by one) to the `local--kstreams-send-many` topic, to partition `0`. The payload is `b'Hello world {event_id}!`

The example proves that `send many` is always faster than `send by one`

You should see something similar to the following logs:

```bash
❯ me@me-pc send-benchmark % poetry run app
INFO:send_benchmark.app:Event consumed: ConsumerRecord(topic='local--kstreams-send-many', partition=0, offset=27244, timestamp=1763982004233, timestamp_type=0, key=None, value=b'Hello world 96!', checksum=None, serialized_key_size=-1, serialized_value_size=15, headers=()) 

INFO:send_benchmark.app:Event consumed: ConsumerRecord(topic='local--kstreams-send-many', partition=0, offset=27245, timestamp=1763982004233, timestamp_type=0, key=None, value=b'Hello world 97!', checksum=None, serialized_key_size=-1, serialized_value_size=15, headers=()) 

INFO:send_benchmark.app:send_many took 0.0022515058517456055 seconds
INFO:send_benchmark.app:send_by_one took 0.07924604415893555 seconds
INFO:send_benchmark.app:send_many took 0.07699453830718994 seconds less that send_by_one to send 200 events
INFO:send_benchmark.app:send many event is 35.19690792608672x faster than send by one event (3419.6907926086724%)
```

## Note

If you plan on using this example, pay attention to the `pyproject.toml` dependencies, where
`kstreams` is pointing to the parent folder. You will have to set the latest version.
