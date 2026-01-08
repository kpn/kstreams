# Get Many Example

## Requirements

python 3.14+, poetry, docker-compose

### Installation

```bash
poetry install
```

## Usage

1. Start the kafka cluster: From `kstreams` project root execute `./scripts/cluster/start`
2. Inside this folder execute `poetry run app`

The app publishes 5 events in batch to the `local--kstreams` topic, partition `0`. The payload is `b'Hello world {event_id}!`
A stream will fetch the 5 events in batches with a timeout of `1 second`. The `ConsumerRecord` will then be consumed and printed by the consumer.

You should see something similar to the following logs:

```bash
❯ me@me-pc get-many % poetry run app
INFO:aiokafka.consumer.subscription_state:Updating subscribed topics to: frozenset({'local--kstreams'})
INFO:aiokafka.consumer.consumer:Subscribed to topic(s): {'local--kstreams'}
INFO:kstreams.prometheus.monitor:Starting Prometheus Monitoring started...
INFO:aiokafka.consumer.group_coordinator:Metadata for topic has changed from {} to {'local--kstreams': 1}. 
Event from local--kstreams: headers: (), payload: b'Hello world 0!'
Event from local--kstreams: headers: (), payload: b'Hello world 1!'
Event from local--kstreams: headers: (), payload: b'Hello world 2!'
Event from local--kstreams: headers: (), payload: b'Hello world 3!'
Event from local--kstreams: headers: (), payload: b'Hello world 4!'
```

## Note

If you plan on using this example, pay attention to the `pyproject.toml` dependencies, where
`kstreams` is pointing to the parent folder. You will have to set the latest version.
