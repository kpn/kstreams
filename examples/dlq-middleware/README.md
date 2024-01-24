#  Dead Letter Queue (DLQ) middleware Example

## Requirements

python 3.8+, poetry, docker-compose

## Installation

```bash
poetry install
```

## Explanation

This shows how the `Middleware` concept can be applied to use a `DLQ`. In this example every time that the `ValueError` exception is raised inside the `stream`, meaning that the event was nor processed, the middleware will send an event to the topic `kstreams--dlq-topic`.

## Usage

1. Start the kafka cluster: From `kstreams` project root execute `./scripts/cluster/start`
2. Inside this folder execute `poetry run app`
3. From `kstreams` project root, you can use the `./scripts/cluster/events/send` to send events to the kafka cluster. A prompt will open. Enter messages to send. The command is:
```bash
./scripts/cluster/events/send "local--hello-world"
```

Then, on the consume side, you should see something similar to the following logs:

```bash
❯ me@me-pc middleware-example % poetry run app

Group Coordinator Request failed: [Error 15] CoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] CoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] CoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] CoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] CoordinatorNotAvailableError
Consumer started
Event consumed: headers: (), payload: ConsumerRecord(topic='local--hello-world', partition=0, offset=0, timestamp=1660733921761, timestamp_type=0, key=None, value=b'boo', checksum=None, serialized_key_size=-1, serialized_value_size=3, headers=())
```

4. Consume from the topic `kstreams--dlq-topic`: `./scripts/cluster/events/read kstreams--dlq-topic`
5. Produce the event `joker` using the terminal opened in step `3`. Then check the terminal opened in the previous step and the event `joker` must appear

## Note

If you plan on using this example, pay attention to the `pyproject.toml` dependencies, where
`kstreams` is pointing to the parent folder. You will have to set the latest version.
