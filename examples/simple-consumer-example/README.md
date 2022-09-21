## Simple Consumer Example

Simple consumer example with `kstreams`

### Requirements

python 3.8+, poetry, docker-compose

### Installation

```bash
poetry install
```

## Usage

1. Start the kafka cluster: From `kstreams` project root execute `./scripts/cluster/start`
2. Inside this folder execute `poetry run app`
3. From `kstreams` project root, you can use the `./scripts/cluster/events/send` to send events to the kafka cluster. A prompt will open. Enter messages to send. The command is:
```bash
./scripts/cluster/events/send "local--hello-world"
```

Then, on the consume side, you should see something similar to the following logs:

```bash
❯ me@me-pc simple-consumer-example % poetry run app

Group Coordinator Request failed: [Error 15] CoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] CoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] CoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] CoordinatorNotAvailableError
Group Coordinator Request failed: [Error 15] CoordinatorNotAvailableError
Consumer started
Event consumed: headers: (), payload: ConsumerRecord(topic='local--hello-world', partition=0, offset=0, timestamp=1660733921761, timestamp_type=0, key=None, value=b'boo', checksum=None, serialized_key_size=-1, serialized_value_size=3, headers=())
```

## Note

If you plan on using this example, pay attention to the `pyproject.toml` dependencies, where
`kstreams` is pointing to the parent folder. You will have to set the latest version.
