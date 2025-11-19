# Recommended usage

This example shows the recommended way to organize your application and to prevent circular imports when producing events

## Requirements

python 3.10+, poetry, docker-compose

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

Then, let's say you typed `foo` <kbd>enter</kbd> and then `bar` <kbd>enter</kbd>, on your app you should see:

```bash
showing bytes: b'foo'
showing bytes: b'bar'
```

4. After the `stream` receives an event, then an event is produced to the topic `local--kstreams` using the `send` argument. To check the event execute:

```bash
./scripts/cluster/events/read "local--kstreams"
```

Then, you should see something like: `Event confirmed. b'foo'`

## Note

If you plan on using this example, pay attention to the `pyproject.toml` dependencies, where
`kstreams` is pointing to the parent folder. You will have to set the latest version.
