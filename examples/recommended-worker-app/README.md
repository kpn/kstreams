## Recommended usage

This example shows the recommended way to organize your application.


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

Then, let's say you typed `foo` <kbd>enter</kbd> and then `bar` <kbd>enter</kbd>, on your app you should see:

```bash
showing bytes: b'foo'
showing bytes: b'bar'
```

## Note

If you plan on using this example, pay attention to the `pyproject.toml` dependencies, where
`kstreams` is pointing to the parent folder. You will have to set the latest version.
