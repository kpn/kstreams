# Kstreams

`kstreams` is a library/micro framework to use with `kafka`. It has simple kafka streams implementation that gives certain guarantees, see below.

![Build status](https://github.com/kpn/kstreams/actions/workflows/pr-tests.yaml/badge.svg?branch=master)
[![codecov](https://codecov.io/gh/kpn/kstreams/branch/master/graph/badge.svg?token=t7pxIPtphF)](https://codecov.io/gh/kpn/kstreams)
![python version](https://img.shields.io/badge/python-3.8%2B-yellowgreen)

---

**Documentation**: https://kpn.github.io/kstreams/

---

## Installation

```bash
pip install kstreams
```

You will need a worker, we recommend [aiorun](https://github.com/cjrh/aiorun)

```bash
pip install aiorun
```

## Usage

```python
import aiorun
from kstreams import create_engine, ConsumerRecord


stream_engine = create_engine(title="my-stream-engine")

@stream_engine.stream("local--kstream")
async def consume(cr: ConsumerRecord):
    print(f"Event consumed: headers: {cr.headers}, payload: {cr.value}")


async def produce():
    payload = b'{"message": "Hello world!"}'

    for i in range(5):
        metadata = await stream_engine.send("local--kstreams", value=payload)
        print(f"Message sent: {metadata}")


async def start():
    await stream_engine.start()
    await produce()


async def shutdown(loop):
    await stream_engine.stop()


if __name__ == "__main__":
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=shutdown)
```

## Features

- [x] Produce events
- [x] Consumer events with `Streams`
- [x] Subscribe to topics by `pattern`
- [x] `Prometheus` metrics and custom monitoring
- [x] TestClient
- [x] Custom Serialization and Deserialization
- [x] Easy to integrate with any `async` framework. No tied to any library!!
- [x] Yield events from streams
- [x] [Opentelemetry Instrumentation](https://github.com/kpn/opentelemetry-instrumentation-kstreams)
- [x] Middlewares
- [x] Hooks (on_startup, on_stop, after_startup, after_stop)
- [ ] Store (kafka streams pattern)
- [ ] Stream Join
- [ ] Windowing

## Development

This repo requires the use of [poetry](https://python-poetry.org/docs/basic-usage/) instead of pip.
*Note*: If you want to have the `virtualenv` in the same path as the project first you should run `poetry config --local virtualenvs.in-project true`

To install the dependencies just execute:

```bash
poetry install
```

Then you can activate the `virtualenv` with

```bash
poetry shell
```

Run test:

```bash
./scripts/test
```

Run code formatting with ruff:

```bash
./scripts/format
```

### Commit messages

We use [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/) for the commit message.

The use of [commitizen](https://commitizen-tools.github.io/commitizen/) is recommended. Commitizen is part of the dev dependencies.

```bash
cz commit
```

