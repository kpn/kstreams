# Kstreams

`kstreams` is a library/micro framework to use with `kafka`. It has simple kafka streams implementation that gives certain guarantees, see below.

![Build status](https://github.com/kpn/kstreams/actions/workflows/pr-tests.yml/badge.svg?branch=master)
[![codecov](https://codecov.io/gh/kpn/kstreams/branch/main/graph/badge.svg?token=t7pxIPtphF)](https://codecov.io/gh/kpn/kstreams)
![python version](https://img.shields.io/badge/python-3.7%2B-yellowgreen)


## Requirements

python 3.8+

## API documentation

## Installation

```bash
pip install kstreams
```

## Usage

```python
import asyncio
from kstreams import create_engine, Stream


stream_engine = create_engine(title="my-stream-engine")

@stream_engine.stream("dev-kpn-des--kstream")
async def consume(stream: Stream):
    for cr in stream:
        print(f"Event consumed: headers: {cr.headers}, payload: {cr.value}")


async def produce():
    payload = b'{"message": "Hello world!"}'

    for i in range(5):
        metadata = await create_engine.send("dev-kpn-des--kstreams", value=payload)
        print(f"Message sent: {metadata}")
        await asyncio.sleep(3)


async def main():
    await stream_engine.init_streaming()
    await produce()
    await stream_engine.stop_streaming()

if __name__ == "__main__":
    asyncio.run(main())
```

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

Run code linting (`black` and `isort`)

```bash
./scripts/lint
```

### Commit messages

The use of [commitizen](https://commitizen-tools.github.io/commitizen/) is recommended. Commitizen is part of the dev dependencies.

```bash
cz commit
```
