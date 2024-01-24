# Joker middleware Example

## Requirements

python 3.8+, poetry, docker-compose

## Installation

```bash
poetry install
```

## Explanation

This shows how the `Middleware` concept can be used with `kstreams`. In this example we simulate a bug in the code. Every time that the event `joker` is consumed the `stream` will raise the `ValueError` exception causing it to crash. This example could also be applied to a `deserialization` exception error or any other operation that might crash the stream.

Let's start with the `stream` code:

```python
@stream_engine.stream(topics=["local--hello-world"])
async def consume(cr: kstreams.ConsumerRecord):
    print(f"Event consumed: headers: {cr.headers}, payload: {cr}")
    if cr.value == b"joker":
        raise ValueError("Stream crashed 🤡 🤡 🤡 🤡 🤡 🤡 🤡 🤡")
```

If there is a joker event, then we have the following error, causing a hard crash:

```bash
Event consumed: headers: (), payload: ConsumerRecord(topic='local--hello-world', partition=0, offset=2, timestamp=1704453280808, timestamp_type=0, key=None, value=b'joker', checksum=None, serialized_key_size=-1, serialized_value_size=5, headers=())
Stream consuming from topics ['local--hello-world'] CRASHED!!! 

 Stream crashed 🤡 🤡 🤡 🤡 🤡 🤡 🤡 🤡
Traceback (most recent call last):
  File "Projects/kstreams/kstreams/middleware/middleware.py", line 60, in __call__
    await self.next_call(cr)
  File "Projects/kstreams/kstreams/streams.py", line 313, in __call__
    await self.handler(cr)
  File "Projects/kstreams/examples/middleware-example/middleware_example/app.py", line 22, in consume
    raise ValueError("Stream crashed 🤡 🤡 🤡 🤡 🤡 🤡 🤡 🤡")
ValueError: Stream crashed 🤡 🤡 🤡 🤡 🤡 🤡 🤡 🤡
```

How can we solve the problem?

- Write buisness logic inside the `stream` to handle the `ValueError` exception
- Create a new funcion that receives the `cr` as parameter. It will also handle the `ValueError` exception, then all the buiness logic can reused and the stream will be simpler
- Create a `middleware` with the logic to handle the exception and attach it to the `stream` or the `stream_engine`

For this case we will create a `middleware` just to show you the concept. In real use cases, this middleware could represent a `Dead Letter Queue` (DLQ) for example

```python
class JokerBlokerMiddleware(kstreams.middleware.BaseMiddleware):
    async def __call__(self, cr: kstreams.ConsumerRecord) -> None:
        try:
            await self.next_call(cr)
        except ValueError:
            print("Blocking the Joker 🤡⃤ 🤡⃤ 🤡⃤. so we can continue processing events... \n")

middlewares = [kstreams.middleware.Middleware(JokerBlokerMiddleware)]


@stream_engine.stream(topics=["local--hello-world"], middlewares=middlewares)
async def consume(cr: kstreams.ConsumerRecord):
    print(f"Event consumed: headers: {cr.headers}, payload: {cr}")
    if cr.value == b"joker":
        raise ValueError("Stream crashed 🤡 🤡 🤡 🤡 🤡 🤡 🤡 🤡")
```

Now that we have applied the `JokerBlokerMiddleware`, if we receive the `joker` event the `stream` will not crash!!!

```bash
Event consumed: headers: (), payload: ConsumerRecord(topic='local--hello-world', partition=0, offset=9, timestamp=1704453577436, timestamp_type=0, key=None, value=b'joker', checksum=None, serialized_key_size=-1, serialized_value_size=5, headers=())
Blocking the Joker 🤡⃤ 🤡⃤ 🤡⃤. so we can continue processing events... 

Event consumed: headers: (), payload: ConsumerRecord(topic='local--hello-world', partition=0, offset=10, timestamp=1704453596288, timestamp_type=0, key=None, value=b'batman', checksum=None, serialized_key_size=-1, serialized_value_size=6, headers=())
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
❯ me@me-pc middleware-example % poetry run app

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
