# Graceful Shutdown Example

The idea is to demostrate that a `Graceful Shutdown` is possbile when a stream crashes. In this example we have two streams, one consuming from `local--hello-world`
topic and the other one consuming from `local--kstream`.

The stream consuming from `local--kstream` has a delay of 20 seconds after an `event` is received (this is to simulate a super slow consumption process).
The stream consuming from `local--hello-world` will raise a `ValueError("error....")` exception when the event value is `error`.
If an event was send to `local--kstream` in a time `t` and later an event with the value `error` was send to `local--hello-world` in a windows of less than `20 seconds`, then the stoping program process will be delay `20 seconds - t seconds`

Example:

1. Send an event to topic `local--kstream` now
2. Send an event to topic `local--hello-world` 5 seconds after sending the event in the previous step
3. You will see that after `15 seconds` the program stops, because it must wait that the event on `step 1` is processed.

## Requirements

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
./scripts/cluster/events/send "local--kstream"
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
4. In another terminal repeat the same to send events to the other topic and send the event `error`
```bash
./scripts/cluster/events/send "local--hello-world"
```
5. Then, on the consume side, you should see something similar to the following logs:
```bash
INFO:graceful_shutdown_example.app:Event finished...
INFO:aiokafka.consumer.group_coordinator:LeaveGroup request succeeded
INFO:kstreams.streams:Stream consuming from topics ['local--kstream'] has stopped!!! 


INFO:kstreams.engine:Streams have STOPPED....
INFO:aiorun:Cancelling pending tasks.
INFO:aiorun:Running pending tasks till complete
INFO:aiorun:Waiting for executor shutdown.
INFO:aiorun:Shutting down async generators
INFO:aiorun:Closing the loop.
INFO:aiorun:Leaving. Bye!
INFO:aiorun:Reraising unhandled exception
Traceback (most recent call last):
  File "<string>", line 1, in <module>
  File "/kstreams/examples/graceful-shutdown-example/graceful_shutdown_example/app.py", line 38, in main
    aiorun.run(start(), stop_on_unhandled_errors=True, shutdown_callback=stop)
  File "/kstreams/examples/graceful-shutdown-example/.venv/lib/python3.12/site-packages/aiorun.py", line 370, in run
    raise pending_exception_to_raise
  File "/kstreams/kstreams/streams.py", line 231, in start
    await self.func_wrapper_with_typing()
  File "/kstreams/kstreams/streams.py", line 239, in func_wrapper_with_typing
    await self.func(cr)
  File "/kstreams/kstreams/middleware/middleware.py", line 80, in __call__
    raise exc
  File "/kstreams/kstreams/middleware/middleware.py", line 66, in __call__
    return await self.next_call(cr)
           ^^^^^^^^^^^^^^^^^^^^^^^^
  File "/kstreams/kstreams/streams.py", line 348, in __call__
    return await self.handler(*params)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/kstreams/examples/graceful-shutdown-example/graceful_shutdown_example/app.py", line 18, in consume
    raise ValueError("error....")
ValueError: error....
Handler: <kstreams.middleware.middleware.ExceptionMiddleware object at 0x10361dd00>
Topics: ['local--hello-world']
```

## Note

If you plan on using this example, pay attention to the `pyproject.toml` dependencies, where
`kstreams` is pointing to the parent folder. You will have to set the latest version.
