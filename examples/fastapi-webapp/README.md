# FastAPI webapp example

Simple [`FastAPI`](https://fastapi.tiangolo.com/) example with `kstreams`

## Requirements

python 3.10+, poetry, docker-compose

## Usage

1. Start the kafka cluster: From `kstreams` project root execute `./scripts/cluster/start`
2. Run `cd examples/fastapi-webapp` and execute `poetry install`
3. Run `poetry run app`

Then you should see something similar to the following logs:

```bash
❯ python -m fastapi_example
INFO:     Will watch for changes in these directories: ['/Users/user/Projects/kstreams/examples']
INFO:     Uvicorn running on http://localhost:8000 (Press CTRL+C to quit)
INFO:     Started reloader process [21915] using statreload
INFO:     Started server process [21917]
INFO:     Waiting for application startup.
consuming.....
INFO:     Application startup complete.
```

## Description

- The applicatin has an endpoint `GET` `/events`. Every time that it is called, an event is produce to the topic `local--kstream`.
- The application also has a `stream` that consumes from the topic `local--kstream`.
- The application `metrics` are exposed with the endpoint `/metrics`. To see the do a `GET` `/metrics`.

After doing a `GET` to `http://localhost:8000/events` you should see the following logs:

```bash
Event consumed: headers: (), payload: b'{"message": "hello world!"}'
```

## Stop FastAPI application

It is possible to stop the `FastAPI` application when a `Stream` crashes using [StreamErrorPolicy.STOP_APPLICATION](https://github.com/kpn/kstreams/blob/master/examples/fastapi-webapp/fastapi_webapp/app.py#L23)
For this use case, if the event `error` is sent then the `stream` will crash with [ValueError("error....")](https://github.com/kpn/kstreams/blob/master/examples/fastapi-webapp/fastapi_webapp/streams.py#L13)

To this this behaviour execute:

```bash
./scripts/cluster/events/send "local--kstream"
```

and type `error`

Then you should see something similar to the following logs:

```bash
INFO:     Will watch for changes in these directories: ['kstreams/examples/fastapi-webapp']
INFO:     Uvicorn running on http://localhost:8000 (Press CTRL+C to quit)
INFO:     Started reloader process [41292] using StatReload
INFO:     Started server process [41297]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
Event consumed: headers: (), payload: b'error'
Unhandled error occurred while listening to the stream. Stream consuming from topics ['local--kstream'] CRASHED!!! 

Traceback (most recent call last):
  File "kstreams/kstreams/middleware/middleware.py", line 83, in __call__
    return await self.next_call(cr)
           ^^^^^^^^^^^^^^^^^^^^^^^^
  File "kstreams/kstreams/middleware/udf_middleware.py", line 59, in __call__
    return await self.next_call(*params)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "kstreams/examples/fastapi-webapp/fastapi_webapp/streams.py", line 13, in consume
    raise ValueError("error....")
ValueError: error....
INFO:     Shutting down
INFO:     Waiting for application shutdown.
INFO:     Application shutdown complete.
INFO:     Finished server process [41297]
```

## Note

If you plan on using this example, pay attention to the `pyproject.toml` dependencies, where
`kstreams` is pointing to the parent folder. You will have to set the latest version.
