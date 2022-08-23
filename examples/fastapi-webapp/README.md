## FastAPI webapp example

Simple [`FastAPI`](https://fastapi.tiangolo.com/) example with `kstreams`

### Requirements

python 3.8+, poetry, docker-compose

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

## Note

If you plan on using this example, pay attention to the `pyproject.toml` dependencies, where
`kstreams` is pointing to the parent folder. You will have to set the latest version.
