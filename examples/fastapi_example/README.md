## FastAPI example

Simple `FastAPI` example with `kstreams`

### Usage

1. Activate your `virtualenv`: `poetry shell`
2. Create the kafka cluster: `make kafka-cluster`
3. Create the `kafka topics`: `make create-dev-topics`
4. Then execute the example that you want.
5. Execute: `python -m fastapi_example`. After it you should see something like:

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

### Stream

There is one `stream` consuming from the topic `dev-kpn-des--kstream`.

### Producer

To produce events to can `GET` the endpoint `http://localhost:8000/events` and it will produce events to the topic `dev-kpn-des--kstream`