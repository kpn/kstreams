## SSE example

Simple `Server Send Events` example with `kstreams`

### Requirements

python 3.8+, poetry, docker-compose

### Installation

```bash
poetry install
```

## Usage

1. Start the kafka cluster: From `kstreams` project root execute `./scripts/cluster/start`
2. Start the `FastAPI webserver`. Inside the `fastapi-sse` folder execute `poetry run app`
3. Consume events from the topic with `fastapi-sse`: `curl http://localhost:8000/topics/local--sse/group-1/`. If everything worked, you should see a log similar to the following one where the `webserever` is running:
   ```bash
    INFO:     127.0.0.1:51060 - "GET /topics/local--sse/group-1/ HTTP/1.1" 200 OK
    Client connected Address(host='127.0.0.1', port=51060)
   ```
4. From a different terminal you can send events to the topic and they should be return to the frontend via `fastapi-sse`. From the `kstreams` project root  execute `./scripts/cluster/events/send local--sse`
   ```bash
   >Hi SSE
   ```

    Then in the same terminal where the `curl` command is running you should see the messages back:
    ```bash
    event: ping
    data: 2022-07-11 15:11:15.662361

    event: ping
    data: 2022-07-11 15:11:30.662774

    data: b'Hi SSE'
    ```
5. If you stop the curl command, the client will be disconnected:
   ```bash
   Disconnected from client (via refresh/close) Address(host='127.0.0.1', port=51580)
   ```

## Note

If you plan on using this example, pay attention to the `pyproject.toml` dependencies, where
`kstreams` is pointing to the parent folder. You will have to set the latest version.
