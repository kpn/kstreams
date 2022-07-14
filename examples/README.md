## Welcome to `kstreams` examples.

In order to run the examples you need `docker-compose`. In the ptoject root you will find the file `docker-compose.yaml` that contains a mininal setup to run `kafka` and `zookeeper`.

### Steps:

1. Activate your `virtualenv`: `poetry shell`
2. Create the kafka cluster: `make kafka-cluster`
3. Create the `kafka topics`: `make create-dev-topics`
4. Then execute the example that you want.

### Examples

1. `simple.py` example: minimal `kstream` example that `produces` and `consumes` events.
2. `consumer_multiple_topics.py`: A streams that consumes from multiple kafka topics.
3. `json_serialization.py`: Example of how to use custom  `serializers` and `deserializers` with `kstreams`. In this case we want `json`.
4. `fastapi_example`: An example of how to integrate `kstreams` with `FastAPI`. Execute it with `python -m fastapi_example`
