## Welcome to `kstreams` examples.

In order to run the examples you need `docker-compose`. In the ptoject root you will find the file `docker-compose.yaml` that contains a mininal setup to run `kafka` and `zookeeper`.

### Steps:

1. Activate your `virtualenv`: `poetry shell`
2. Create the kafka cluster: `make kafka-cluster`
3. Create the `kafka topics`: `make create-dev-topics`
4. Then execute the example that you want.

### Examples

1. `simple.py` example: minimal `kstream` example that `produces` and `consumes` events. The consumed events are printed in the terminal. Execute it with `poetry run python simple.py`
2. `consumer_multiple_topics.py`: A streams that consumes from multiple kafka topics. Execute it with `poetry run python consumer_multiple_topics.py`
3. `json_serialization.py`: Example of how to use custom  `serializers` and `deserializers` with `kstreams`. In this case we want `json`. Execute it with `poetry run python json_serialization.py`
4. [fastapi-webapp](https://github.com/kpn/kstreams/tree/0.11.8/examples/fastapi-webapp): An example of how to integrate `kstreams` with `FastAPI`.
