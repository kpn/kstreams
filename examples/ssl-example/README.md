# Kstreams SSL example

This example shows how setup an SSL connection with `kstreams`. For this purpose we have to setup a local kafka SSL cluster.

`ENV` varialbles are exported using the script `/scripts/ssl/export-env-variables` and loaded in python using [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/).

Check [resources.py](https://github.com/kpn/kstreams/blob/master/examples/ssl-example/ssl_example/resources.py) to see how the backend with `SSL` is created

## Requirements

`python 3.8+`, `poetry`, `docker-compose`, `openssl`

### Usage

First create the server and client sertificates:

```bash
./scripts/ssl/ssl-setup
```

After executing the privious script, you will see a new folder called `kafka-certs`. The folder contains the `server` (inside the server folder),
`admin` (inside the admin folder) and `client` certicicates. Do not worry about the content of them, it is just an example and they can be deleted,
shared and recreated (it is just a local example)

Now you can run the local SSL cluster:

```bash
./scripts/cluster/start
```

Second, you need to install the project dependencies dependencies. In a different terminal execute:

```bash
poetry install
```

Export the env variables:

```bash
. ./scripts/ssl/export-env-variables
```

Then we can run the project

```bash
poetry run app
```

You should see something similar to the following logs:

```bash
kstreams/examples/ssl-example via 🐳 colima is 📦 v0.1.0 via 🐍 v3.12.4 
❯ poetry run app

INFO:ssl_example.app:Starting application...
INFO:aiokafka.consumer.subscription_state:Updating subscribed topics to: frozenset({'local--kstreams'})
INFO:aiokafka.consumer.consumer:Subscribed to topic(s): {'local--kstreams'}
INFO:kstreams.prometheus.monitor:Starting Prometheus Monitoring started...
INFO:ssl_example.app:Producing event 0
INFO:ssl_example.app:Producing event 1
INFO:ssl_example.app:Producing event 2
INFO:ssl_example.app:Producing event 3
INFO:ssl_example.app:Producing event 4
ERROR:aiokafka.consumer.group_coordinator:Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
ERROR:aiokafka.consumer.group_coordinator:Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
ERROR:aiokafka.consumer.group_coordinator:Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
ERROR:aiokafka.consumer.group_coordinator:Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
ERROR:aiokafka.consumer.group_coordinator:Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
ERROR:aiokafka.consumer.group_coordinator:Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
ERROR:aiokafka.consumer.group_coordinator:Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
ERROR:aiokafka.consumer.group_coordinator:Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
ERROR:aiokafka.consumer.group_coordinator:Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
ERROR:aiokafka.consumer.group_coordinator:Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
ERROR:aiokafka.consumer.group_coordinator:Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
ERROR:aiokafka.consumer.group_coordinator:Group Coordinator Request failed: [Error 15] GroupCoordinatorNotAvailableError
INFO:aiokafka.consumer.group_coordinator:Discovered coordinator 1 for group example-group
INFO:aiokafka.consumer.group_coordinator:Revoking previously assigned partitions set() for group example-group
INFO:aiokafka.consumer.group_coordinator:(Re-)joining group example-group
INFO:aiokafka.consumer.group_coordinator:Joined group 'example-group' (generation 1) with member_id aiokafka-0.11.0-5fb10c73-64b2-42a8-ae8a-23f59d4a3b6b
INFO:aiokafka.consumer.group_coordinator:Elected group leader -- performing partition assignments using roundrobin
INFO:aiokafka.consumer.group_coordinator:Successfully synced group example-group with generation 1
INFO:aiokafka.consumer.group_coordinator:Setting newly assigned partitions {TopicPartition(topic='local--kstreams', partition=0)} for group example-group
```

## Note

If you plan on using this example, pay attention to the `pyproject.toml` dependencies, where
`kstreams` is pointing to the parent folder. You will have to set the latest version.
