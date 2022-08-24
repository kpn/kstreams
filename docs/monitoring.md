This page discusses how to monitor your application using the Kafka metrics that are accessible in Prometheus.

Before we begin, it's crucial to note that Kafka itself makes a number of useful metrics available, including the cluster, broker, and clients (producer and consumers).

This means that we can quickly add some graphs to our dashboards by utilizing the already-exposed metrics.

`Kstreams` includes a collection of metrics. See [Metrics Docs](./metrics.md) for more information.

## Consumer Metrics

We advise including the `consumer_lag` in your application's grafana dashboard.

`consumer_lag` will show you how far your consumers are lagging behind the published events in the topic they are reading. For instance, if you have a single consumer and another team is producing millions of events, the consumer might not be able to handle them in time (where in time is defined by you, like: "in an hour of receiving a message it should be consumed").

Based on the lag, you will have to develop your own alerts. An alert should be pushed to Slack if you experience more than a particular amount of lag.

You will require your `consumer_group` name in order to design a basic dashboard using the `consumer_lag`.

We could add a query in Grafana like this:

```promql
sum(kafka_consumer_group_ConsumerLagMetrics_Value{topic =~ "YOUR_OWN_TOPIC_NAME", groupId =~"YOUR_CONSUMER_GROUP", name="SumOffsetLag"}) by (topic)
```

Remember to replace `YOUR_CONSUMER_GROUP` and `YOUR_OWN_TOPIC_NAME` with your `consumer_group` and `topic` respectively ⬆️

## Producer Metrics

If you have producers, it's a good idea to monitor the growth of Log End Offset (LEO).

The increase in LEO indicates the number of events produced in the last `N` minutes.

If you know that events should occur every `N` minutes, you can trigger alerts if no events occur because this metric will tell you whether or not events occurred.

We could add a query in Grafana like this, where `N` is `10m`:

```promql
sum(max(increase(kafka_log_Log_Value{name="LogEndOffset", topic =~ "TOPIC_NAME"}[10m])) by (partition, topic)) by (topic)
```

Remember to modify `TOPIC_NAME` to the name of the topic you want to track ⬆️

## Custom Business Metrics

One benefit of Prometheus is that you can design your own custom metrics.

**Scenario**: Consider an event-based ordering system. Assume you receive X orders daily and ship Y orders daily. Most likely, you will create a dashboard using this data.

Fortunately, we can create our own custom metrics by using the Prometheus Python client.

You can construct a variety of metrics with prometheus:

- `Gauge`
- `Counter`
- `Histogram`
- `Summary`

You can read more about it in [prometheus metric_types](https://prometheus.io/docs/concepts/metric_types/) website.

In our scenario, we will most likely want a `Counter` for orders received and a `Counter` for orders shipped.

```python
from prometheus_client import Counter
from kstreams.prometheus.monitor import PrometheusMonitor

class MyAppPrometheusMonitor(PrometheusMonitor):
    def __init__(self):
        super().__init__() # initialize kstream metrics
        self.orders_received = Counter('orders_received', 'Amount of orders received')
        self.orders_shipped = Counter('orders_shipped', 'Amount of orders shipped')

    def increase_received(self, amount: int = 1):
        self.orders_received.inc(amount)

    def increase_shipped(self, amount: int = 1):
        self.orders_shipped.inc(amount)
```

In our kstreams app, we can:

```python

stream_engine = create_engine(title="my-engine", monitor=MyAppPrometheusMonitor())

@stream_engine.stream("my-special-orders")
async def consume_orders_received(consumer):
    for cr, value, _ in consumer:
        if value.status == "NEW":
            stream_engine.monitor.increase_received()
        elif value.status == "SHIPPED":
            stream_engine.monitor.increase_shipped()
```

Your app's prometheus would display this data, which you might utilize to build a stylish ✨dashboard✨ interface.

For further details, see the [Prometheus python client](https://github.com/prometheus/client) documentation.