`Kafka 0.11.0` includes support for `idempotent` and `transactional` capabilities in the `producer`. Idempotent delivery ensures that messages are delivered `exactly once`
to a particular topic partition during the lifetime of a `single producer`. Transactional delivery allows producers to send data to multiple partitions such that either
all messages are successfully delivered, or none of them are. Together, these capabilities enable `*exactly once semantics*` in Kafka.

It is important to notice that:

- `Transaction` always start from a `send` (producer)
- Events sent to one or more topics will only be visible on consumers after the transaction is committed.
- To use transactions, a `transaction id` (unique id per transaction) must be set prior an event is sent. If you do not provide one `kstreams` will auto generate one for you.
- Transaction state is stored in a new internal topic `__transaction_state`. This topic is not created until the the first attempt to use a transactional request API. There are several settings to control the topic's configuration.
- `Topics` which are included in transactions should be configured for durability. In particular, the `replication.factor` should be at least `3`, and the `min.insync.replicas` for these topics should be set to `2`
- `Transactions` always add overhead, meaning that more effort is needed to produce events and the consumer have to apply filters
For example, `transaction.state.log.min.isr` controls the minimum ISR for this topic.
- `Streams` (consumers) will have to filter or not transactional events

```plantuml
@startuml
!theme crt-amber

!include https://raw.githubusercontent.com/plantuml-stdlib/C4-PlantUML/master/C4_Context.puml

agent "Producer" as producer
agent "Stream A" as stream_a
agent "Stream B" as stream_b
queue "Topic A" as topic_a

producer -> topic_a: "produce with a transaction"
topic_a --> stream_a: "consume only transactional events"
topic_a --> stream_b: "consume all events"
@enduml
```

## Transaction State

A transaction have state, which is store in the special topic `__transaction_state`. This topic can be tuned in different ways, but it is outside of this guide scope.
The only important detailt that you must be aware of is that a `transaction` can be `commited` or `aborted`. You will not manage the internal states but you are responsible
of filtering or not transactions on the `stream` side.

```plantuml
@startuml
!theme crt-amber

!include https://raw.githubusercontent.com/plantuml-stdlib/C4-PlantUML/master/C4_Context.puml

[*] --> Init
Init : Initialized with a `transaction_id`

Init -> Commited
Commited --> [*]

Init -> Aborted
Aborted --> [*]

@enduml
```

## Usage

From the `kstreams` point of view, the `transaction pattern` is a `context manager` that will `start` a transaction and then `commit` or `aboort` it. Always a `transaction` starts when an event is send. Once that we have the `context` we can send events in two ways:

Using the `StreamEngine` directly:

```python
from kstreams import create_engine
from kstreams.backends import Kafka

from .serializers import JsonSerializer

stream_engine = create_engine(
    title="transaction-engine",
    serializer=JsonSerializer(),
    backend=Kafka(
        bootstrap_servers=["localhost:9091", "localhost:9092", "localhost:9093"],
    ),
)


transaction_id = "my-transaction-id-" + str(uuid.uuid4())
async with stream_engine.transaction(transaction_id=transaction_id) as t:
    await t.send(
        "transactional_topic",
        value=f"Transaction id {transaction_id} from argument in coroutine \n".encode(),
    )
```

or using the `Transaction` annotation:

```python title="Transaction inside a stream"
from kstreams import ConsumerRecord, Transaction


@stream_engine.stream("a-topic", group_id="my-group")
async def stream_a(cr: ConsumerRecord, transaction: Transaction):
    transaction_id = "my-transaction-id-" + str(uuid.uuid4())
    async with transaction(transaction_id=transaction_id) as t:
        await t.send(
            "transactional_topic",
            value=f"Transaction id {transaction_id} from argument in coroutine \n".encode(),
        )
```  

Once the `context` is left then the transaction is `commit` or `aborted`.

## Commiting offsets

With transactions it is possible to `commit` consumer offsets. This is quite important feature as it will guarantee that only a consumer offset is commit once the whole transaction as has been commited, including sending events to other topics.
To commit consumer offsets we need the `offsetes` and the `group_id` that we want to commit to. Example:

```python
from kstreams import ConsumerRecord, Transaction


@stream_engine.stream("a-topic", group_id="my-group", enable_auto_commit=False)
async def stream_a(cr: ConsumerRecord, transaction: Transaction):
    transaction_id = "my-transaction-id-" + str(uuid.uuid4())
    async with transaction(transaction_id=transaction_id) as t:
        metadata = await t.send(
            transactional_topic,
            value=f"Transaction id {transaction_id} from argument in coroutine \n".encode(),
        )

        tp = TopicPartition(topic=cr.topic, partition=cr.partition)
        await t.commit_offsets(offsets={tp: cr.offset + 1}, group_id="my-group")
        logger.info(f"Message sent inside transaction with metadata: {metadata}")
```

!!! note
    - The property `enable_auto_commit` must be set to `False`
    - When commiting offsets the `group_id` must be the same as the `consumer group_id`
    - Offsets is a dictionary of TopicPartition: offset
    - If the `transaction` is `aborted` then the commit offset has NOT effect

## Reading committed events

It is the `stream` (consumer) responsability to filter commited events. The property `isolation_level="read_committed"` must be used. If the `isolation_level` is NOT set to `read_committed` then events that were sent with an `aborted` transaction are also consumed

```plantuml
@startuml
!theme crt-amber

!include https://raw.githubusercontent.com/plantuml-stdlib/C4-PlantUML/master/C4_Context.puml

agent "Stream A" as stream_a
agent "Stream B" as stream_b
queue "Topic A" as topic_a
queue "Topic B" as topic_b

topic_a -> stream_a: "consume all events"
stream_a --> topic_b: "produce with a transaction and commit offsets"
topic_b -> stream_b: "filter commited events"
@enduml
```

The above diagram implementation is the following:

```python
from kstreams import ConsumerRecord, Transaction


@stream(
    "transactional_topic",
    group_id="another-group",
    enable_auto_commit=False,
    isolation_level="read_committed",  # <-- This will filter aborted txn's
)
async def stream_b(cr: ConsumerRecord, stream: Stream):
    logger.info(
        f"Event consumed from topic {transactional_topic} with value: {cr.value} \n\n"
    )
    await stream.commit()


@stream_engine.stream("a-topic", group_id="my-group")
async def stream_a(cr: ConsumerRecord, transaction: Transaction):
    transaction_id = "my-transaction-id-" + str(uuid.uuid4())
    async with transaction(transaction_id=transaction_id) as t:
        metadata = await t.send(
            "transactional_topic",
            value=f"Transaction id {transaction_id} from argument in coroutine \n".encode(),
        )

        tp = TopicPartition(topic=cr.topic, partition=cr.partition)
        await t.commit_offsets(offsets={tp: cr.offset + 1}, group_id="my-group")
        logger.info(f"Message sent inside transaction with metadata: {metadata}")
```

## Aborted transactions

A transaction can be aborted, for example when we have a bug in our code. Consired the following example in which an error is `raised`:

```python
from kstreams import ConsumerRecord, Transaction


@stream_engine.stream("a-topic", group_id="my-group")
async def consume_json(cr: ConsumerRecord, transaction: Transaction):
    transaction_id = "my-transaction-id-" + str(uuid.uuid4())
    async with transaction(transaction_id=transaction_id) as t:
        metadata = await t.send(
            transactional_topic,
            value=f"Transaction id {transaction_id} from argument in coroutine \n".encode(),
            serializer=None,
        )

        tp = TopicPartition(topic=cr.topic, partition=cr.partition)
        await t.commit_offsets(
            offsets={tp: cr.offset + 1}, group_id="my-group-json-data"
        )

        raise ValueError("This is a test error")
```

The code will crash when the line with `raise ValueError` is reached, causing to leave the context manager with an `aborted` transaction. Any error that ocuurs inside the `context manager` will `abort` the `transaction`.

It is also important to notice that:

- If the transaction is aborted then `commited offsets` has not effect
- Events are always sent regarless whether the transaction was aborted. The consumer is responsible to filter events

## Example

A full transaction application example with `kstreams` can be found in [transaction example](https://github.com/kpn/kstreams/tree/master/examples/transactions). It also contains an `e2e` test to demostrate how can test applications with `transactions`. It implements the following diagrama:

```plantuml
@startuml
!theme crt-amber

!include https://raw.githubusercontent.com/plantuml-stdlib/C4-PlantUML/master/C4_Context.puml

agent "Producer" as producer
agent "Stream A" as stream_a
agent "Stream B" as stream_b
queue "Topic A" as topic_a
queue "Topic B" as topic_b

producer -> topic_a: "produce"
topic_a <-- stream_a: "consume"
stream_a -> topic_b: "produce with transaction"
topic_b <-- stream_b: "consume read_committed events"
@enduml
```
