import logging
import uuid

from kstreams import (
    ConsumerRecord,
    Stream,
    TopicPartition,
    middleware,
    stream,
)
from kstreams.types import Send, Transaction

from .serializers import JsonDeserializerMiddleware

logger = logging.getLogger(__name__)


json_data = {"message": "Hello world!"}
raw_data = b"Hello world!"
transactional_topic = "local--kstreams-transactional"
json_topic = "local--kstreams-json"


async def save_to_db(data):
    logger.info(f"Saving data to db: {data}")


@stream(
    transactional_topic,
    group_id="transactional-group",
    enable_auto_commit=False,
    isolation_level="read_committed",  # <-- This will filter aborted txn's
    name="transactional-stream",
)
async def consume_from_transaction(cr: ConsumerRecord, stream: Stream):
    logger.info(
        f"Event consumed from topic {transactional_topic} with value: {cr.value} \n\n"
    )
    tp = TopicPartition(
        topic=transactional_topic,
        partition=cr.partition,
    )
    await save_to_db(cr.value)
    await stream.commit({tp: cr.offset + 1})


@stream(
    json_topic,
    group_id="my-group",
    enable_auto_commit=False,
    middlewares=[middleware.Middleware(JsonDeserializerMiddleware)],
    name="json-stream",
)
async def consume_json(cr: ConsumerRecord, send: Send, transaction: Transaction):
    logger.info(f"Json Event consumed with offset: {cr.offset}, value: {cr.value}\n\n")
    transaction_id = "my-transaction-id-" + str(uuid.uuid4())
    async with transaction(transaction_id=transaction_id) as t:
        # send raw data to show that it is possible to send data without serialization
        metadata = await t.send(
            transactional_topic,
            value=f"Transaction id {transaction_id} from argument in coroutine \n".encode(),
            serializer=None,
        )

        tp = TopicPartition(topic=cr.topic, partition=cr.partition)
        await t.commit_offsets(offsets={tp: cr.offset + 1}, group_id="my-group")
        # raise ValueError("This is a test error")
        logger.info(f"Message sent inside transaction with metadata: {metadata}")

    # The same can be achied using the STREAM ENGINE di
    # async with stream_engine.transaction(transaction_id=transaction_id) as t:
    # ...
