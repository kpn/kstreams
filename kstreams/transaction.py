import logging
import typing
import uuid
from functools import partial

from .backends.kafka import Kafka
from .clients import Producer
from .types import Send

logger = logging.getLogger(__name__)


class Transaction:
    def __init__(self, transaction_id: str, producer: Producer, send: Send) -> None:
        self.transaction_id = transaction_id
        self.producer = producer
        # inject the unique producer per transaction_id
        self.send = partial(send, producer=self.producer)

    async def commit_offsets(self, *, offsets: dict[str, int], group_id: str) -> None:
        await self.producer.send_offsets_to_transaction(offsets, group_id)
        logger.info(f"Commiting offsets {offsets} for group {group_id}")

    async def __aenter__(self):
        await self.producer.start()
        await self.producer.begin_transaction()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc:
            if self.producer._txn_manager.is_fatal_error():
                logger.error(f"Error in transaction: {exc}")
                # TODO: check if it is possible to reach this line
                return
            await self.producer.abort_transaction()
            logger.error(f"Transaction with id {self.transaction_id} aborted")
        else:
            await self.producer.commit_transaction()
            logger.info(f"Transaction with id {self.transaction_id} committed")

        await self.producer.stop()


class TransactionManager:
    def __init__(
        self, producer_class: typing.Type[Producer], backend: Kafka, send: Send
    ) -> None:
        self.backend = backend
        self.producer_class = producer_class
        self.send = send

        # map active transaction ids to transaction objects
        # TODO: check if a transaction can be reused
        self._transactions: dict[str, Transaction] = {}

    def get_or_create_transaction(
        self, transaction_id: typing.Optional[str] = None
    ) -> Transaction:
        # transaction_id must be unique and it can not be reused, otherwise
        # it will raise an error aiokafka.errors.KafkaError:
        # KafkaError: Unexpected error during batch delivery
        transaction_id = transaction_id or str(uuid.uuid4())

        if (
            transaction_id in self._transactions
        ):  # do .get() instead and fallback to function
            return self._transactions[transaction_id]

        producer = self._create_producer(transaction_id=transaction_id)
        transaction = Transaction(
            transaction_id=transaction_id, producer=producer, send=self.send
        )
        self._transactions[transaction_id] = transaction
        return transaction

    def _create_producer(self, transaction_id: str, **kwargs) -> Producer:
        config = {**self.backend.model_dump(), **kwargs}
        config["transactional_id"] = transaction_id
        producer = self.producer_class(**config)
        return producer
