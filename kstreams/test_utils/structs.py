from enum import Enum
from typing import NamedTuple


class RecordMetadata(NamedTuple):
    offset: int
    partition: int
    topic: str
    timestamp: int


class TransactionStatus(str, Enum):
    NOT_STARTED = "NOT_STARTED"
    INITIALIZED = "INITIALIZED"
    ABORTED = "ABORTED"
    COMMITTED = "COMMITTED"
