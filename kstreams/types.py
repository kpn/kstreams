from typing import Dict, Sequence, Tuple

from aiokafka.structs import ConsumerRecord as AIOConsumerRecord

Headers = Dict[str, str]
EncodedHeaders = Sequence[Tuple[str, bytes]]


class ConsumerRecord(AIOConsumerRecord):
    ...
