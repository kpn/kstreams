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
