from kstreams import backends, create_engine

backend = backends.Kafka(
    bootstrap_servers=["localhost:9092"],
    security_protocol=backends.kafka.SecurityProtocol.PLAINTEXT,
)

stream_engine = create_engine(
    title="my-stream-engine",
    backend=backend,
)
