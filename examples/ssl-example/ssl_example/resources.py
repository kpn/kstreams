import ssl

from pydantic_settings import BaseSettings, SettingsConfigDict

from kstreams import backends, create_engine, utils


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KAFKA_")

    bootstrap_servers: list[str]
    ssl_cert_data: str
    ssl_key_data: str
    ssl_cabundle_data: str

    def get_ssl_context(self) -> ssl.SSLContext | None:
        return utils.create_ssl_context_from_mem(
            certdata=self.ssl_cert_data,
            keydata=self.ssl_key_data,
            cadata=self.ssl_cabundle_data,
        )


settings = Settings()
backend = backends.Kafka(
    bootstrap_servers=settings.bootstrap_servers,
    security_protocol=backends.kafka.SecurityProtocol.SSL,
    ssl_context=settings.get_ssl_context(),
)


stream_engine = create_engine(title="my-stream-engine", backend=backend)
