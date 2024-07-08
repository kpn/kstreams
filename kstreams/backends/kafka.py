import ssl
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, model_validator


class SecurityProtocol(str, Enum):
    SSL = "SSL"
    PLAINTEXT = "PLAINTEXT"
    SASL_PLAINTEXT = "SASL_PLAINTEXT"
    SASL_SSL = "SASL_SSL"


class SaslMechanism(str, Enum):
    PLAIN = "PLAIN"
    GSSAPI = "GSSAPI"
    SCRAM_SHA_256 = "SCRAM-SHA-256"
    SCRAM_SHA_512 = "SCRAM-SHA-512"
    OAUTHBEARER = "OAUTHBEARER"


class Kafka(BaseModel):
    """
    The `Kafka` backend validates the given attributes.

    It uses pydantic internally.

    Attributes:
        bootstrap_servers: kafka list of `hostname:port`
        security_protocol: Protocol used to communicate with brokers
        ssl_context: a python std `ssl.SSLContext` instance, you can generate
            it with `create_ssl_context`
            or `create_ssl_context_from_mem`
        sasl_mechanism: Authentication mechanism when `security_protocol` is configured
            for `SASL_PLAINTEXT` or `SASL_SSL`
        sasl_plain_username: username for sasl PLAIN authentication
        sasl_plain_password: password for sasl PLAIN authentication
        sasl_oauth_token_provider: smth

    Raises:
        ValidationError: a `pydantic.ValidationError` exception

    ## PLAINTEXT

    !!! Example
        ```python
        from kstreams.backends.kafka import Kafka
        from kstreams import create_engine, Stream

        backend = Kafka(bootstrap_servers=["localhost:9092"])
        stream_engine = create_engine(title="my-stream-engine", backend=backend)
        ```

    ## SSL

    !!! Example
        ```python title="Create SSL context"
        import ssl

        from kstreams.backends.kafka import Kafka
        from kstreams import create_engine, utils, Stream


        def get_ssl_context() -> ssl.SSLContext:
            return utils.create_ssl_context(
                cafile="certificate-authority-file-path",
                capath="points-to-directory-with-several-ca-certificates",
                cadata="same-as-cafile-but-ASCII-or-bytes-format",
                certfile="client-certificate-file-name",
                keyfile="client-private-key-file-name",
                password="password-to-load-certificate-chain",
            )

        backend = Kafka(
            bootstrap_servers=["localhost:9094"],
            security_protocol="SSL",
            ssl_context=get_ssl_context(),
        )

        stream_engine = create_engine(title="my-stream-engine", backend=backend)
        ```

        !!! note
            Check [create ssl context util](https://kpn.github.io/kstreams/utils/#kstreams.utils.create_ssl_context)

    !!! Example
        ```python title="Create SSL context from memory"
        import ssl

        from kstreams.backends.kafka import Kafka
        from kstreams import create_engine, utils, Stream


        def get_ssl_context() -> ssl.SSLContext:
            return utils.create_ssl_context_from_mem(
                cadata="ca-certificates-as-unicode",
                certdata="client-certificate-as-unicode",
                keydata="client-private-key-as-unicode",
                password="optional-password-to-load-certificate-chain",
            )

        backend = Kafka(
            bootstrap_servers=["localhost:9094"],
            security_protocol="SSL",
            ssl_context=get_ssl_context(),
        )

        stream_engine = create_engine(title="my-stream-engine", backend=backend)
        ```

        !!! note
            Check [create ssl context from memerory util](https://kpn.github.io/kstreams/utils/#kstreams.utils.create_ssl_context_from_mem)
    """

    bootstrap_servers: List[str] = ["localhost:9092"]
    security_protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT

    ssl_context: Optional[ssl.SSLContext] = None

    sasl_mechanism: SaslMechanism = SaslMechanism.PLAIN
    sasl_plain_username: Optional[str] = None
    sasl_plain_password: Optional[str] = None
    sasl_oauth_token_provider: Optional[str] = None
    model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)

    @model_validator(mode="after")
    @classmethod
    def protocols_validation(cls, values):
        security_protocol = values.security_protocol

        if security_protocol == SecurityProtocol.PLAINTEXT:
            return values
        elif security_protocol == SecurityProtocol.SSL:
            if values.ssl_context is None:
                raise ValueError("`ssl_context` is required")
            return values
        elif security_protocol == SecurityProtocol.SASL_PLAINTEXT:
            if values.sasl_mechanism is SaslMechanism.OAUTHBEARER:
                # We don't perform a username and password check if OAUTHBEARER
                return values
            if (
                values.sasl_mechanism is SaslMechanism.PLAIN
                and values.sasl_plain_username is None
            ):
                raise ValueError(
                    "`sasl_plain_username` is required when using SASL_PLAIN"
                )
            if (
                values.sasl_mechanism is SaslMechanism.PLAIN
                and values.sasl_plain_password is None
            ):
                raise ValueError(
                    "`sasl_plain_password` is required when using SASL_PLAIN"
                )
            return values
        elif security_protocol == SecurityProtocol.SASL_SSL:
            if values.ssl_context is None:
                raise ValueError("`ssl_context` is required")
            if (
                values.sasl_mechanism is SaslMechanism.PLAIN
                and values.sasl_plain_username is None
            ):
                raise ValueError(
                    "`sasl_plain_username` is required when using SASL_PLAIN"
                )
            if (
                values.sasl_mechanism is SaslMechanism.PLAIN
                and values.sasl_plain_password is None
            ):
                raise ValueError(
                    "`sasl_plain_password` is required when using SASL_PLAIN"
                )
            return values
