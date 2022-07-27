import contextlib
import ssl
import typing
from tempfile import NamedTemporaryFile

from kstreams import custom_types
from kstreams.conf import settings


class EmptySSLDataException(Exception):
    pass


class IncorrectCertificateFormat(Exception):
    pass


def encode_headers(headers: custom_types.Headers) -> custom_types.KafkaHeaders:
    return [(header, value.encode()) for header, value in headers.items()]


def retrieve_kafka_config(settings_prefix: str = "SERVICE_KSTREAMS_") -> typing.Dict:
    # Will convert all entries in the settings with the
    # `SERVICE_KSTREAMS_KAFKA_CONFIG_` prefix as keywords in a dict
    # Will also verify there is an entry for BOOTSTRAP_SERVERS
    # and will create the ssl context if the
    # SECURITY_PROTOCOL equals "SSL"
    prefix = settings_prefix + "KAFKA_CONFIG_"
    config = {
        k.replace(prefix, "").lower(): v
        for k, v in settings.as_dict().items()
        if k.startswith(prefix)
    }
    assert config["bootstrap_servers"]
    if config["security_protocol"] == "SSL" and "ssl_context" not in config:
        config["ssl_context"] = create_ssl_context_from_pkgsettings(
            settings_prefix=settings_prefix
        )
    return config


def verify_certificate_format(cert: str, cert_name: str):
    if "\n" not in cert:
        raise IncorrectCertificateFormat(
            f"{cert_name} data have the wrong format, line breaks are missing"
        )


def verify_certificate(cert: str, cert_name: str):
    if cert is None:
        raise EmptySSLDataException(
            f"{cert_name} data is empty while security_protocol is SSL. "
            f"{cert_name} is required"
        )

    verify_certificate_format(cert, cert_name)


def create_ssl_context_from_pkgsettings(
    settings_prefix: str = "SERVICE_KSTREAMS_",
) -> typing.Union[ssl.SSLContext, None]:
    config = settings.as_dict()

    cert_data = config[settings_prefix + "KAFKA_SSL_CERT_DATA"]
    key_data = config[settings_prefix + "KAFKA_SSL_KEY_DATA"]
    cabundle_data = config.get(settings_prefix + "KAFKA_SSL_CABUNDLE_DATA")

    verify_certificate(cert_data, "certificate")
    verify_certificate(key_data, "key")

    if cabundle_data is not None:
        verify_certificate_format(cabundle_data, "cabundle")

    return create_ssl_context_from_mem(cabundle_data, cert_data, key_data)


def create_ssl_context_from_mem(
    cabundle: typing.Optional[str], cert: str, priv_key: str
) -> typing.Optional[ssl.SSLContext]:
    """Create a SSL context from data on memory.

    This makes it easy to read the certificates from environmental variables
    Usually the data is loaded from env variables.
    """
    with contextlib.ExitStack() as stack:
        cert_file = stack.enter_context(NamedTemporaryFile(suffix=".crt"))
        key_file = stack.enter_context(NamedTemporaryFile(suffix=".key"))

        # expecting unicode data, writing it as bytes to files as utf-8
        cert_file.write(cert.encode("utf-8"))
        cert_file.flush()

        key_file.write(priv_key.encode("utf-8"))
        key_file.flush()

        ssl_context = ssl.create_default_context(cadata=cabundle)
        ssl_context.load_cert_chain(cert_file.name, keyfile=key_file.name)
        return ssl_context
    return None
