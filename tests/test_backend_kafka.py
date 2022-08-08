import pytest
from pydantic import ValidationError

from kstreams.backends.kafka import Kafka, SecurityProtocol


def test_default_ok():
    kafka_backend = Kafka()
    assert kafka_backend.bootstrap_servers == ["localhost:9092"]
    assert kafka_backend.security_protocol == SecurityProtocol.PLAINTEXT


def test_plaintext_ok():
    b_servers = ["kafka:9092", "localhost:9093"]
    kafka_backend = Kafka(bootstrap_servers=b_servers, security_protocol="PLAINTEXT")
    assert kafka_backend.bootstrap_servers == b_servers
    assert kafka_backend.security_protocol == SecurityProtocol.PLAINTEXT


def test_ssl_ok(ssl_context):

    kafka_backend = Kafka(
        security_protocol=SecurityProtocol.SSL, ssl_context=ssl_context
    )

    assert kafka_backend.security_protocol == SecurityProtocol.SSL


def test_ssl_fail_missing_context():
    with pytest.raises(ValidationError):
        Kafka(security_protocol=SecurityProtocol.SSL)


def test_sasl_plain_ok():
    username = "admin"
    pwd = "admin"
    kafka_backend = Kafka(
        security_protocol=SecurityProtocol.SASL_PLAINTEXT,
        sasl_plain_username=username,
        sasl_plain_password=pwd,
    )
    assert kafka_backend.security_protocol == SecurityProtocol.SASL_PLAINTEXT


def test_sasl_plain_fail_missing_pass():
    username = "admin"

    with pytest.raises(ValidationError) as e:
        Kafka(
            security_protocol=SecurityProtocol.SASL_PLAINTEXT,
            sasl_plain_username=username,
        )
    assert "sasl_plain_password" in str(e.value.args[0])


def test_sasl_plain_fail_missing_username():
    password = "admin"

    with pytest.raises(ValidationError) as e:
        Kafka(
            security_protocol=SecurityProtocol.SASL_PLAINTEXT,
            sasl_plain_password=password,
        )
    assert "sasl_plain_username" in str(e.value.args[0])


def test_sasl_ssl_fail_missing_pass(ssl_context):
    username = "admin"

    with pytest.raises(ValidationError) as e:
        Kafka(
            security_protocol=SecurityProtocol.SASL_SSL,
            ssl_context=ssl_context,
            sasl_plain_username=username,
        )
    assert "sasl_plain_password" in str(e.value.args[0])


def test_sasl_ssl_fail_missing_username(ssl_context):
    password = "admin"

    with pytest.raises(ValidationError) as e:
        Kafka(
            security_protocol=SecurityProtocol.SASL_SSL,
            ssl_context=ssl_context,
            sasl_plain_password=password,
        )
    assert "sasl_plain_username" in str(e.value.args[0])


def test_sasl_ssl_fail_missing_ssl_context():
    username = "admin"
    password = "admin"

    with pytest.raises(ValidationError) as e:
        Kafka(
            security_protocol=SecurityProtocol.SASL_SSL,
            sasl_plain_username=username,
            sasl_plain_password=password,
        )
    assert "ssl_context" in str(e.value.args[0])
