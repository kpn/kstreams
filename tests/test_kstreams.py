from kstreams.utils import (
    create_ssl_context_from_pkgsettings,
    EmptySSLDataException,
    IncorrectCertificateFormat,
    settings,
)

import pytest

settings_prefix = "TEST_"


def test_empty_certificates():
    settings.configure(TEST_KAFKA_SSL_CERT_DATA=None, TEST_KAFKA_SSL_KEY_DATA=None)
    with pytest.raises(EmptySSLDataException):
        create_ssl_context_from_pkgsettings(settings_prefix=settings_prefix)


def test_wrong_certificate_format():
    settings.configure(
        TEST_KAFKA_SSL_CERT_DATA="a string", TEST_KAFKA_SSL_KEY_DATA="a string"
    )
    with pytest.raises(IncorrectCertificateFormat):
        create_ssl_context_from_pkgsettings(settings_prefix=settings_prefix)


def test_wrong_cabundle_format(ssl_data):
    settings.configure(
        TEST_KAFKA_CONFIG_SECURITY_PROTOCOL="SSL",
        TEST_KAFKA_SSL_CERT_DATA=ssl_data.cert,
        TEST_KAFKA_SSL_KEY_DATA=ssl_data.key,
        TEST_KAFKA_SSL_CABUNDLE_DATA="a string",
    )

    with pytest.raises(IncorrectCertificateFormat):
        create_ssl_context_from_pkgsettings(settings_prefix=settings_prefix)


def test_create_ssl_context(ssl_data):
    settings.configure(
        TEST_KAFKA_CONFIG_SECURITY_PROTOCOL="SSL",
        TEST_KAFKA_SSL_CERT_DATA=ssl_data.cert,
        TEST_KAFKA_SSL_KEY_DATA=ssl_data.key,
        TEST_KAFKA_SSL_CABUNDLE_DATA=None,
    )

    create_ssl_context_from_pkgsettings(settings_prefix=settings_prefix)


def test_create_ssl_context_with_cabundle_ssl(ssl_data):
    settings.configure(
        TEST_KAFKA_CONFIG_SECURITY_PROTOCOL="SSL",
        TEST_KAFKA_SSL_CERT_DATA=ssl_data.cert,
        TEST_KAFKA_SSL_KEY_DATA=ssl_data.key,
        TEST_KAFKA_SSL_CABUNDLE_DATA=ssl_data.cabundle,
    )

    create_ssl_context_from_pkgsettings(settings_prefix=settings_prefix)
