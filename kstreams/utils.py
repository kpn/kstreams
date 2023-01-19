import contextlib
import ssl
from tempfile import NamedTemporaryFile
from typing import Optional, Union

from aiokafka.helpers import create_ssl_context as aiokafka_create_ssl_context

from kstreams import types


def encode_headers(headers: types.Headers) -> types.EncodedHeaders:
    return [(header, value.encode()) for header, value in headers.items()]


def create_ssl_context_from_mem(
    *,
    certdata: str,
    keydata: str,
    password: Optional[str] = None,
    cadata: Optional[str] = None,
) -> Optional[ssl.SSLContext]:
    """Create a SSL context from data on memory.

    This makes it easy to read the certificates from environmental variables
    Usually the data is loaded from env variables.

    Arguments:
        cadata: certificates used to sign broker certificates provided as unicode str
        certdata: the client certificate, as well as any CA certificates needed to
            establish the certificate's authenticity provided as unicode str
        keydata: the client private key provided as unicode str
        password: optional password to be used when loading the
            certificate chain
    """
    with contextlib.ExitStack() as stack:
        cert_file = stack.enter_context(NamedTemporaryFile(suffix=".crt"))
        key_file = stack.enter_context(NamedTemporaryFile(suffix=".key"))

        # expecting unicode data, writing it as bytes to files as utf-8
        cert_file.write(certdata.encode("utf-8"))
        cert_file.flush()

        key_file.write(keydata.encode("utf-8"))
        key_file.flush()

        ssl_context = ssl.create_default_context(cadata=cadata)
        ssl_context.load_cert_chain(
            cert_file.name, keyfile=key_file.name, password=password
        )
        return ssl_context
    return None


def create_ssl_context(
    *,
    cafile: str = None,
    capath: str = None,
    cadata: Union[str, bytes] = None,
    certfile: str = None,
    keyfile: str = None,
    password: str = None,
    crlfile=None,
):
    """Wrapper of [aiokafka.helpers.create_ssl_context](
        https://aiokafka.readthedocs.io/en/stable/api.html#helpers
    )
    with typehints.

    Arguments:
        cafile: Certificate Authority file path containing certificates
            used to sign broker certificates
        capath: Same as `cafile`, but points to a directory containing
            several CA certificates
        cadata: Same as `cafile`, but instead contains already
            read data in either ASCII or bytes format
        certfile: optional filename of file in PEM format containing
            the client certificate, as well as any CA certificates needed to
            establish the certificate's authenticity
        keyfile: optional filename containing the client private key.
        password: optional password to be used when loading the
            certificate chain

    """
    return aiokafka_create_ssl_context(
        cafile=cafile,
        capath=capath,
        cadata=cadata,
        certfile=certfile,
        keyfile=keyfile,
        password=password,
        crlfile=crlfile,
    )


__all__ = ["create_ssl_context", "create_ssl_context_from_mem", "encode_headers"]
