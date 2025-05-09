import asyncio
import contextlib
import inspect
import logging
import ssl
import sys
from tempfile import NamedTemporaryFile
from typing import Any, Optional, Union

from aiokafka.helpers import create_ssl_context as aiokafka_create_ssl_context

from kstreams import types

logger = logging.getLogger(__name__)

PY_VERSION = sys.version_info


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
    cafile: Optional[str] = None,
    capath: Optional[str] = None,
    cadata: Union[str, bytes, None] = None,
    certfile: Optional[str] = None,
    keyfile: Optional[str] = None,
    password: Optional[str] = None,
    crlfile: Any = None,
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


async def execute_hooks(hooks: types.EngineHooks) -> None:
    for hook in hooks:
        if inspect.iscoroutinefunction(hook):
            await hook()
        else:
            hook()


async def stop_task(task: asyncio.Task) -> None:
    async def cancel_attempt(task: asyncio.Task) -> None:
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            logger.debug(f"Cancelling {task} now")

    if PY_VERSION >= (3, 11):
        # extra check for python 3.11 to prevent
        #  RuntimeError: await wasn't used with future
        if not task.done() and not task.cancelling():  # type: ignore [attr-defined]
            await cancel_attempt(task)
    else:
        if not task.done():
            # python < 3.11 we can not do anything to prevent the error
            await cancel_attempt(task)


__all__ = ["create_ssl_context", "create_ssl_context_from_mem", "encode_headers"]
