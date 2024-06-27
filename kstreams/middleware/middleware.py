import logging
import sys
import typing

from aiokafka import errors

from kstreams import ConsumerRecord, types

if typing.TYPE_CHECKING:
    from kstreams import Stream  #  pragma: no cover


logger = logging.getLogger(__name__)


class MiddlewareProtocol(typing.Protocol):
    def __init__(
        self,
        *,
        next_call: types.NextMiddlewareCall,
        send: types.Send,
        stream: "Stream",
        **kwargs: typing.Any,
    ) -> None:
        ...  #  pragma: no cover

    async def __call__(self, cr: ConsumerRecord) -> typing.Any:
        ...  #  pragma: no cover


class Middleware:
    def __init__(
        self, middleware: typing.Type[MiddlewareProtocol], **kwargs: typing.Any
    ) -> None:
        self.middleware = middleware
        self.kwargs = kwargs

    def __iter__(self) -> typing.Iterator:
        return iter((self.middleware, self.kwargs))

    def __repr__(self) -> str:
        middleware_name = self.middleware.__name__
        extra_options = [f"{key}={value!r}" for key, value in self.kwargs.items()]
        return f"{middleware_name}({extra_options})"


class BaseMiddleware:
    def __init__(
        self,
        *,
        next_call: types.NextMiddlewareCall,
        send: types.Send,
        stream: "Stream",
    ) -> None:
        self.next_call = next_call
        self.send = send
        self.stream = stream

    async def __call__(self, cr: ConsumerRecord) -> typing.Any:
        raise NotImplementedError


class ExceptionMiddleware(BaseMiddleware):
    async def __call__(self, cr: ConsumerRecord) -> typing.Any:
        try:
            return await self.next_call(cr)
        except errors.ConsumerStoppedError as exc:
            await self.cleanup_policy()
            raise exc
        except Exception as exc:
            logger.exception(
                "Unhandled error occurred while listening to the stream. "
                f"Stream consuming from topics {self.stream.topics} CRASHED!!! \n\n "
            )
            if sys.version_info >= (3, 11):
                exc.add_note(f"Handler: {self.stream.func}")
                exc.add_note(f"Topics: {self.stream.topics}")

            await self.cleanup_policy()
            raise exc

    async def cleanup_policy(self) -> None:
        # always release the asyncio.Lock `is_processing` to
        # stop properly the `stream`
        self.stream.is_processing.release()
        await self.stream.stop()

        # acquire the asyncio.Lock `is_processing` again to resume the processing
        # and avoid `RuntimeError: Lock is not acquired.`
        await self.stream.is_processing.acquire()
