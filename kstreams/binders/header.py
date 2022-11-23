import inspect
from typing import Any, NamedTuple, Optional

from kstreams.exceptions import HeaderNotFound
from kstreams.types import ConsumerRecord


class HeaderExtractor(NamedTuple):
    name: str

    def __hash__(self) -> int:
        return hash((self.__class__, self.name))

    def __eq__(self, __o: object) -> bool:
        return isinstance(__o, HeaderExtractor) and __o.name == self.name

    async def extract(self, consumer_record: ConsumerRecord) -> Any:
        if isinstance(consumer_record.headers, dict):
            headers = tuple(consumer_record.headers.items())
            # NEXT: Check also if it is a sequence, if not it means
            # someone modified the headers in the serializer in a way
            # that we cannot extract it, raise a readable error
        else:
            headers = consumer_record.headers
        found_headers = [value for header, value in headers if header == self.name]

        try:
            header = found_headers.pop()
        except IndexError as e:
            message = (
                f"No header `{self.name}` found.\n"
                "Check if your broker is sending the header.\n"
                "Try adding a default value to your parameter like `None`.\n"
                "Or set `convert_underscores = False`."
            )
            raise HeaderNotFound(message) from e
        else:
            return header


class HeaderMarker(NamedTuple):
    alias: Optional[str]
    convert_underscores: bool

    def register_parameter(self, param: inspect.Parameter) -> HeaderExtractor:
        if self.alias is not None:
            name = self.alias
        elif self.convert_underscores:
            name = param.name.replace("_", "-")
        else:
            name = param.name
        return HeaderExtractor(name=name)
