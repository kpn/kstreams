from typing import Optional, TypeVar

from kstreams._di.binders.api import BinderMarker
from kstreams._di.binders.header import HeaderMarker
from kstreams.typing import Annotated


def Header(
    *, alias: Optional[str] = None, convert_underscores: bool = True
) -> BinderMarker:
    """Construct another type from the headers of a kafka record.

    Args:
        alias: Use a different header name
        convert_underscores: If True, convert underscores to dashes.

    Usage:

    ```python
    from kstream import Header, Annotated

    def user_fn(event_type: Annotated[str, Header(alias="EventType")]):
        ...
    ```
    """
    header_marker = HeaderMarker(alias=alias, convert_underscores=convert_underscores)
    binder = BinderMarker(extractor_marker=header_marker)
    return binder


T = TypeVar("T")

FromHeader = Annotated[T, Header()]
FromHeader.__doc__ = """General purpose convenient header type.

Use `Annotated` to provide custom params.
"""
