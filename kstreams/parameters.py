from typing import Optional, TypeVar

from kstreams.binders.api import BinderMarker
from kstreams.binders.header import HeaderMarker
from kstreams.typing import Annotated


def Header(
    *, alias: Optional[str] = None, convert_underscores: bool = True
) -> BinderMarker:
    """Construct another type from the headers of a kafka record.

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

FromHeader = Annotated[T, Header(alias=None)]
FromHeader.__doc__ = """General purpose convenient header type.

Use `Annotated` to provide custom params.
"""
