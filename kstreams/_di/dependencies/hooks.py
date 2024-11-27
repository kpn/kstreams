import inspect
from typing import Any, get_origin

from di._container import BindHook
from di._utils.inspect import get_type
from di.api.dependencies import DependentBase


def bind_by_generic(
    provider: DependentBase[Any],
    dependency: type,
) -> BindHook:
    """Hook to substitute the matched dependency based on its generic."""

    def hook(
        param: inspect.Parameter | None, dependent: DependentBase[Any]
    ) -> DependentBase[Any] | None:
        if dependent.call == dependency:
            return provider
        if param is None:
            return None

        type_annotation_option = get_type(param)
        if type_annotation_option is None:
            return None
        type_annotation = type_annotation_option.value
        if get_origin(type_annotation) is dependency:
            return provider
        return None

    return hook
