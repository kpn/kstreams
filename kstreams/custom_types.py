from typing import Any, Callable, Dict, Sequence, Tuple, TypeVar

DecoratedCallable = TypeVar("DecoratedCallable", bound=Callable[..., Any])
Headers = Dict[str, str]
KafkaHeaders = Sequence[Tuple[str, bytes]]
