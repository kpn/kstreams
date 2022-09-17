import typing

import pytest
from di import Container
from di.dependent import Dependent
from di.executors import SyncExecutor

from kstreams._di.dependencies.hooks import bind_by_generic

KT = typing.TypeVar("KT")
VT = typing.TypeVar("VT")


class Record(typing.Generic[KT, VT]):
    def __init__(self, key: KT, value: VT):
        self.key = key
        self.value = value


def func_hinted(record: Record[str, int]) -> Record[str, int]:
    return record


def func_base(record: Record) -> Record:
    return record


@pytest.mark.parametrize(
    "func",
    [
        func_hinted,
        func_base,
    ],
)
def test_bind_generic_ok(func: typing.Callable):
    dep = Dependent(func)
    container = Container()
    container.bind(
        bind_by_generic(
            Dependent(lambda: Record("foo", 1), wire=False),
            Record,
        )
    )
    solved = container.solve(dep, scopes=[None])
    with container.enter_scope(None) as state:
        instance = solved.execute_sync(executor=SyncExecutor(), state=state)
    assert isinstance(instance, Record)


def func_str(record: str) -> str:
    return record


@pytest.mark.parametrize(
    "func",
    [
        func_str,
    ],
)
def test_bind_generic_unrelated(func: typing.Callable):
    dep = Dependent(func)
    container = Container()
    container.bind(
        bind_by_generic(
            Dependent(lambda: Record("foo", 1), wire=False),
            Record,
        )
    )
    solved = container.solve(dep, scopes=[None])
    with container.enter_scope(None) as state:
        instance = solved.execute_sync(executor=SyncExecutor(), state=state)
        print(type(instance))
        print(instance)
    assert not isinstance(instance, Record)
    assert isinstance(instance, str)
