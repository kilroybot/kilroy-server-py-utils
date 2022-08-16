from asyncio import Event
from contextlib import asynccontextmanager
from typing import (
    AsyncIterable,
    Awaitable,
    Callable,
    Generic,
    Tuple,
    Type,
    TypeVar,
)

from fifolock import FifoLock

from kilroy_server_py_utils.locks import Read, Write
from kilroy_server_py_utils.observable import (
    FetchableObservable,
    Observable,
    ReadOnlyObservableWrapper,
    ReadableObservable,
)

T = TypeVar("T")
LoadableType = TypeVar("LoadableType", bound="Loadable")


class EventBasedObservableWrapper(FetchableObservable[T], Generic[T]):
    _observable: Observable[T]
    _event: Event

    def __init__(self, observable: Observable, event: Event) -> None:
        self._observable = observable
        self._event = event

    async def fetch(self) -> T:
        await self._event.wait()
        return await self._observable.fetch()

    async def subscribe(self) -> AsyncIterable[T]:
        async for value in self._observable.subscribe():
            yield value


class ValueWrapper(Generic[T]):
    _value: T

    def __init__(self, getter: Callable[[], Awaitable[T]]) -> None:
        super().__init__()
        self._getter = getter

    @property
    def value(self) -> T:
        return self._value

    @property
    def has_value(self) -> bool:
        return hasattr(self, "_value")

    async def get(self) -> T:
        return await self._getter()

    async def set(self, value: T) -> None:
        self._value = value


class Loadable(Generic[T]):
    _value: Observable[T]
    _ready: Observable[bool]
    _event: Event
    _lock: FifoLock

    def __init__(
        self,
        value: Observable[T],
        ready: Observable[bool],
        event: Event,
        lock: FifoLock,
    ) -> None:
        self._value = value
        self._ready = ready
        self._event = event
        self._lock = lock
        self._event.set()

    @classmethod
    async def build(cls: Type[LoadableType], **kwargs) -> LoadableType:
        return cls(
            value=await Observable.build(),
            ready=await Observable.build(False),
            event=Event(),
            lock=FifoLock(),
            **kwargs,
        )

    @property
    def value(self) -> FetchableObservable[T]:
        return EventBasedObservableWrapper(self._value, self._event)

    @property
    def ready(self) -> ReadableObservable[bool]:
        return ReadOnlyObservableWrapper(self._ready)

    @asynccontextmanager
    async def read_lock(self) -> T:
        async with self._lock(Read):
            yield await self._value.fetch()

    @asynccontextmanager
    async def write_lock(self) -> T:
        async with self._lock(Write):
            yield await self._value.fetch()

    @asynccontextmanager
    async def load(
        self,
    ) -> Tuple[Callable[[], Awaitable[T]], Callable[[], Awaitable[None]]]:
        async with self._lock(Write):
            self._event.clear()
            ready = await self._ready.get()
            await self._ready.set(False)
            try:
                wrapper = ValueWrapper(self._value.get)
                yield wrapper.get, wrapper.set
                if wrapper.has_value:
                    await self._value.set(wrapper.value)
                    ready = True
            finally:
                await self._ready.set(ready)
                self._event.set()
