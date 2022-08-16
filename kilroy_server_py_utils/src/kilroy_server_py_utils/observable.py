from abc import ABC, abstractmethod
from asyncio import Event, Lock, Queue
from contextlib import asynccontextmanager
from typing import AsyncIterable, Dict, Generic, Type, TypeVar
from uuid import UUID, uuid4

T = TypeVar("T")
ObservableType = TypeVar("ObservableType", bound="Observable")
ObservableImplType = TypeVar("ObservableImplType", bound="ObservableImpl")


class NotInitializedError(Exception):
    pass


class FetchableObservable(Generic[T], ABC):
    @abstractmethod
    async def fetch(self) -> T:
        pass

    @abstractmethod
    def subscribe(self) -> AsyncIterable[T]:
        pass


class ReadableObservable(FetchableObservable[T], Generic[T], ABC):
    @abstractmethod
    async def get(self) -> T:
        pass

    @property
    def fetch_only(self) -> FetchableObservable[T]:
        return FetchOnlyObservableWrapper(self)


class Observable(ReadableObservable[T], Generic[T], ABC):
    @abstractmethod
    async def set(self, value: T) -> None:
        pass

    @classmethod
    async def build(
        cls: Type[ObservableType], *args, **kwargs
    ) -> ObservableType:
        return await ObservableImpl.build(*args, **kwargs)

    @property
    def read_only(self) -> ReadableObservable[T]:
        return ReadOnlyObservableWrapper(self)


class ObservableImpl(Observable[T], Generic[T]):
    _value_lock: Lock
    _queue_lock: Lock
    _ready_event: Event
    _value: T
    _queues: Dict[UUID, Queue[T]]

    def __init__(
        self,
        *args,
        value_lock: Lock,
        queue_lock: Lock,
        ready_event: Event,
        **kwargs,
    ) -> None:
        self._value_lock = value_lock
        self._queue_lock = queue_lock
        self._ready_event = ready_event
        if len(args) > 0:
            self._value = args[0]
            self._ready_event.set()
        elif "value" in kwargs:
            self._value = kwargs["value"]
            self._ready_event.set()
        self._queues = {}

    @classmethod
    async def build(
        cls: Type[ObservableImplType], *args, **kwargs
    ) -> ObservableImplType:
        return cls(
            *args,
            value_lock=Lock(),
            queue_lock=Lock(),
            ready_event=Event(),
            **kwargs,
        )

    @asynccontextmanager
    async def _create_queue(self) -> Queue[T]:
        queue_id = uuid4()
        queue = Queue()

        async with self._queue_lock:
            self._queues[queue_id] = queue

        yield queue

        async with self._queue_lock:
            self._queues.pop(queue_id)

    async def _notify(self) -> None:
        async with self._queue_lock:
            for queue in self._queues.values():
                await queue.put(self._value)

    async def set(self, value: T) -> None:
        async with self._value_lock:
            if not self._ready_event.is_set():
                self._value = value
                await self._notify()
                self._ready_event.set()
            elif self._value != value:
                self._value = value
                await self._notify()

    async def get(self) -> T:
        async with self._value_lock:
            if not self._ready_event.is_set():
                raise NotInitializedError()
            return self._value

    async def fetch(self) -> T:
        await self._ready_event.wait()
        return await self.get()

    async def subscribe(self) -> AsyncIterable[T]:
        async with self._create_queue() as queue:
            while (message := await queue.get()) is not None:
                yield message


class FetchOnlyObservableWrapper(FetchableObservable[T], Generic[T]):
    _observable: FetchableObservable[T]

    def __init__(self, observable: FetchableObservable[T]) -> None:
        self._observable = observable

    async def fetch(self) -> T:
        return await self._observable.fetch()

    def subscribe(self) -> AsyncIterable[T]:
        return self._observable.subscribe()


class ReadOnlyObservableWrapper(ReadableObservable[T], Generic[T]):
    _observable: ReadableObservable[T]

    def __init__(self, observable: ReadableObservable[T]) -> None:
        self._observable = observable

    async def get(self) -> T:
        return await self._observable.get()

    async def fetch(self) -> T:
        return await self._observable.fetch()

    def subscribe(self) -> AsyncIterable[T]:
        return self._observable.subscribe()
