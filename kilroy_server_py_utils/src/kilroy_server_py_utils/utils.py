import inspect
import weakref
from asyncio import AbstractEventLoop, get_running_loop
from base64 import urlsafe_b64decode, urlsafe_b64encode
from concurrent.futures import Executor
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import (
    Callable,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    _GenericAlias,
    get_args,
    Iterable,
    AsyncIterable,
)

from aiostream.stream import iterate, chain, take, preserve
from humps import camelize, kebabize

T = TypeVar("T")


def get_generic_args(ref: Union[T, Type[T]], cls: Type[T]) -> Tuple:
    bases = ref.__orig_bases__
    generic_base = next(
        base
        for base in bases
        if isinstance(base, _GenericAlias) and issubclass(base.__origin__, cls)
    )
    return get_args(generic_base)


def can_call_with_arg(f: Callable, arg_name: str) -> bool:
    try:
        inspect.signature(f).bind_partial(**{arg_name: None})
    except TypeError:
        return False

    return True


class SelfDeletingDirectory:
    def __init__(self, *args, **kwargs):
        self._tempdir = TemporaryDirectory(*args, **kwargs)
        weakref.finalize(self, self.cleanup)

    @property
    def path(self) -> Path:
        return Path(self._tempdir.name)

    def cleanup(self):
        self._tempdir.cleanup()


class classproperty:
    def __init__(self, method=None):
        self.fget = method

    def __get__(self, instance, cls=None):
        return self.fget(cls)

    def getter(self, method):
        self.fget = method
        return self


def base64_encode(value: bytes) -> str:
    return urlsafe_b64encode(value).decode("ascii")


def base64_decode(value: str) -> bytes:
    return urlsafe_b64decode(value.encode("ascii"))


async def noop(*args, **kwargs) -> None:
    pass


async def background(
    f: Callable[..., T],
    *args,
    loop: Optional[AbstractEventLoop] = None,
    executor: Optional[Executor] = None,
    **kwargs,
) -> T:
    loop = loop or get_running_loop()
    f = partial(f, *args, **kwargs)
    return await loop.run_in_executor(executor, f)


def normalize(name: str) -> str:
    name = name.lower() if name.isupper() else name
    return camelize(kebabize(name))


async def batchify(
    iterable: Union[Iterable[T], AsyncIterable[T]],
    n: Optional[int],
) -> AsyncIterable[AsyncIterable[T]]:
    xs = iterate(iterable)

    async with xs.stream() as streamer:
        if n is None:
            yield streamer
            return

        async for first in streamer:

            async def afirst():
                yield first

            batch = chain(afirst(), take(preserve(streamer), n - 1))

            async with batch.stream() as batch_streamer:
                yield batch_streamer
