from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable, Dict, Generic, TypeVar

from humps import decamelize

from kilroy_server_py_utils.categorizable import Categorizable
from kilroy_server_py_utils.utils import classproperty, noop, normalize

StateType = TypeVar("StateType")
ParameterType = TypeVar("ParameterType")


class ParameterGetError(Exception):
    pass


class ParameterSetError(Exception):
    pass


class Parameter(Categorizable, Generic[StateType, ParameterType], ABC):
    async def get(self, state: StateType) -> ParameterType:
        try:
            return await self._get(state)
        except Exception as e:
            raise ParameterGetError() from e

    async def set(
        self,
        state: StateType,
        value: ParameterType,
    ) -> Callable[[], Awaitable]:
        if (await self.get(state)) == value:
            return noop
        try:
            return await self._set(state, value)
        except Exception as e:
            raise ParameterSetError() from e

    async def _get(self, state: StateType) -> ParameterType:
        return getattr(state, decamelize(self.name))

    async def _set(
        self,
        state: StateType,
        value: ParameterType,
    ) -> Callable[[], Awaitable]:
        name = decamelize(self.name)
        original_value = getattr(state, name)

        async def undo():
            setattr(state, name, original_value)

        setattr(state, name, value)
        return undo

    @classproperty
    def category(cls) -> str:
        return cls.name

    @classproperty
    def name(cls) -> str:
        class_name: str = cls.__name__
        name = class_name.removesuffix("Parameter").removeprefix("Parameter")
        return normalize(name) or "parameter"

    @classproperty
    @abstractmethod
    def schema(cls) -> Dict[str, Any]:
        pass
