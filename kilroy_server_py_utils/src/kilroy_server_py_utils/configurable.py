import inspect
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Generic,
    Set,
    Type,
    TypeVar,
)

from kilroy_server_py_utils.categorizable import Categorizable
from kilroy_server_py_utils.loadable import Loadable
from kilroy_server_py_utils.observable import (
    Observable,
    ReadOnlyObservableWrapper,
    ReadableObservable,
)
from kilroy_server_py_utils.parameters.base import Parameter
from kilroy_server_py_utils.savable import Savable
from kilroy_server_py_utils.schema import JSONSchema
from kilroy_server_py_utils.utils import classproperty, get_generic_args

T = TypeVar("T")
StateType = TypeVar("StateType")
ConfigurationType = TypeVar("ConfigurationType", bound="Configuration")
ConfigurableType = TypeVar("ConfigurableType", bound="Configurable")
CategorizableType = TypeVar("CategorizableType", bound=Categorizable)
SavableType = TypeVar("SavableType", bound=Savable)


class Configuration(Generic[StateType]):
    _parameters: Set[Parameter]
    _state: Loadable[StateType]
    _json: Observable[Dict[str, Any]]

    def __init__(
        self,
        parameters: Set[Parameter],
        state: Loadable[StateType],
        json_: Observable[Dict[str, Any]],
    ) -> None:
        self._parameters = parameters
        self._state = state
        self._json = json_

    @classmethod
    async def build(
        cls: Type[ConfigurationType], parameters: Set[Parameter], **kwargs
    ) -> ConfigurationType:
        return cls(
            parameters=parameters,
            state=await Loadable.build(),
            json_=await Observable.build(),
            **kwargs,
        )

    @property
    def state(self) -> Loadable[StateType]:
        return self._state

    @property
    def json(self) -> ReadableObservable[Dict[str, Any]]:
        return ReadOnlyObservableWrapper(self._json)

    async def _build_json(self) -> Dict[str, Any]:
        state = await self._state.value.fetch()
        return {
            parameter.name: await parameter.get(state)
            for parameter in self._parameters
        }

    @asynccontextmanager
    async def load(
        self,
    ) -> AsyncIterator[Callable[[StateType], Awaitable[None]]]:
        async with self._state.load() as (_, setter):
            yield setter
        await self._json.set(await self._build_json())

    async def set(self, config: Dict[str, Any]) -> Dict[str, Any]:
        params_map = {
            parameter.name: parameter for parameter in self._parameters
        }

        async with self._state.load() as (getter, setter):
            state = await getter()
            undos = []

            try:
                for name, value in config.items():
                    parameter = params_map.get(name, None)
                    if parameter is not None:
                        undo = await parameter.set(state, value)
                        undos.append(undo)
                await setter(state)
            except Exception as e:
                for undo in reversed(undos):
                    try:
                        await undo()
                    except Exception:
                        pass
                raise e

        config = await self._build_json()
        await self._json.set(config)
        return config


class Configurable(Savable, Generic[StateType]):
    _config: Configuration[StateType]

    def __init__(self, config: Configuration[StateType], **kwargs) -> None:
        self._config = config
        self._kwargs = kwargs

    @classmethod
    async def build(cls: Type[ConfigurableType], **kwargs) -> ConfigurableType:
        return cls(config=await Configuration.build(cls.parameters), **kwargs)

    @property
    def config(self) -> Configuration[StateType]:
        return self._config

    @property
    def state(self) -> Loadable[StateType]:
        return self._config.state

    @classmethod
    async def build_configurable(
        cls, configurable: Type[ConfigurableType], **kwargs
    ) -> ConfigurableType:
        instance = await configurable.build(**kwargs)
        await instance.init()
        return instance

    @classmethod
    async def build_categorizable(
        cls, categorizable: Type[CategorizableType], category: str, **kwargs
    ) -> CategorizableType:
        subclass = categorizable.for_category(category)
        if issubclass(subclass, Configurable):
            # noinspection PyTypeChecker
            return await cls.build_configurable(subclass, **kwargs)
        return subclass(**kwargs)

    @classmethod
    async def build_generic(cls, type_: Type[T], **kwargs) -> T:
        if issubclass(type_, Categorizable):
            return await cls.build_categorizable(type_, **kwargs)
        if issubclass(type_, Configurable):
            return await cls.build_configurable(type_, **kwargs)
        return type_(**kwargs)

    @classproperty
    def state_class(cls) -> Type[StateType]:
        return get_generic_args(cls, Configurable)[0]

    async def build_default_state(self) -> StateType:
        return self.state_class(**self._kwargs)

    async def init(self) -> None:
        async with self._config.load() as setter:
            state = await self.build_default_state()
            await setter(state)

    async def cleanup(self) -> None:
        pass

    @classmethod
    async def save_state(cls, state: StateType, directory: Path) -> None:
        with open(directory / "state.json", "w") as f:
            json.dump(vars(state), f)

    async def save(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        async with self.state.read_lock() as state:
            await self.save_state(state, directory)

    @classmethod
    async def from_saved(
        cls: Type[ConfigurableType], directory: Path, **kwargs
    ) -> ConfigurableType:
        instance = await cls.build(**kwargs)
        await instance.load_saved(directory)
        return instance

    @classmethod
    async def load_savable(
        cls, directory: Path, savable: Type[SavableType]
    ) -> SavableType:
        return await savable.from_saved(directory)

    @classmethod
    async def load_categorizable(
        cls,
        directory: Path,
        categorizable: Type[CategorizableType],
        category: str,
        **kwargs,
    ) -> CategorizableType:
        subclass = categorizable.for_category(category)
        if issubclass(subclass, Savable):
            # noinspection PyTypeChecker
            return await subclass.from_saved(directory, **kwargs)
        return await cls.build_categorizable(categorizable, category, **kwargs)

    @classmethod
    async def load_generic(
        cls, directory: Path, type_: Type[T], **kwargs
    ) -> T:
        if issubclass(type_, Categorizable):
            return await cls.load_categorizable(directory, type_, **kwargs)
        if issubclass(type_, Savable):
            return await cls.load_savable(directory, type_)
        return await cls.build_generic(type_, **kwargs)

    async def load_saved_state(self, directory: Path) -> StateType:
        with open(directory / "state.json", "r") as f:
            state_dict = json.load(f)
        return self.state_class(**state_dict)

    async def load_saved(self, directory: Path) -> None:
        async with self._config.load() as setter:
            state = await self.load_saved_state(directory)
            await setter(state)

    @classproperty
    def parameters(cls) -> Set[Parameter]:
        return {
            inner_cls()
            for inner_cls in cls.__dict__.values()
            if inspect.isclass(inner_cls) and issubclass(inner_cls, Parameter)
        }

    @classproperty
    def schema_name(cls) -> str:
        return f"{cls.__name__} config schema"

    @classproperty
    def schema(cls) -> JSONSchema:
        # noinspection PyArgumentList
        return JSONSchema(
            title=cls.schema_name,
            type="object",
            properties=cls.properties_schema,
        )

    @classproperty
    def properties_schema(cls) -> Dict[str, Any]:
        return {
            parameter.name: parameter.schema for parameter in cls.parameters
        }
