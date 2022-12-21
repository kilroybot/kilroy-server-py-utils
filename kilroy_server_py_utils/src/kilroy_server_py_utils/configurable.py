import inspect
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
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

    @classmethod
    async def create(
        cls: Type[ConfigurableType], **kwargs
    ) -> ConfigurableType:
        instance = await cls.build(**kwargs)
        await instance.init()
        return instance

    @property
    def config(self) -> Configuration[StateType]:
        return self._config

    @property
    def state(self) -> Loadable[StateType]:
        return self._config.state

    @classmethod
    async def _build_configurable(
        cls, configurable: Type[ConfigurableType], **kwargs
    ) -> ConfigurableType:
        instance = await configurable.build(**kwargs)
        await instance.init()
        return instance

    @classmethod
    async def _build_categorizable(
        cls, categorizable: Type[CategorizableType], category: str, **kwargs
    ) -> CategorizableType:
        subclass = categorizable.for_category(category)
        if issubclass(subclass, Configurable):
            # noinspection PyTypeChecker
            return await cls._build_configurable(subclass, **kwargs)
        return subclass(**kwargs)

    @classmethod
    async def _build_generic(cls, type_: Type[T], **kwargs) -> T:
        if issubclass(type_, Categorizable):
            return await cls._build_categorizable(type_, **kwargs)
        if issubclass(type_, Configurable):
            return await cls._build_configurable(type_, **kwargs)
        return type_(**kwargs)

    # noinspection PyMethodParameters
    @classproperty
    def _state_class(cls) -> Type[StateType]:
        return get_generic_args(cls, Configurable)[0]

    async def _build_default_state(self) -> StateType:
        return self._state_class(**self._kwargs)

    async def init(self) -> None:
        async with self._config.load() as setter:
            state = await self._build_default_state()
            await setter(state)

    async def cleanup(self) -> None:
        pass

    @staticmethod
    async def _save_state_dict(
        state_dict: Dict[str, Any], directory: Path, name: str = "state.json"
    ) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        with open(directory / name, "w") as f:
            json.dump(state_dict, f)

    @staticmethod
    async def _load_state_dict(
        directory: Path, name: str = "state.json"
    ) -> Dict[str, Any]:
        try:
            with open(directory / name, "r") as f:
                return json.load(f)
        except OSError:
            return {}

    @classmethod
    async def _save_state(cls, state: StateType, directory: Path) -> None:
        state_dict = vars(state)
        await cls._save_state_dict(state_dict, directory)

    async def save(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        async with self.state.read_lock() as state:
            await self._save_state(state, directory)

    @classmethod
    async def from_saved(
        cls: Type[ConfigurableType], directory: Path, **kwargs
    ) -> ConfigurableType:
        instance = await cls.build(**kwargs)
        await instance.load_saved(directory)
        return instance

    @classmethod
    async def _load_savable(
        cls, directory: Path, savable: Type[SavableType], **kwargs
    ) -> SavableType:
        return await savable.from_saved(directory, **kwargs)

    @classmethod
    async def _load_categorizable(
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
        return await cls._build_categorizable(
            categorizable, category, **kwargs
        )

    @classmethod
    async def _load_generic(
        cls,
        directory: Path,
        type_: Type[T],
        default: Optional[Callable[[], Awaitable[T]]] = None,
        logger: Optional[logging.Logger] = None,
        **kwargs,
    ) -> T:
        try:
            if issubclass(type_, Categorizable):
                return await cls._load_categorizable(
                    directory, type_, **kwargs
                )
            if issubclass(type_, Savable):
                return await cls._load_savable(directory, type_, **kwargs)
        except Exception as e:
            if logger is not None:
                logger.warning(f"Failed to load {type_}.", exc_info=e)

        if default is not None:
            return await default()

        return await cls._build_generic(type_, **kwargs)

    async def _load_saved_state(self, directory: Path) -> StateType:
        state_dict = await self._load_state_dict(directory)
        return self._state_class(**state_dict)

    async def load_saved(self, directory: Path) -> None:
        async with self._config.load() as setter:
            state = await self._load_saved_state(directory)
            await setter(state)

    # noinspection PyMethodParameters
    @classproperty
    def parameters(cls) -> Set[Type[Parameter]]:
        return {
            inner_cls
            for inner_cls in cls.__dict__.values()
            if inspect.isclass(inner_cls) and issubclass(inner_cls, Parameter)
        }

    # noinspection PyMethodParameters
    @classproperty
    def schema_name(cls) -> str:
        return f"{cls.__name__} configuration schema"

    # noinspection PyMethodParameters
    @classproperty
    def schema(cls) -> JSONSchema:
        schema = {
            "title": cls.schema_name,
            "type": "object",
            "required": cls.required_properties,
            "properties": cls.properties_schema,
        }
        return JSONSchema(**schema)

    # noinspection PyMethodParameters
    @classproperty
    def required_properties(cls) -> List[str]:
        return [
            parameter.name
            for parameter in cls.parameters
            if parameter.required
        ]

    # noinspection PyMethodParameters
    @classproperty
    def properties_schema(cls) -> Dict[str, Any]:
        return {
            parameter.name: parameter.schema for parameter in cls.parameters
        }
