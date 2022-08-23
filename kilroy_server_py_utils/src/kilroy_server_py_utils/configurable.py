import inspect
from typing import Any, Dict, Generic, Set, Type, TypeVar

from kilroy_server_py_utils.loadable import Loadable
from kilroy_server_py_utils.observable import (
    Observable,
    ReadOnlyObservableWrapper,
    ReadableObservable,
)
from kilroy_server_py_utils.parameters.base import Parameter
from kilroy_server_py_utils.schema import JSONSchema
from kilroy_server_py_utils.utils import classproperty, get_generic_args

T = TypeVar("T")
StateType = TypeVar("StateType")
ConfigurationType = TypeVar("ConfigurationType", bound="Configuration")
ConfigurableType = TypeVar("ConfigurableType", bound="Configurable")


class Configuration(Generic[StateType]):
    _parameters: Set[Parameter]
    _state: Loadable[StateType]
    _json: Observable[Dict[str, Any]]

    def __init__(
        self,
        parameters: Set[Parameter],
        state: Loadable[StateType],
        json: Observable[Dict[str, Any]],
    ) -> None:
        self._parameters = parameters
        self._state = state
        self._json = json

    @classmethod
    async def build(
        cls: Type[ConfigurationType], parameters: Set[Parameter], **kwargs
    ) -> ConfigurationType:
        return cls(
            parameters=parameters,
            state=await Loadable.build(),
            json=await Observable.build(),
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

    async def init(self, state: StateType) -> None:
        async with self._state.load() as (_, setter):
            await setter(state)
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


class Configurable(Generic[StateType]):
    _config: Configuration[StateType]

    def __init__(self, config: Configuration[StateType], **kwargs) -> None:
        self._config = config
        self._kwargs = kwargs

    @classmethod
    async def build(
        cls: Type[ConfigurableType], *args, **kwargs
    ) -> ConfigurableType:
        return cls(
            config=await Configuration.build(cls.parameters),
            *args,
            **kwargs,
        )

    @property
    def config(self) -> Configuration[StateType]:
        return self._config

    @property
    def state(self) -> Loadable[StateType]:
        return self._config.state

    async def build_default_state(self) -> StateType:
        # noinspection PyUnresolvedReferences
        return get_generic_args(self, Configurable)[0](**self._kwargs)

    async def init(self) -> None:
        state = await self.build_default_state()
        await self._config.init(state)

    async def cleanup(self) -> None:
        pass

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
