from abc import ABC
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Generic,
    Optional,
    Type,
    TypeVar,
)

from humps import decamelize

from kilroy_server_py_utils.configurable import Configurable
from kilroy_server_py_utils.parameters.base import OptionalParameter, Parameter
from kilroy_server_py_utils.utils import (
    SelfDeletingDirectory,
    classproperty,
    get_generic_args,
    noop,
)

StateType = TypeVar("StateType")
ParameterType = TypeVar("ParameterType")
ConfigurableType = TypeVar("ConfigurableType", bound=Configurable)


class NestedParameter(
    Parameter[StateType, Dict[str, Any]],
    ABC,
    Generic[StateType, ConfigurableType],
):
    async def _get(self, state: StateType) -> Dict[str, Any]:
        configurable = await self._get_configurable(state)
        return await configurable.config.json.fetch()

    async def _set(
        self, state: StateType, value: Dict[str, Any]
    ) -> Callable[[], Awaitable]:
        configurable = await self._get_configurable(state)
        undo = await self._create_undo(configurable)
        await configurable.config.set(value)
        return undo

    @staticmethod
    async def _create_undo(
        configurable: Configurable,
    ) -> Callable[[], Awaitable]:
        original_config = await configurable.config.json.get()

        async def undo():
            await configurable.config.set(original_config)

        return undo

    async def _get_configurable(self, state: StateType) -> ConfigurableType:
        return getattr(state, decamelize(self.name))

    @classproperty
    def configurable_class(cls) -> Type[ConfigurableType]:
        return get_generic_args(cls, NestedParameter)[1]

    @classproperty
    def schema(cls) -> Dict[str, Any]:
        return {
            "type": "object",
            "title": cls.pretty_name,
            "required": cls.configurable_class.required_properties,
            "properties": cls.configurable_class.properties_schema,
        }


class NestedOptionalParameter(
    OptionalParameter[StateType, Dict[str, Any]],
    ABC,
    Generic[StateType, ConfigurableType],
):
    async def _get(self, state: StateType) -> Optional[Dict[str, Any]]:
        configurable = await self._get_configurable(state)
        if configurable is None:
            return None
        return await configurable.config.json.fetch()

    async def _set(
        self, state: StateType, value: Optional[Dict[str, Any]]
    ) -> Callable[[], Awaitable]:
        configurable = await self._get_configurable(state)

        if value is None:
            undo = await self._create_undo(state, configurable, None)
            await self._set_configurable(state, None)
            if configurable is not None:
                await configurable.cleanup()
            return undo

        if configurable is None:
            params = await self._get_params(state)
            new = await self.configurable_class.create(**params)
            await new.config.set(value)
            undo = await self._create_undo(state, None, new)
            await self._set_configurable(state, new)
            return undo

        undo = await self._create_undo(state, configurable, configurable)
        await configurable.config.set(value)
        return undo

    async def _create_undo(
        self,
        state: StateType,
        old: Optional[Configurable],
        new: Optional[Configurable],
    ) -> Callable[[], Awaitable]:
        if old is None and new is None:
            return noop

        if old is new:
            config = await old.config.json.get()

            async def undo():
                await old.config.set(config)

            return undo

        if old is None:

            async def undo():
                await self._set_configurable(state, None)
                await new.cleanup()

            return undo

        tempdir = SelfDeletingDirectory()
        await old.save(tempdir.path)
        params = await self._get_params(state)

        async def undo():
            loaded = await self.configurable_class.from_saved(
                tempdir.path, **params
            )
            await self._set_configurable(state, loaded)
            if new is not None:
                await new.cleanup()

        return undo

    async def _get_configurable(
        self, state: StateType
    ) -> Optional[ConfigurableType]:
        return getattr(state, decamelize(self.name))

    async def _set_configurable(
        self, state: StateType, value: Optional[ConfigurableType]
    ) -> None:
        setattr(state, decamelize(self.name), value)

    async def _get_params(self, state: StateType) -> Dict[str, Any]:
        return getattr(state, f"{decamelize(self.name)}_params")

    @classproperty
    def configurable_class(cls) -> Type[ConfigurableType]:
        return get_generic_args(cls, NestedOptionalParameter)[1]

    @classproperty
    def schema(cls) -> Dict[str, Any]:
        return {
            "type": ["object", "null"],
            "title": cls.pretty_name,
            "required": cls.configurable_class.required_properties,
            "properties": cls.configurable_class.properties_schema,
        }
