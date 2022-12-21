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
    @classmethod
    async def _get(cls, state: StateType) -> Dict[str, Any]:
        configurable = await cls._get_configurable(state)
        return await configurable.config.json.fetch()

    @classmethod
    async def _set(
        cls, state: StateType, value: Dict[str, Any]
    ) -> Callable[[], Awaitable]:
        configurable = await cls._get_configurable(state)
        undo = await cls._create_undo(configurable)
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

    @classmethod
    async def _get_configurable(cls, state: StateType) -> ConfigurableType:
        return getattr(state, decamelize(cls.name))

    # noinspection PyMethodParameters
    @classproperty
    def configurable_class(cls) -> Type[ConfigurableType]:
        return get_generic_args(cls, NestedParameter)[1]

    # noinspection PyMethodParameters
    @classproperty
    def schema(cls) -> Dict[str, Any]:
        return {
            "type": "object",
            "title": cls.pretty_name,
            "default": {},
            "required": cls.configurable_class.required_properties,
            "properties": cls.configurable_class.properties_schema,
        }


class NestedOptionalParameter(
    OptionalParameter[StateType, Dict[str, Any]],
    ABC,
    Generic[StateType, ConfigurableType],
):
    @classmethod
    async def _get(cls, state: StateType) -> Optional[Dict[str, Any]]:
        configurable = await cls._get_configurable(state)
        if configurable is None:
            return None
        return await configurable.config.json.fetch()

    @classmethod
    async def _set(
        cls, state: StateType, value: Optional[Dict[str, Any]]
    ) -> Callable[[], Awaitable]:
        configurable = await cls._get_configurable(state)

        if value is None:
            undo = await cls._create_undo(state, configurable, None)
            await cls._set_configurable(state, None)
            if configurable is not None:
                await configurable.cleanup()
            return undo

        if configurable is None:
            params = await cls._get_params(state)
            new = await cls.configurable_class.create(**params)
            await new.config.set(value)
            undo = await cls._create_undo(state, None, new)
            await cls._set_configurable(state, new)
            return undo

        undo = await cls._create_undo(state, configurable, configurable)
        await configurable.config.set(value)
        return undo

    @classmethod
    async def _create_undo(
        cls,
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
                await cls._set_configurable(state, None)
                await new.cleanup()

            return undo

        tempdir = SelfDeletingDirectory()
        await old.save(tempdir.path)
        params = await cls._get_params(state)

        async def undo():
            loaded = await cls.configurable_class.from_saved(
                tempdir.path, **params
            )
            await cls._set_configurable(state, loaded)
            if new is not None:
                await new.cleanup()

        return undo

    @classmethod
    async def _get_configurable(
        cls, state: StateType
    ) -> Optional[ConfigurableType]:
        return getattr(state, decamelize(cls.name))

    @classmethod
    async def _set_configurable(
        cls, state: StateType, value: Optional[ConfigurableType]
    ) -> None:
        setattr(state, decamelize(cls.name), value)

    @classmethod
    async def _get_params(cls, state: StateType) -> Dict[str, Any]:
        return getattr(state, f"{decamelize(cls.name)}_params")

    # noinspection PyMethodParameters
    @classproperty
    def configurable_class(cls) -> Type[ConfigurableType]:
        return get_generic_args(cls, NestedOptionalParameter)[1]

    # noinspection PyMethodParameters
    @classproperty
    def schema(cls) -> Dict[str, Any]:
        return {
            "title": cls.pretty_name,
            "default": None,
            "oneOf": [
                {
                    "type": "object",
                    "title": "Yes",
                    "required": cls.configurable_class.required_properties,
                    "properties": cls.configurable_class.properties_schema,
                },
                {"type": "null", "title": "No"},
            ],
        }
