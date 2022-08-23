from abc import ABC
from typing import Any, Awaitable, Callable, Dict, Generic, Type, TypeVar

from humps import decamelize

from kilroy_server_py_utils.categorizable import Categorizable
from kilroy_server_py_utils.configurable import Configurable
from kilroy_server_py_utils.parameters.base import Parameter
from kilroy_server_py_utils.utils import classproperty, get_generic_args, noop

StateType = TypeVar("StateType")
ParameterType = TypeVar("ParameterType")
ConfigurableType = TypeVar("ConfigurableType", bound=Configurable)
CategorizableType = TypeVar("CategorizableType", bound=Categorizable)


class NestedParameter(
    Parameter[StateType, Dict[str, Any]],
    Generic[StateType, ConfigurableType],
    ABC,
):
    async def _get(self, state: StateType) -> Dict[str, Any]:
        configurable = await self._get_configurable(state)
        return await configurable.config.json.fetch()

    async def _set(
        self, state: StateType, value: Dict[str, Any]
    ) -> Callable[[], Awaitable]:
        configurable = await self._get_configurable(state)
        original_value = await configurable.config.json.get()

        async def undo():
            await configurable.config.set(original_value)

        await configurable.config.set(value)
        return undo

    async def _get_configurable(self, state: StateType) -> ConfigurableType:
        return getattr(state, decamelize(self.name))

    @classproperty
    def configurable_class(cls) -> Type[ConfigurableType]:
        # noinspection PyUnresolvedReferences
        return get_generic_args(cls, NestedParameter)[1]

    @classproperty
    def schema(cls) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": cls.configurable_class.properties_schema,
        }


class CategorizableBasedParameter(
    Parameter[StateType, Dict[str, Any]],
    Generic[StateType, CategorizableType],
    ABC,
):
    async def _get(self, state: StateType) -> Dict[str, Any]:
        categorizable = await self._get_categorizable(state)
        category = categorizable.category
        if isinstance(categorizable, Configurable):
            return {
                "type": category,
                "config": await categorizable.config.json.get(),
            }
        return {"type": category}

    async def _set(
        self, state: StateType, value: Dict[str, Any]
    ) -> Callable[[], Awaitable]:
        current = await self._get_categorizable(state)
        category = value.get("type", current.category)

        current = await self._get_categorizable(state)
        if current.category == category:
            if isinstance(current, Configurable):
                original_value = await current.config.json.get()

                async def undo():
                    await current.config.set(original_value)

                await current.config.set(value.get("config", {}))
                return undo

            return noop

        if isinstance(current, Configurable):
            await current.cleanup()

        params = await self._get_params(state, category)
        cls = self.categorizable_base_class.for_category(category)
        if issubclass(cls, Configurable):
            instance = await cls.build(**params)
            await instance.init()
            original_config = await instance.config.json.get()

            async def undo():
                await instance.config.set(original_config)
                await self._set_categorizable(state, current)

            await instance.config.set(value.get("config", {}))
            # noinspection PyTypeChecker
            await self._set_categorizable(state, instance)

            return undo
        else:
            instance = cls(**params)

            async def undo():
                await self._set_categorizable(state, current)

            await self._set_categorizable(state, instance)

            return undo

    async def _get_categorizable(self, state: StateType) -> CategorizableType:
        return getattr(state, decamelize(self.name))

    async def _set_categorizable(
        self, state: StateType, value: CategorizableType
    ) -> None:
        setattr(state, decamelize(self.name), value)

    async def _get_params(
        self, state: StateType, category: str
    ) -> Dict[str, Any]:
        all_params = getattr(state, f"{decamelize(self.name)}s_params")
        return all_params.get(category, {})

    @classproperty
    def categorizable_base_class(cls) -> Type[CategorizableType]:
        # noinspection PyUnresolvedReferences
        return get_generic_args(cls, CategorizableBasedParameter)[1]

    @classproperty
    def schema(cls) -> Dict[str, Any]:
        possible_properties = []
        for category in cls.categorizable_base_class.all_categories:
            properties = {
                "type": {"type": "string", "const": category},
            }
            sampler_class = cls.categorizable_base_class.for_category(category)
            if issubclass(sampler_class, Configurable):
                properties["config"] = {
                    "type": "object",
                    "properties": sampler_class.properties_schema,
                }
            possible_properties.append(properties)
        return {
            "type": "object",
            "oneOf": [
                {"type": "object", "properties": properties}
                for properties in possible_properties
            ],
        }
