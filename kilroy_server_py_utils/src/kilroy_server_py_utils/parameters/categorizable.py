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

from kilroy_server_py_utils.categorizable import Categorizable
from kilroy_server_py_utils.configurable import Configurable
from kilroy_server_py_utils.parameters.base import Parameter
from kilroy_server_py_utils.utils import (
    SelfDeletingDirectory,
    classproperty,
    get_generic_args,
    noop,
)

StateType = TypeVar("StateType")
CategorizableType = TypeVar("CategorizableType", bound=Categorizable)


class CategorizableBasedParameter(
    Parameter[StateType, Dict[str, Any]],
    ABC,
    Generic[StateType, CategorizableType],
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
        new_category = value.get("type", current.category)

        if new_category == current.category:
            undo = await self._create_undo(state, current, current)
            await current.config.set(value.get("config", {}))
            return undo

        params = await self._get_params(state, new_category)
        subclass = self.categorizable_base_class.for_category(new_category)
        if issubclass(subclass, Configurable):
            instance = await subclass.create(**params)
            await instance.config.set(value.get("config", {}))
        else:
            instance = subclass(**params)

        undo = await self._create_undo(state, current, instance)
        await self._set_categorizable(state, instance)
        if isinstance(current, Configurable):
            await current.cleanup()
        return undo

    async def _create_undo(
        self, state: StateType, old: Categorizable, new: Categorizable
    ) -> Callable[[], Awaitable]:
        if old is new:
            if not isinstance(old, Configurable):
                return noop

            config = await old.config.json.get()

            async def undo():
                # noinspection PyUnresolvedReferences
                await old.config.set(config)

            return undo

        if isinstance(old, Configurable):
            tempdir = SelfDeletingDirectory()
            await old.save(tempdir.path)
            # noinspection PyUnresolvedReferences
            params = await self._get_params(state, old.category)

            async def undo():
                loaded = await old.from_saved(tempdir.path, **params)
                await self._set_categorizable(state, loaded)
                if isinstance(new, Configurable):
                    await new.cleanup()

            return undo

        async def undo():
            await self._set_categorizable(state, old)
            if isinstance(new, Configurable):
                await new.cleanup()

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
        return get_generic_args(cls, CategorizableBasedParameter)[1]

    @classproperty
    def schema(cls) -> Dict[str, Any]:
        possible_properties = []
        for category in cls.categorizable_base_class.all_categories:
            properties = {
                "type": {"type": "string", "const": category},
            }
            subclass = cls.categorizable_base_class.for_category(category)
            if issubclass(subclass, Configurable):
                properties["config"] = {
                    "type": "object",
                    "required": subclass.required_properties,
                    "properties": subclass.properties_schema,
                }
            possible_properties.append(properties)
        return {
            "oneOf": [
                {"type": "object", "properties": properties}
                for properties in possible_properties
            ],
        }


# noinspection DuplicatedCode
class CategorizableBasedOptionalParameter(
    Parameter[StateType, Optional[Dict[str, Any]]],
    ABC,
    Generic[StateType, CategorizableType],
):
    async def _get(self, state: StateType) -> Optional[Dict[str, Any]]:
        categorizable = await self._get_categorizable(state)
        if categorizable is None:
            return None
        category = categorizable.category
        if isinstance(categorizable, Configurable):
            return {
                "type": category,
                "config": await categorizable.config.json.get(),
            }
        return {"type": category}

    async def _set(
        self, state: StateType, value: Optional[Dict[str, Any]]
    ) -> Callable[[], Awaitable]:
        current = await self._get_categorizable(state)

        if value is None:
            undo = await self._create_undo(state, current, None)
            await self._set_categorizable(state, None)
            if isinstance(current, Configurable):
                await current.cleanup()
            return undo

        if current is not None:
            new_category = value.get("type", current.category)
            if new_category == current.category:
                undo = await self._create_undo(state, current, current)
                await current.config.set(value.get("config", {}))
                return undo
        else:
            new_category = value["type"]

        params = await self._get_params(state, new_category)
        subclass = self.categorizable_base_class.for_category(new_category)
        if issubclass(subclass, Configurable):
            instance = await subclass.create(**params)
            await instance.config.set(value.get("config", {}))
        else:
            instance = subclass(**params)

        undo = await self._create_undo(state, current, instance)
        await self._set_categorizable(state, instance)
        if isinstance(current, Configurable):
            await current.cleanup()
        return undo

    async def _create_undo(
        self,
        state: StateType,
        old: Optional[Categorizable],
        new: Optional[Categorizable],
    ) -> Callable[[], Awaitable]:
        if old is None and new is None:
            return noop

        if old is new:
            if not isinstance(old, Configurable):
                return noop

            config = await old.config.json.get()

            async def undo():
                # noinspection PyUnresolvedReferences
                await old.config.set(config)

            return undo

        if old is None:

            async def undo():
                await self._set_categorizable(state, None)
                if isinstance(new, Configurable):
                    await new.cleanup()

            return undo

        if isinstance(old, Configurable):
            tempdir = SelfDeletingDirectory()
            await old.save(tempdir.path)
            # noinspection PyUnresolvedReferences
            params = await self._get_params(state, old.category)

            async def undo():
                loaded = await old.from_saved(tempdir.path, **params)
                await self._set_categorizable(state, loaded)
                if isinstance(new, Configurable):
                    await new.cleanup()

            return undo

        async def undo():
            await self._set_categorizable(state, old)
            if isinstance(new, Configurable):
                await new.cleanup()

        return undo

    async def _get_categorizable(
        self, state: StateType
    ) -> Optional[CategorizableType]:
        return getattr(state, decamelize(self.name))

    async def _set_categorizable(
        self, state: StateType, value: Optional[CategorizableType]
    ) -> None:
        setattr(state, decamelize(self.name), value)

    async def _get_params(
        self, state: StateType, category: str
    ) -> Dict[str, Any]:
        all_params = getattr(state, f"{decamelize(self.name)}s_params")
        return all_params.get(category, {})

    @classproperty
    def categorizable_base_class(cls) -> Type[CategorizableType]:
        return get_generic_args(cls, CategorizableBasedOptionalParameter)[1]

    @classproperty
    def schema(cls) -> Dict[str, Any]:
        possible_properties = []
        for category in cls.categorizable_base_class.all_categories:
            properties = {
                "type": {"type": "string", "const": category},
            }
            subclass = cls.categorizable_base_class.for_category(category)
            if issubclass(subclass, Configurable):
                properties["config"] = {
                    "type": "object",
                    "required": subclass.required_properties,
                    "properties": subclass.properties_schema,
                }
            possible_properties.append(properties)
        return {
            "oneOf": [
                {"type": "object", "properties": properties}
                for properties in possible_properties
            ]
            + [{"type": "null"}],
        }
