from abc import ABC, abstractmethod
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
from kilroy_server_py_utils.parameters.base import Parameter, OptionalParameter
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
    @classmethod
    async def _get(cls, state: StateType) -> Dict[str, Any]:
        categorizable = await cls._get_categorizable(state)
        category = categorizable.category
        if isinstance(categorizable, Configurable):
            return {
                "type": category,
                "config": await categorizable.config.json.get(),
            }
        return {"type": category}

    @classmethod
    async def _set(
        cls, state: StateType, value: Dict[str, Any]
    ) -> Callable[[], Awaitable]:
        current = await cls._get_categorizable(state)
        new_category = value.get("type", current.category)

        if new_category == current.category:
            undo = await cls._create_undo(state, current, current)
            await current.config.set(value.get("config", {}))
            return undo

        params = await cls._get_params(state, new_category)
        subclass = cls.categorizable_base_class.for_category(new_category)
        if issubclass(subclass, Configurable):
            instance = await subclass.create(**params)
            await instance.config.set(value.get("config", {}))
        else:
            instance = subclass(**params)

        undo = await cls._create_undo(state, current, instance)
        await cls._set_categorizable(state, instance)
        if isinstance(current, Configurable):
            await current.cleanup()
        return undo

    @classmethod
    async def _create_undo(
        cls, state: StateType, old: Categorizable, new: Categorizable
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

            async def undo():
                # noinspection PyUnresolvedReferences
                await old.load_saved(tempdir.path)
                await cls._set_categorizable(state, old)
                if isinstance(new, Configurable):
                    await new.cleanup()

            return undo

        async def undo():
            await cls._set_categorizable(state, old)
            if isinstance(new, Configurable):
                await new.cleanup()

        return undo

    @classmethod
    async def _get_categorizable(cls, state: StateType) -> CategorizableType:
        return getattr(state, decamelize(cls.name))

    @classmethod
    async def _set_categorizable(
        cls, state: StateType, value: CategorizableType
    ) -> None:
        setattr(state, decamelize(cls.name), value)

    @classmethod
    async def _get_params(
        cls, state: StateType, category: str
    ) -> Dict[str, Any]:
        all_params = getattr(state, f"{decamelize(cls.name)}s_params")
        return all_params.get(category, {})

    # noinspection PyMethodParameters
    @classproperty
    def categorizable_base_class(cls) -> Type[CategorizableType]:
        return get_generic_args(cls, CategorizableBasedParameter)[1]

    # noinspection PyMethodParameters
    @classproperty
    def default(cls) -> Dict[str, Any]:
        categorizable = cls.default_categorizable
        category = categorizable.category

        if not issubclass(categorizable, Configurable):
            return {"type": category}

        config = cls.default_config

        if config is None:
            return {"type": category}

        return {"type": category, "config": config}

    # noinspection PyMethodParameters
    @classproperty
    @abstractmethod
    def default_categorizable(cls) -> Type[CategorizableType]:
        pass

    # noinspection PyMethodParameters
    @classproperty
    def default_config(cls) -> Optional[Dict[str, Any]]:
        ctg = cls.default_categorizable

        if ctg is None or not issubclass(ctg, Configurable):
            return None

        return {}

    # noinspection PyMethodParameters
    @classproperty
    def schema(cls) -> Dict[str, Any]:
        options = []
        for categorizable in cls.categorizable_base_class.all_categorizables:
            properties = {
                "type": {
                    "type": "string",
                    "title": "Type",
                    "const": categorizable.category,
                    "default": categorizable.category,
                    "readOnly": True,
                },
            }
            subclass = cls.categorizable_base_class.for_category(
                categorizable.category
            )
            if issubclass(subclass, Configurable):
                properties["config"] = {
                    "type": "object",
                    "title": "Configuration",
                    "required": subclass.required_properties,
                    "properties": subclass.properties_schema,
                }
            options.append(
                {
                    "title": categorizable.pretty_category,
                    "type": "object",
                    "properties": properties,
                }
            )
        return {
            "title": cls.pretty_name,
            "default": cls.default,
            "oneOf": options,
        }


# noinspection DuplicatedCode
class CategorizableBasedOptionalParameter(
    OptionalParameter[StateType, Dict[str, Any]],
    ABC,
    Generic[StateType, CategorizableType],
):
    @classmethod
    async def _get(cls, state: StateType) -> Optional[Dict[str, Any]]:
        categorizable = await cls._get_categorizable(state)
        if categorizable is None:
            return None
        category = categorizable.category
        if isinstance(categorizable, Configurable):
            return {
                "type": category,
                "config": await categorizable.config.json.get(),
            }
        return {"type": category}

    @classmethod
    async def _set(
        cls, state: StateType, value: Optional[Dict[str, Any]]
    ) -> Callable[[], Awaitable]:
        current = await cls._get_categorizable(state)

        if value is None:
            undo = await cls._create_undo(state, current, None)
            await cls._set_categorizable(state, None)
            if isinstance(current, Configurable):
                await current.cleanup()
            return undo

        if current is not None:
            new_category = value.get("type", current.category)
            if new_category == current.category:
                undo = await cls._create_undo(state, current, current)
                await current.config.set(value.get("config", {}))
                return undo
        else:
            new_category = value["type"]

        params = await cls._get_params(state, new_category)
        subclass = cls.categorizable_base_class.for_category(new_category)
        if issubclass(subclass, Configurable):
            instance = await subclass.create(**params)
            await instance.config.set(value.get("config", {}))
        else:
            instance = subclass(**params)

        undo = await cls._create_undo(state, current, instance)
        await cls._set_categorizable(state, instance)
        if isinstance(current, Configurable):
            await current.cleanup()
        return undo

    @classmethod
    async def _create_undo(
        cls,
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
                await cls._set_categorizable(state, None)
                if isinstance(new, Configurable):
                    await new.cleanup()

            return undo

        if isinstance(old, Configurable):
            tempdir = SelfDeletingDirectory()
            await old.save(tempdir.path)

            async def undo():
                # noinspection PyUnresolvedReferences
                await old.load_saved(tempdir.path)
                await cls._set_categorizable(state, old)
                if isinstance(new, Configurable):
                    await new.cleanup()

            return undo

        async def undo():
            await cls._set_categorizable(state, old)
            if isinstance(new, Configurable):
                await new.cleanup()

        return undo

    @classmethod
    async def _get_categorizable(
        cls, state: StateType
    ) -> Optional[CategorizableType]:
        return getattr(state, decamelize(cls.name))

    @classmethod
    async def _set_categorizable(
        cls, state: StateType, value: Optional[CategorizableType]
    ) -> None:
        setattr(state, decamelize(cls.name), value)

    @classmethod
    async def _get_params(
        cls, state: StateType, category: str
    ) -> Dict[str, Any]:
        all_params = getattr(state, f"{decamelize(cls.name)}s_params")
        return all_params.get(category, {})

    # noinspection PyMethodParameters
    @classproperty
    def categorizable_base_class(cls) -> Type[CategorizableType]:
        return get_generic_args(cls, CategorizableBasedOptionalParameter)[1]

    # noinspection PyMethodParameters
    @classproperty
    def default(cls) -> Optional[Dict[str, Any]]:
        categorizable = cls.default_categorizable

        if categorizable is None:
            return None

        category = categorizable.category

        if not issubclass(categorizable, Configurable):
            return {"type": category}

        config = cls.default_config

        if config is None:
            return {"type": category}

        return {"type": category, "config": config}

    # noinspection PyMethodParameters
    @classproperty
    def default_categorizable(cls) -> Optional[Type[CategorizableType]]:
        return None

    # noinspection PyMethodParameters
    @classproperty
    def default_config(cls) -> Optional[Dict[str, Any]]:
        ctg = cls.default_categorizable

        if ctg is None or not issubclass(ctg, Configurable):
            return None

        return {}

    # noinspection PyMethodParameters
    @classproperty
    def schema(cls) -> Dict[str, Any]:
        options: List[Dict[str, Any]] = []
        for categorizable in cls.categorizable_base_class.all_categorizables:
            properties = {
                "type": {
                    "type": "string",
                    "title": "Type",
                    "const": categorizable.category,
                    "default": categorizable.category,
                    "readOnly": True,
                },
            }
            subclass = cls.categorizable_base_class.for_category(
                categorizable.category
            )
            if issubclass(subclass, Configurable):
                properties["config"] = {
                    "type": "object",
                    "title": "Configuration",
                    "required": subclass.required_properties,
                    "properties": subclass.properties_schema,
                }
            options.append(
                {
                    "title": categorizable.pretty_category,
                    "type": "object",
                    "properties": properties,
                }
            )

        return {
            "title": cls.pretty_name,
            "default": cls.default,
            "oneOf": options + [{"title": "None", "type": "null"}],
        }
