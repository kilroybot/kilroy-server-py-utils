from abc import ABC, abstractmethod
from inspect import isabstract
from typing import Set, TypeVar

from humps import kebabize

from kilroy_server_py_utils.utils import classproperty

T = TypeVar("T")


def _get_all_subclasses(cls: T) -> Set[T]:
    all_subclasses = set()

    for subclass in cls.__subclasses__():
        all_subclasses.add(subclass)
        all_subclasses.update(_get_all_subclasses(subclass))

    return all_subclasses


class Categorizable(ABC):
    # noinspection PyMethodParameters
    @classproperty
    @abstractmethod
    def category(cls) -> str:
        pass

    # noinspection PyMethodParameters
    @classproperty
    def pretty_category(cls) -> str:
        return kebabize(cls.category).replace("-", " ").title()

    @classmethod
    def for_category(cls: T, category: str) -> T:
        for categorizable in cls.all_categorizables:
            if categorizable.category == category:
                return categorizable
        raise ValueError(f'Categorizable for category "{category}" not found.')

    # noinspection PyMethodParameters
    @classproperty
    def all_categorizables(cls: T) -> Set[T]:
        subclasses = _get_all_subclasses(cls)
        return {
            subclass for subclass in subclasses if not isabstract(subclass)
        }

    def __eq__(self, other):
        return (
            isinstance(other, Categorizable)
            and self.category == other.category
        )

    def __hash__(self) -> int:
        return hash(self.category)
