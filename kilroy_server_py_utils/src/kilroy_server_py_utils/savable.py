from abc import ABC, abstractmethod
from pathlib import Path
from typing import Type, TypeVar

SavableType = TypeVar("SavableType", bound="Savable")


class Savable(ABC):
    @abstractmethod
    async def save(self, directory: Path) -> None:
        pass

    @classmethod
    @abstractmethod
    async def from_saved(
        cls: Type[SavableType], directory: Path, **kwargs
    ) -> SavableType:
        pass
