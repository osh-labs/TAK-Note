from abc import ABC, abstractmethod

class Plugin(ABC):
    metadata: dict = {}

    @property
    @abstractmethod
    def group(self) -> str: ...

    @abstractmethod
    def activate(self, app, enabled: bool) -> None: ...

    @abstractmethod
    def stop(self) -> None: ...

    @abstractmethod
    def get_info(self) -> dict | None: ...

    @abstractmethod
    def load_metadata(self) -> dict: ...
