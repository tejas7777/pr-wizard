from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class StateStore(Protocol):
    def read(self, key: str) -> Optional[str]:
        ...

    def write(self, key: str, value: str) -> None:
        ...
