from typing import Protocol, runtime_checkable


@runtime_checkable
class Logger(Protocol):
    def debug(self, message: str, **kwargs: object) -> None:
        ...

    def info(self, message: str, **kwargs: object) -> None:
        ...

    def warning(self, message: str, **kwargs: object) -> None:
        ...

    def error(self, message: str, **kwargs: object) -> None:
        ...

    def exception(self, message: str, **kwargs: object) -> None:
        ...
