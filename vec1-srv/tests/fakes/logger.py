from typing import Any

from vec1.core.ports.logger import Logger


class FakeLogger(Logger):
    def __init__(self) -> None:
        self.records: list[tuple[str, str, dict[str, Any]]] = []

    def debug(self, message: str, **kwargs: object) -> None:
        self.records.append(("debug", message, dict(kwargs)))

    def info(self, message: str, **kwargs: object) -> None:
        self.records.append(("info", message, dict(kwargs)))

    def warning(self, message: str, **kwargs: object) -> None:
        self.records.append(("warning", message, dict(kwargs)))

    def error(self, message: str, **kwargs: object) -> None:
        self.records.append(("error", message, dict(kwargs)))

    def exception(self, message: str, **kwargs: object) -> None:
        self.records.append(("exception", message, dict(kwargs)))
