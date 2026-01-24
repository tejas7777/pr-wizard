import logging
from typing import Any

from vec1.core.ports.logger import Logger


class _KeyValueFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        context = getattr(record, 'context', None)
        if context:
            pairs = ' '.join(
                f'{key}={value!r}' for key, value in context.items()
            )
            return f'{base} | {pairs}'
        return base


class ConsoleLogger(Logger):
    def __init__(self, name: str) -> None:
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.DEBUG)
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                _KeyValueFormatter('%(levelname)s %(name)s: %(message)s')
            )
            self._logger.addHandler(handler)
        self._logger.propagate = False

    def debug(self, message: str, **kwargs: Any) -> None:
        self._log(logging.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        self._log(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        self._log(logging.WARNING, message, **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        self._log(logging.ERROR, message, **kwargs)

    def exception(self, message: str, **kwargs: Any) -> None:
        self._logger.exception(message, extra={'context': kwargs})

    def _log(self, level: int, message: str, **kwargs: Any) -> None:
        self._logger.log(level, message, extra={'context': kwargs})
