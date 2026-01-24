from typing import Any

from vec1.core.ports.logger import Logger


def _load_logfire():
    try:
        import logfire
    except ImportError as error:
        raise RuntimeError('logfire library is not installed') from error
    return logfire


def configure_logfire(api_token: str) -> None:
    logfire = _load_logfire()
    logfire.configure(token=api_token)


class LogfireLogger(Logger):
    def __init__(self, name: str) -> None:
        logfire = _load_logfire()
        self._logger = logfire.get_logger(name)

    def debug(self, message: str, **kwargs: Any) -> None:
        self._logger.debug(message, **kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        self._logger.info(message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        self._logger.warning(message, **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        self._logger.error(message, **kwargs)

    def exception(self, message: str, **kwargs: Any) -> None:
        self._logger.exception(message, **kwargs)
