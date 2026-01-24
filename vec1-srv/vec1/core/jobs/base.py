from abc import ABC, abstractmethod
from typing import final

from vec1.core.exceptions import Vec1Error
from vec1.core.ports.clock import Clock
from vec1.core.ports.logger import Logger


class BaseJob(ABC):
    def __init__(
        self,
        logger: Logger,
        poll_interval: float,
        clock: Clock,
    ) -> None:
        self._logger = logger
        self._poll_interval = poll_interval
        self._clock = clock
        self._running = False

    @final
    def run(self) -> None:
        job_name = self.__class__.__name__
        self._running = True
        self._logger.info("Job starting", job=job_name)
        try:
            self.setup()
            while self._running and self.should_continue():
                try:
                    self.execute_once()
                except Vec1Error as error:
                    self.handle_error(error)
                if self._poll_interval > 0:
                    self._sleep(self._poll_interval)
        finally:
            try:
                self.teardown()
            finally:
                self._running = False
                self._logger.info("Job stopping", job=job_name)

    @abstractmethod
    def setup(self) -> None: ...

    @abstractmethod
    def execute_once(self) -> None: ...

    @abstractmethod
    def teardown(self) -> None: ...

    def handle_error(self, error: Vec1Error) -> None:
        self._logger.exception(
            "Job error",
            error=str(error),
            job=self.__class__.__name__,
        )

    def should_continue(self) -> bool:
        return True

    def stop(self) -> None:
        self._running = False

    def _sleep(self, duration: float) -> None:
        self._clock.sleep(duration)
