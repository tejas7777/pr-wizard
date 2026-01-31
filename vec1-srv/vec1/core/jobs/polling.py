from datetime import datetime, timedelta

from vec1.core.jobs.base import BaseJob
from vec1.core.ports.clock import Clock
from vec1.core.ports.logger import Logger
from vec1.core.ports.polling_state import PollingState
from vec1.core.ports.pr_source import PRSource
from vec1.core.ports.queue import Queue
from vec1.core.schema.pr import FetchedPR


class PollingJob(BaseJob):
    def __init__(
        self,
        logger: Logger,
        poll_interval: float,
        pr_source: PRSource,
        output_queue: Queue[FetchedPR],
        polling_state: PollingState,
        clock: Clock,
        *,
        default_lookback_hours: int,
    ) -> None:
        super().__init__(logger, poll_interval, clock)
        self._pr_source = pr_source
        self._output_queue = output_queue
        self._polling_state = polling_state
        self._clock = clock
        self._default_lookback_hours = default_lookback_hours
        self._last_polled: datetime | None = None

    def setup(self) -> None:
        self._last_polled = self._polling_state.load_last_polled() or datetime.now()
        if self._last_polled is None:
            self._last_polled = self._clock.now() - timedelta(
                hours=self._default_lookback_hours
            )
        self._logger.info(
            "Polling initialized",
            last_polled=self._last_polled.isoformat(),
        )

    def execute_once(self) -> None:
        assert self._last_polled is not None
        count = 0
        for fetched_pr in self._pr_source.fetch_merged_since(self._last_polled):
            self._output_queue.put(fetched_pr)
            count += 1
        self._last_polled = self._clock.now()
        self._polling_state.store_last_polled(self._last_polled)
        self._logger.info(
            "Polling cycle complete",
            processed=count,
            last_polled=self._last_polled.isoformat(),
        )

    def teardown(self) -> None:
        if self._last_polled is not None:
            self._polling_state.store_last_polled(self._last_polled)
