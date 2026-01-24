from collections.abc import Iterable
from datetime import datetime

from vec1.core.ports.clock import Clock


class FakeClock(Clock):
    def __init__(self, now_values: datetime | Iterable[datetime]) -> None:
        if isinstance(now_values, datetime):
            self._times = iter([now_values])
            self._repeat_last = True
        else:
            self._times = iter(now_values)
            self._repeat_last = False
        self._last: datetime | None = None
        self.slept: list[float] = []

    def now(self) -> datetime:
        try:
            self._last = next(self._times)
            return self._last
        except StopIteration:
            if self._repeat_last and self._last is not None:
                return self._last
            raise

    def sleep(self, duration: float) -> None:
        self.slept.append(duration)
