import time
from datetime import datetime, timezone

from vec1.core.ports.clock import Clock


class SystemClock(Clock):
    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def sleep(self, duration: float) -> None:
        time.sleep(duration)
