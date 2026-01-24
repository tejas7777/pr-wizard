from datetime import datetime
from typing import Protocol, runtime_checkable


@runtime_checkable
class PollingState(Protocol):
    def load_last_polled(self) -> datetime | None: ...

    def store_last_polled(self, value: datetime) -> None: ...
