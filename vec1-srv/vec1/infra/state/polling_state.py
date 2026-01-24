from datetime import datetime

from vec1.core.ports.polling_state import PollingState
from vec1.core.ports.state_store import StateStore


class PollingStateStore(PollingState):
    _LAST_POLLED_KEY = 'polling.last_polled'

    def __init__(self, state_store: StateStore) -> None:
        self._state_store = state_store

    def load_last_polled(self) -> datetime | None:
        stored_timestamp = self._state_store.read(self._LAST_POLLED_KEY)
        if not stored_timestamp:
            return None
        return datetime.fromisoformat(stored_timestamp)

    def store_last_polled(self, value: datetime) -> None:
        self._state_store.write(self._LAST_POLLED_KEY, value.isoformat())
