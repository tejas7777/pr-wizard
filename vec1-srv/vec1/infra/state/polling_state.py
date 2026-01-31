from datetime import datetime

from vec1.core.ports.polling_state import PollingState
from vec1.core.ports.state_store import StateStore


class PollingStateStore(PollingState):
    _LAST_POLLED_KEY = "polling.last_polled"

    def __init__(self, state_store: StateStore, save_state_in_dev: bool) -> None:
        self._state_store = state_store
        self._save_state_in_dev = save_state_in_dev

    def _allow_state_op(self):
        return self._save_state_in_dev

    def load_last_polled(self) -> datetime | None:
        if not self._allow_state_op():
            return None

        stored_timestamp = self._state_store.read(self._LAST_POLLED_KEY)
        if not stored_timestamp:
            return None
        return datetime.fromisoformat(stored_timestamp)

    def store_last_polled(self, value: datetime) -> None:
        if not self._allow_state_op():
            return
        self._state_store.write(self._LAST_POLLED_KEY, value.isoformat())
