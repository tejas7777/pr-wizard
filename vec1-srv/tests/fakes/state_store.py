from typing import Dict, Optional

from vec1.core.ports.state_store import StateStore


class FakeStateStore(StateStore):
    def __init__(self, initial: Optional[Dict[str, str]] = None) -> None:
        self._store: Dict[str, str] = dict(initial or {})

    def read(self, key: str) -> Optional[str]:
        return self._store.get(key)

    def write(self, key: str, value: str) -> None:
        self._store[key] = value
