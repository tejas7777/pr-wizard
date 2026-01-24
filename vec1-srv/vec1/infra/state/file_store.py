import os
from pathlib import Path
from typing import Optional

from vec1.core.ports.state_store import StateStore


class FileStateStore(StateStore):
    def __init__(self, base_dir: str | Path) -> None:
        self._base_dir = Path(base_dir)

    def read(self, key: str) -> Optional[str]:
        path = self._path_for(key)
        if not path.exists():
            return None
        return path.read_text(encoding="utf-8")

    def write(self, key: str, value: str) -> None:
        path = self._path_for(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_suffix(path.suffix + ".tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        temp_path.replace(path)

    def _path_for(self, key: str) -> Path:
        safe_key = key.replace("/", "_")
        return self._base_dir / f"{safe_key}.state"
