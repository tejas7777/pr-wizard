from datetime import datetime
from typing import Iterator, Protocol, runtime_checkable

from vec1.core.schema.pr import FetchedPR


@runtime_checkable
class PRSource(Protocol):
    def fetch_merged_since(self, since: datetime) -> Iterator[FetchedPR]:
        ...
