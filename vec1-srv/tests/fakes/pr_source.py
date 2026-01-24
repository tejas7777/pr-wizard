from datetime import datetime
from typing import Iterable, Iterator, List

from vec1.core.ports.pr_source import PRSource
from vec1.core.schema.pr import FetchedPR


class FakePRSource(PRSource):
    def __init__(self, prs: Iterable[FetchedPR]) -> None:
        self._prs: List[FetchedPR] = list(prs)

    def fetch_merged_since(self, since: datetime) -> Iterator[FetchedPR]:
        for fetched in self._prs:
            merged_at = fetched.metadata.merged_at
            if merged_at is None or merged_at < since:
                continue
            yield fetched
