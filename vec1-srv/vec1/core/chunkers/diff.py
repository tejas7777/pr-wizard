import re
from typing import Callable, Iterator, List

from vec1.core.chunkers.base import BaseChunker
from vec1.core.schema.chunk import Chunk, ChunkType
from vec1.core.schema.pr import FetchedPR

DEFAULT_MAX_TOKENS = 2000
_HUNK_SPLIT_PATTERN = re.compile(r'\n(?=@@)')


class DiffChunker(BaseChunker):
    def __init__(
        self,
        hasher: Callable[[str], str],
        token_counter: Callable[[str], int],
        *,
        max_tokens: int = DEFAULT_MAX_TOKENS,
    ) -> None:
        super().__init__(hasher, token_counter)
        self._max_tokens = max_tokens

    def chunk(self, fetched_pr: FetchedPR) -> Iterator[Chunk]:
        for diff in fetched_pr.diffs:
            patch = diff.patch
            token_count = self._token_counter(patch)
            if token_count <= self._max_tokens:
                yield self._make_chunk(
                    pr_identifier=fetched_pr.metadata.identifier,
                    chunk_type=ChunkType.DIFF,
                    content=patch,
                    file_path=diff.file_path,
                    metadata={
                        'additions': diff.additions,
                        'deletions': diff.deletions,
                    },
                )
                continue

            hunks = self._split_patch(patch)
            hunk_count = len(hunks)
            for index, hunk in enumerate(hunks, start=1):
                hunk_metadata = {
                    'additions': diff.additions,
                    'deletions': diff.deletions,
                    'hunk_index': index,
                    'hunk_count': hunk_count,
                }
                yield self._make_chunk(
                    pr_identifier=fetched_pr.metadata.identifier,
                    chunk_type=ChunkType.DIFF,
                    content=hunk,
                    file_path=diff.file_path,
                    metadata=hunk_metadata,
                )

    def _split_patch(self, patch: str) -> List[str]:
        parts = _HUNK_SPLIT_PATTERN.split(patch)
        return parts or [patch]
