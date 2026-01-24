from abc import ABC, abstractmethod
from types import MappingProxyType
from typing import Callable, Iterator, Mapping, Optional

from vec1.core.schema.chunk import Chunk, ChunkType
from vec1.core.schema.pr import FetchedPR, PRIdentifier


class BaseChunker(ABC):
    def __init__(
        self,
        hasher: Callable[[str], str],
        token_counter: Callable[[str], int],
    ) -> None:
        self._hasher = hasher
        self._token_counter = token_counter

    @abstractmethod
    def chunk(self, fetched_pr: FetchedPR) -> Iterator[Chunk]:
        ...

    def _make_chunk(
        self,
        *,
        pr_identifier: PRIdentifier,
        chunk_type: ChunkType,
        content: str,
        file_path: Optional[str],
        metadata: Mapping[str, object] | None = None,
    ) -> Chunk:
        token_count = self._token_counter(content)
        content_hash = self._hasher(content)
        safe_metadata = MappingProxyType(dict(metadata or {}))
        return Chunk(
            pr_identifier=pr_identifier,
            chunk_type=chunk_type,
            content=content,
            file_path=file_path,
            content_hash=content_hash,
            token_count=token_count,
            metadata=safe_metadata,
        )
