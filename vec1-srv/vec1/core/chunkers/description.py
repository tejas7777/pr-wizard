from typing import Iterator

from vec1.core.chunkers.base import BaseChunker
from vec1.core.schema.chunk import Chunk, ChunkType
from vec1.core.schema.pr import FetchedPR


class DescriptionChunker(BaseChunker):
    def chunk(self, fetched_pr: FetchedPR) -> Iterator[Chunk]:
        metadata = fetched_pr.metadata
        content_parts = [metadata.title]
        if metadata.description:
            content_parts.append("")
            content_parts.append(metadata.description)
        content = "\n".join(content_parts)
        chunk_metadata = {
            "author": metadata.author,
            "created_at": metadata.created_at,
            "merged_at": metadata.merged_at,
            "base_branch": metadata.base_branch,
            "head_branch": metadata.head_branch,
            "files_changed": metadata.files_changed,
        }
        yield self._make_chunk(
            pr_identifier=metadata.identifier,
            chunk_type=ChunkType.SUMMARY,
            content=content,
            file_path=None,
            metadata=chunk_metadata,
        )
