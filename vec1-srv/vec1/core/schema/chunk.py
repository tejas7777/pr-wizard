from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Optional

from vec1.core.schema.pr import PRIdentifier


class ChunkType(Enum):
    SUMMARY = "summary"
    DIFF = "diff"
    REVIEW_COMMENT = "review_comment"


@dataclass(frozen=True, slots=True)
class Chunk:
    pr_identifier: PRIdentifier
    chunk_type: ChunkType
    content: str
    file_path: Optional[str]
    content_hash: str
    token_count: int
    metadata: Mapping[str, Any]
