from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Tuple


@dataclass(frozen=True, slots=True)
class PRIdentifier:
    source: str
    owner: str
    repo: str
    pr_number: int


@dataclass(frozen=True, slots=True)
class PRMetadata:
    identifier: PRIdentifier
    title: str
    description: Optional[str]
    author: str
    created_at: datetime
    merged_at: Optional[datetime]
    base_branch: str
    head_branch: str
    files_changed: Tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PRDiff:
    file_path: str
    patch: str
    additions: int
    deletions: int


@dataclass(frozen=True, slots=True)
class PRReviewComment:
    id: str
    body: str
    path: Optional[str]
    created_at: datetime
    author: str


@dataclass(frozen=True, slots=True)
class FetchedPR:
    metadata: PRMetadata
    diffs: Tuple[PRDiff, ...]
    review_comments: Tuple[PRReviewComment, ...]
