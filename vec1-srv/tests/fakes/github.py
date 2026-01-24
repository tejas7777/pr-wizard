from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass
class FakeUser:
    login: str


@dataclass
class FakePullRequestPart:
    ref: str


@dataclass
class FakeFile:
    filename: str
    patch: Optional[str]
    additions: int
    deletions: int


@dataclass
class FakePullRequestComment:
    id: int
    body: str
    path: Optional[str]
    created_at: datetime
    user: Optional[FakeUser]


@dataclass
class FakePullRequest:
    number: int
    title: str
    body: Optional[str]
    state: str
    created_at: datetime
    merged_at: Optional[datetime]
    user: Optional[FakeUser]
    base: Optional[FakePullRequestPart]
    head: Optional[FakePullRequestPart]
    _files: List[FakeFile]
    _comments: List[FakePullRequestComment]

    def get_files(self) -> List[FakeFile]:
        return self._files

    def get_review_comments(self) -> List[FakePullRequestComment]:
        return self._comments


class FakeRepository:
    def __init__(self, pulls: List[FakePullRequest]) -> None:
        self._pulls = pulls

    def get_pulls(
        self,
        state: str = "open",
        sort: str = "created",
        direction: str = "desc",
    ) -> List[FakePullRequest]:
        return self._pulls


class FakeGitHubClient:
    def __init__(self, repo: FakeRepository) -> None:
        self._repo = repo

    def get_repo(self, owner: str, name: str) -> FakeRepository:
        return self._repo

    def close(self) -> None:
        pass
