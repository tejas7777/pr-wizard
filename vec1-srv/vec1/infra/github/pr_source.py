from datetime import datetime, timezone
from typing import Iterator, List

from github import GithubException
from github.Repository import Repository

from vec1.core.exceptions import (
    PRFetchError,
    SourceAuthenticationError,
    SourceError,
    SourceNotFoundError,
    SourceRateLimitError,
)
from vec1.core.ports.pr_source import PRSource
from vec1.core.schema.pr import (
    FetchedPR,
    PRDiff,
    PRIdentifier,
    PRMetadata,
    PRReviewComment,
)
from vec1.infra.github.client import GitHubClient


STATUS_OPEN = "open"
STATUS_CLOSED = "closed"
STATUS_ALL = "all"


class GitHubPRSource(PRSource):
    def __init__(
        self,
        client: GitHubClient,
        owner: str,
        repo: str,
        pr_status: List[str] = [STATUS_CLOSED],
    ) -> None:
        self._client = client
        self._owner = owner
        self._repo_name = repo
        self._repo: Repository | None = None
        self._pr_status = pr_status

    def fetch_merged_since(self, since: datetime) -> Iterator[FetchedPR]:
        repo = self._get_repo()
        try:
            pulls = repo.get_pulls(
                state=STATUS_ALL,
                sort="updated",
                direction="desc",
            )
        except GithubException as error:
            self._translate_exception(
                "Failed to fetch pull requests",
                error,
                resource=f"{self._owner}/{self._repo_name}",
            )
        else:
            for pr in pulls:
                if pr.merged_at is None or pr.merged_at < since:
                    continue
                yield self._build_fetched_pr(pr)

    def _build_fetched_pr(self, pr) -> FetchedPR:
        identifier = PRIdentifier(
            source="github",
            owner=self._owner,
            repo=self._repo_name,
            pr_number=pr.number,
        )
        try:
            files = list(pr.get_files())
            files_changed = tuple(file.filename for file in files)
            diffs = tuple(self._to_diff(file) for file in files)
            review_comments = tuple(
                self._to_review_comment(comment) for comment in pr.get_review_comments()
            )

            metadata = PRMetadata(
                identifier=identifier,
                title=pr.title or "",
                description=pr.body,
                author=pr.user.login if pr.user else "",
                created_at=pr.created_at,
                merged_at=pr.merged_at,
                base_branch=pr.base.ref if pr.base else "",
                head_branch=pr.head.ref if pr.head else "",
                files_changed=files_changed,
            )
            return FetchedPR(
                metadata=metadata,
                diffs=diffs,
                review_comments=review_comments,
            )
        except GithubException as error:
            raise PRFetchError(
                "Failed to fetch pull request details",
                identifier,
            ) from error

    def _to_diff(self, file) -> PRDiff:
        patch = file.patch if file.patch is not None else ""
        return PRDiff(
            file_path=file.filename,
            patch=patch,
            additions=file.additions,
            deletions=file.deletions,
        )

    def _to_review_comment(self, comment) -> PRReviewComment:
        return PRReviewComment(
            id=str(comment.id),
            body=comment.body,
            path=comment.path,
            created_at=comment.created_at,
            author=comment.user.login if comment.user else "",
        )

    def _get_repo(self) -> Repository:
        if self._repo is None:
            try:
                self._repo = self._client.get_repo(
                    self._owner,
                    self._repo_name,
                )
            except GithubException as error:
                self._translate_exception(
                    "Failed to access repository",
                    error,
                    resource=f"{self._owner}/{self._repo_name}",
                )
        assert self._repo is not None
        return self._repo

    def _translate_exception(
        self,
        message: str,
        error: GithubException,
        resource: str | None = None,
    ) -> None:
        status = getattr(error, "status", None)
        headers = getattr(error, "headers", {}) or {}
        if status == 401:
            raise SourceAuthenticationError(message) from error
        if status == 404:
            raise SourceNotFoundError(
                message,
                resource or "resource",
            ) from error
        if status == 403:
            retry_after = self._retry_after_from_headers(headers)
            if retry_after:
                raise SourceRateLimitError(message, retry_after) from error
        raise SourceError(message) from error

    def _retry_after_from_headers(self, headers) -> datetime | None:  # noqa: ANN001
        reset = headers.get("Retry-After") or headers.get("X-RateLimit-Reset")
        if reset is None:
            return None
        try:
            reset_time = float(reset)
            return datetime.fromtimestamp(reset_time, tz=timezone.utc)
        except (TypeError, ValueError):
            return None
