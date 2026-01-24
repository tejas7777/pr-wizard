from datetime import datetime, timedelta, timezone

from tests.fakes import (
    FakeFile,
    FakeGitHubClient,
    FakePullRequest,
    FakePullRequestComment,
    FakePullRequestPart,
    FakeRepository,
    FakeUser,
)
from vec1.infra.github.pr_source import GitHubPRSource


def _make_fake_pr(
    number: int = 1,
    title: str = "Test PR",
    body: str = "Test body",
    state: str = "closed",
    created_at: datetime | None = None,
    merged_at: datetime | None = None,
    files: list[FakeFile] | None = None,
    comments: list[FakePullRequestComment] | None = None,
) -> FakePullRequest:
    now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    return FakePullRequest(
        number=number,
        title=title,
        body=body,
        state=state,
        created_at=created_at or now - timedelta(hours=2),
        merged_at=merged_at,
        user=FakeUser(login="test-author"),
        base=FakePullRequestPart(ref="main"),
        head=FakePullRequestPart(ref="feature"),
        _files=files
        or [
            FakeFile(
                filename="file1.py",
                patch="@@ -1 +1 @@\n-old\n+new",
                additions=1,
                deletions=1,
            )
        ],
        _comments=comments or [],
    )


class TestGitHubPRSourceFetchMergedSince:
    def test_returns_prs_merged_after_since(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = now - timedelta(hours=1)
        since = now - timedelta(hours=2)

        pr = _make_fake_pr(merged_at=merged_at)
        repo = FakeRepository([pr])
        client = FakeGitHubClient(repo)
        source = GitHubPRSource(client, "owner", "repo")

        result = list(source.fetch_merged_since(since))

        assert len(result) == 1
        assert result[0].metadata.identifier.pr_number == 1

    def test_excludes_prs_merged_before_since(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = now - timedelta(hours=3)
        since = now - timedelta(hours=2)

        pr = _make_fake_pr(merged_at=merged_at)
        repo = FakeRepository([pr])
        client = FakeGitHubClient(repo)
        source = GitHubPRSource(client, "owner", "repo")

        result = list(source.fetch_merged_since(since))

        assert len(result) == 0

    def test_excludes_unmerged_prs(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        since = now - timedelta(hours=2)

        pr = _make_fake_pr(merged_at=None, state="open")
        repo = FakeRepository([pr])
        client = FakeGitHubClient(repo)
        source = GitHubPRSource(client, "owner", "repo")

        result = list(source.fetch_merged_since(since))

        assert len(result) == 0

    def test_returns_multiple_merged_prs(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        since = now - timedelta(hours=3)

        prs = [
            _make_fake_pr(number=1, merged_at=now - timedelta(hours=1)),
            _make_fake_pr(number=2, merged_at=now - timedelta(hours=2)),
            _make_fake_pr(number=3, merged_at=None),
        ]
        repo = FakeRepository(prs)
        client = FakeGitHubClient(repo)
        source = GitHubPRSource(client, "owner", "repo")

        result = list(source.fetch_merged_since(since))

        assert len(result) == 2
        pr_numbers = {r.metadata.identifier.pr_number for r in result}
        assert pr_numbers == {1, 2}

    def test_includes_pr_merged_exactly_at_since(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        since = now - timedelta(hours=2)
        merged_at = since

        pr = _make_fake_pr(merged_at=merged_at)
        repo = FakeRepository([pr])
        client = FakeGitHubClient(repo)
        source = GitHubPRSource(client, "owner", "repo")

        result = list(source.fetch_merged_since(since))

        assert len(result) == 1


class TestGitHubPRSourceBuildsFetchedPR:
    def test_builds_metadata_correctly(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        created_at = now - timedelta(hours=2)
        merged_at = now - timedelta(hours=1)
        since = now - timedelta(hours=3)

        pr = _make_fake_pr(
            number=42,
            title="Add feature",
            body="This adds a feature",
            created_at=created_at,
            merged_at=merged_at,
        )
        repo = FakeRepository([pr])
        client = FakeGitHubClient(repo)
        source = GitHubPRSource(client, "test-owner", "test-repo")

        result = list(source.fetch_merged_since(since))

        assert len(result) == 1
        metadata = result[0].metadata
        assert metadata.identifier.source == "github"
        assert metadata.identifier.owner == "test-owner"
        assert metadata.identifier.repo == "test-repo"
        assert metadata.identifier.pr_number == 42
        assert metadata.title == "Add feature"
        assert metadata.description == "This adds a feature"
        assert metadata.author == "test-author"
        assert metadata.created_at == created_at
        assert metadata.merged_at == merged_at
        assert metadata.base_branch == "main"
        assert metadata.head_branch == "feature"

    def test_builds_diffs_correctly(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        since = now - timedelta(hours=2)

        files = [
            FakeFile(
                filename="src/main.py",
                patch="@@ -1,5 +1,10 @@\n-old code\n+new code",
                additions=10,
                deletions=5,
            ),
            FakeFile(
                filename="tests/test_main.py",
                patch="@@ -1 +1,20 @@\n+new tests",
                additions=20,
                deletions=0,
            ),
        ]
        pr = _make_fake_pr(merged_at=now - timedelta(hours=1), files=files)
        repo = FakeRepository([pr])
        client = FakeGitHubClient(repo)
        source = GitHubPRSource(client, "owner", "repo")

        result = list(source.fetch_merged_since(since))

        assert len(result) == 1
        diffs = result[0].diffs
        assert len(diffs) == 2
        assert diffs[0].file_path == "src/main.py"
        assert diffs[0].additions == 10
        assert diffs[0].deletions == 5
        assert diffs[1].file_path == "tests/test_main.py"
        assert diffs[1].additions == 20
        assert diffs[1].deletions == 0

    def test_handles_none_patch(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        since = now - timedelta(hours=2)

        files = [FakeFile(filename="binary.png", patch=None, additions=0, deletions=0)]
        pr = _make_fake_pr(merged_at=now - timedelta(hours=1), files=files)
        repo = FakeRepository([pr])
        client = FakeGitHubClient(repo)
        source = GitHubPRSource(client, "owner", "repo")

        result = list(source.fetch_merged_since(since))

        assert len(result) == 1
        assert result[0].diffs[0].patch == ""

    def test_builds_review_comments_correctly(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        since = now - timedelta(hours=2)
        comment_time = now - timedelta(minutes=30)

        comments = [
            FakePullRequestComment(
                id=123,
                body="Please fix this",
                path="src/main.py",
                created_at=comment_time,
                user=FakeUser(login="reviewer"),
            ),
        ]
        pr = _make_fake_pr(merged_at=now - timedelta(hours=1), comments=comments)
        repo = FakeRepository([pr])
        client = FakeGitHubClient(repo)
        source = GitHubPRSource(client, "owner", "repo")

        result = list(source.fetch_merged_since(since))

        assert len(result) == 1
        review_comments = result[0].review_comments
        assert len(review_comments) == 1
        assert review_comments[0].id == "123"
        assert review_comments[0].body == "Please fix this"
        assert review_comments[0].path == "src/main.py"
        assert review_comments[0].created_at == comment_time
        assert review_comments[0].author == "reviewer"

    def test_handles_none_user(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        since = now - timedelta(hours=2)

        pr = FakePullRequest(
            number=1,
            title="Test",
            body=None,
            state="closed",
            created_at=now - timedelta(hours=2),
            merged_at=now - timedelta(hours=1),
            user=None,
            base=FakePullRequestPart(ref="main"),
            head=FakePullRequestPart(ref="feature"),
            _files=[],
            _comments=[],
        )
        repo = FakeRepository([pr])
        client = FakeGitHubClient(repo)
        source = GitHubPRSource(client, "owner", "repo")

        result = list(source.fetch_merged_since(since))

        assert len(result) == 1
        assert result[0].metadata.author == ""

    def test_handles_none_base_and_head(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        since = now - timedelta(hours=2)

        pr = FakePullRequest(
            number=1,
            title="Test",
            body=None,
            state="closed",
            created_at=now - timedelta(hours=2),
            merged_at=now - timedelta(hours=1),
            user=FakeUser(login="author"),
            base=None,
            head=None,
            _files=[],
            _comments=[],
        )
        repo = FakeRepository([pr])
        client = FakeGitHubClient(repo)
        source = GitHubPRSource(client, "owner", "repo")

        result = list(source.fetch_merged_since(since))

        assert len(result) == 1
        assert result[0].metadata.base_branch == ""
        assert result[0].metadata.head_branch == ""

    def test_files_changed_tuple(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        since = now - timedelta(hours=2)

        files = [
            FakeFile(filename="a.py", patch="", additions=1, deletions=0),
            FakeFile(filename="b.py", patch="", additions=1, deletions=0),
            FakeFile(filename="c.py", patch="", additions=1, deletions=0),
        ]
        pr = _make_fake_pr(merged_at=now - timedelta(hours=1), files=files)
        repo = FakeRepository([pr])
        client = FakeGitHubClient(repo)
        source = GitHubPRSource(client, "owner", "repo")

        result = list(source.fetch_merged_since(since))

        assert len(result) == 1
        assert result[0].metadata.files_changed == ("a.py", "b.py", "c.py")
