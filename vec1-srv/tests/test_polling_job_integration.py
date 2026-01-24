from datetime import datetime, timedelta, timezone
from typing import Iterator

import pytest

from vec1.core.exceptions import (
    PRFetchError,
    QueueFullError,
    SourceAuthenticationError,
    SourceRateLimitError,
)
from vec1.core.jobs.polling import PollingJob
from vec1.core.ports.pr_source import PRSource
from vec1.core.schema.pr import (
    FetchedPR,
    PRDiff,
    PRIdentifier,
    PRMetadata,
)
from tests.fakes import FakeClock, FakeLogger, FakePRSource, FakeQueue


def _make_fetched_pr(
    pr_number: int,
    merged_at: datetime,
    title: str = "Test PR",
    owner: str = "test-owner",
    repo: str = "test-repo",
    author: str = "test-author",
    files: tuple[str, ...] = ("file1.py",),
) -> FetchedPR:
    identifier = PRIdentifier(
        source="github",
        owner=owner,
        repo=repo,
        pr_number=pr_number,
    )
    metadata = PRMetadata(
        identifier=identifier,
        title=title,
        description="Test description",
        author=author,
        created_at=merged_at - timedelta(hours=1),
        merged_at=merged_at,
        base_branch="main",
        head_branch="feature",
        files_changed=files,
    )
    diffs = tuple(
        PRDiff(
            file_path=f,
            patch="@@ -1 +1 @@\n-old\n+new",
            additions=1,
            deletions=1,
        )
        for f in files
    )
    return FetchedPR(metadata=metadata, diffs=diffs, review_comments=())


class FakePollingState:
    def __init__(self, last_polled: datetime | None = None) -> None:
        self._last_polled = last_polled
        self.stored_values: list[datetime] = []
        self.load_count: int = 0

    def load_last_polled(self) -> datetime | None:
        self.load_count += 1
        return self._last_polled

    def store_last_polled(self, value: datetime) -> None:
        self._last_polled = value
        self.stored_values.append(value)

    @property
    def current_value(self) -> datetime | None:
        return self._last_polled


class ErroringPRSource(PRSource):
    def __init__(
        self,
        prs: list[FetchedPR],
        error_on_call: int,
        error: Exception,
    ) -> None:
        self._prs = prs
        self._error_on_call = error_on_call
        self._error = error
        self._call_count = 0

    def fetch_merged_since(self, since: datetime) -> Iterator[FetchedPR]:
        self._call_count += 1
        if self._call_count == self._error_on_call:
            raise self._error
        for pr in self._prs:
            if pr.metadata.merged_at is not None and pr.metadata.merged_at >= since:
                yield pr


class PartialErrorPRSource(PRSource):
    def __init__(
        self,
        prs: list[FetchedPR],
        error_after: int,
        error: Exception,
    ) -> None:
        self._prs = prs
        self._error_after = error_after
        self._error = error

    def fetch_merged_since(self, since: datetime) -> Iterator[FetchedPR]:
        count = 0
        for pr in self._prs:
            if pr.metadata.merged_at is not None and pr.metadata.merged_at >= since:
                if count >= self._error_after:
                    raise self._error
                yield pr
                count += 1


class DynamicPRSource(PRSource):
    def __init__(self) -> None:
        self._prs_by_call: list[list[FetchedPR]] = []
        self._call_count = 0

    def add_prs_for_cycle(self, prs: list[FetchedPR]) -> None:
        self._prs_by_call.append(prs)

    def fetch_merged_since(self, since: datetime) -> Iterator[FetchedPR]:
        if self._call_count < len(self._prs_by_call):
            prs = self._prs_by_call[self._call_count]
        else:
            prs = []
        self._call_count += 1
        for pr in prs:
            if pr.metadata.merged_at is not None and pr.metadata.merged_at >= since:
                yield pr


class CountingPRSource(PRSource):
    def __init__(self, prs: list[FetchedPR]) -> None:
        self._prs = prs
        self.calls: list[datetime] = []

    def fetch_merged_since(self, since: datetime) -> Iterator[FetchedPR]:
        self.calls.append(since)
        for pr in self._prs:
            if pr.metadata.merged_at is not None and pr.metadata.merged_at >= since:
                yield pr


def _create_job(
    pr_source: PRSource,
    clock: FakeClock,
    polling_state: FakePollingState,
    queue: FakeQueue[FetchedPR] | None = None,
    poll_interval: float = 0,
    default_lookback_hours: int = 24,
) -> tuple[PollingJob, FakeLogger, FakeQueue[FetchedPR]]:
    logger = FakeLogger()
    if queue is None:
        queue = FakeQueue[FetchedPR](clock)
    job = PollingJob(
        logger=logger,
        poll_interval=poll_interval,
        pr_source=pr_source,
        output_queue=queue,
        polling_state=polling_state,
        clock=clock,
        default_lookback_hours=default_lookback_hours,
    )
    return job, logger, queue


class TestEndToEndPollingFlow:
    def test_complete_flow_with_multiple_prs_from_multiple_repos(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_time = now - timedelta(hours=2)

        prs = [
            _make_fetched_pr(
                1, merged_time, "PR from repo A", owner="org1", repo="repoA"
            ),
            _make_fetched_pr(
                2,
                merged_time + timedelta(minutes=30),
                "PR from repo B",
                owner="org1",
                repo="repoB",
            ),
            _make_fetched_pr(
                3,
                merged_time + timedelta(hours=1),
                "PR from repo C",
                owner="org2",
                repo="repoC",
            ),
        ]

        clock = FakeClock(now)
        pr_source = FakePRSource(prs)
        polling_state = FakePollingState(merged_time - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()
        job.execute_once()

        assert queue.size() == 3
        messages = [queue.get() for _ in range(3)]
        repos = {m.payload.metadata.identifier.repo for m in messages if m}
        assert repos == {"repoA", "repoB", "repoC"}

    def test_complete_flow_with_prs_from_different_authors(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_time = now - timedelta(hours=2)

        prs = [
            _make_fetched_pr(1, merged_time, "PR by Alice", author="alice"),
            _make_fetched_pr(
                2, merged_time + timedelta(minutes=15), "PR by Bob", author="bob"
            ),
            _make_fetched_pr(
                3,
                merged_time + timedelta(minutes=30),
                "PR by Charlie",
                author="charlie",
            ),
            _make_fetched_pr(
                4,
                merged_time + timedelta(minutes=45),
                "Another PR by Alice",
                author="alice",
            ),
        ]

        clock = FakeClock(now)
        pr_source = FakePRSource(prs)
        polling_state = FakePollingState(merged_time - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()
        job.execute_once()

        assert queue.size() == 4
        messages = [queue.get() for _ in range(4)]
        authors = [m.payload.metadata.author for m in messages if m]
        assert authors.count("alice") == 2
        assert authors.count("bob") == 1
        assert authors.count("charlie") == 1

    def test_complete_flow_preserves_pr_order(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        base_time = now - timedelta(hours=3)

        prs = [
            _make_fetched_pr(i, base_time + timedelta(minutes=i * 10), f"PR {i}")
            for i in range(1, 11)
        ]

        clock = FakeClock(now)
        pr_source = FakePRSource(prs)
        polling_state = FakePollingState(base_time - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()
        job.execute_once()

        assert queue.size() == 10
        for expected_number in range(1, 11):
            message = queue.get()
            assert message is not None
            assert message.payload.metadata.identifier.pr_number == expected_number

    def test_complete_flow_with_large_pr_batch(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        base_time = now - timedelta(hours=6)

        prs = [
            _make_fetched_pr(i, base_time + timedelta(minutes=i), f"PR {i}")
            for i in range(1, 101)
        ]

        clock = FakeClock(now)
        pr_source = FakePRSource(prs)
        polling_state = FakePollingState(base_time - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()
        job.execute_once()

        assert queue.size() == 100
        info_records = [r for r in logger.records if r[0] == "info"]
        cycle_complete = [r for r in info_records if "Polling cycle complete" in r[1]]
        assert len(cycle_complete) == 1
        assert cycle_complete[0][2]["processed"] == 100


class TestEdgeCases:
    def test_empty_results_still_updates_state(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        pr_source = FakePRSource([])
        polling_state = FakePollingState(now - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()
        job.execute_once()

        assert queue.size() == 0
        assert len(polling_state.stored_values) == 1
        assert polling_state.stored_values[0] == now
        info_records = [r for r in logger.records if r[0] == "info"]
        cycle_complete = [r for r in info_records if "Polling cycle complete" in r[1]]
        assert cycle_complete[0][2]["processed"] == 0

    def test_all_prs_filtered_out_by_since_time(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        old_time = now - timedelta(days=7)

        prs = [
            _make_fetched_pr(1, old_time, "Old PR 1"),
            _make_fetched_pr(2, old_time + timedelta(hours=1), "Old PR 2"),
        ]

        clock = FakeClock(now)
        pr_source = FakePRSource(prs)
        polling_state = FakePollingState(now - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()
        job.execute_once()

        assert queue.size() == 0

    def test_pr_source_error_is_propagated(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        identifier = PRIdentifier("github", "owner", "repo", 1)
        error = PRFetchError("Failed to fetch PR", identifier)

        clock = FakeClock(now)
        pr_source = ErroringPRSource([], error_on_call=1, error=error)
        polling_state = FakePollingState(now - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()

        with pytest.raises(PRFetchError) as exc_info:
            job.execute_once()
        assert exc_info.value.pr_identifier == identifier

    def test_authentication_error_from_source(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        error = SourceAuthenticationError("Invalid token")

        clock = FakeClock(now)
        pr_source = ErroringPRSource([], error_on_call=1, error=error)
        polling_state = FakePollingState(now - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()

        with pytest.raises(SourceAuthenticationError) as exc_info:
            job.execute_once()
        assert "Invalid token" in str(exc_info.value)

    def test_rate_limit_error_from_source(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        retry_after = now + timedelta(minutes=5)
        error = SourceRateLimitError("Rate limited", retry_after)

        clock = FakeClock(now)
        pr_source = ErroringPRSource([], error_on_call=1, error=error)
        polling_state = FakePollingState(now - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()

        with pytest.raises(SourceRateLimitError) as exc_info:
            job.execute_once()
        assert exc_info.value.retry_after == retry_after

    def test_partial_error_during_iteration(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_time = now - timedelta(hours=2)

        prs = [
            _make_fetched_pr(1, merged_time, "PR 1"),
            _make_fetched_pr(2, merged_time + timedelta(minutes=10), "PR 2"),
            _make_fetched_pr(3, merged_time + timedelta(minutes=20), "PR 3"),
        ]

        identifier = PRIdentifier("github", "owner", "repo", 3)
        error = PRFetchError("Failed on PR 3", identifier)

        clock = FakeClock(now)
        pr_source = PartialErrorPRSource(prs, error_after=2, error=error)
        polling_state = FakePollingState(merged_time - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()

        with pytest.raises(PRFetchError):
            job.execute_once()

        assert queue.size() == 2

    def test_non_vec1_error_propagates_unchanged(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        error = RuntimeError("Unexpected error")

        clock = FakeClock(now)
        pr_source = ErroringPRSource([], error_on_call=1, error=error)
        polling_state = FakePollingState(now - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()

        with pytest.raises(RuntimeError) as exc_info:
            job.execute_once()
        assert "Unexpected error" in str(exc_info.value)


class TestStatePersistence:
    def test_state_loaded_once_during_setup(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        pr_source = FakePRSource([])
        polling_state = FakePollingState(now - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()

        assert polling_state.load_count == 1

    def test_state_stored_after_each_execute_once(self) -> None:
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        clock = FakeClock(t1)
        pr_source = FakePRSource([])
        polling_state = FakePollingState(t1 - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()
        job.execute_once()
        job.execute_once()
        job.execute_once()

        assert len(polling_state.stored_values) == 3
        assert all(v == t1 for v in polling_state.stored_values)

    def test_state_persists_between_cycles(self) -> None:
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        prs: list[FetchedPR] = []

        clock = FakeClock(t1)
        pr_source = CountingPRSource(prs)
        initial_state = t1 - timedelta(hours=1)
        polling_state = FakePollingState(initial_state)

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()
        job.execute_once()
        job.execute_once()

        assert len(pr_source.calls) == 2
        assert pr_source.calls[0] == initial_state
        assert pr_source.calls[1] == t1

    def test_teardown_stores_final_state(self) -> None:
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        clock = FakeClock(t1)
        pr_source = FakePRSource([])
        polling_state = FakePollingState(t1 - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()
        job.execute_once()
        initial_store_count = len(polling_state.stored_values)
        job.teardown()

        assert len(polling_state.stored_values) == initial_store_count + 1
        assert polling_state.stored_values[-1] == t1

    def test_teardown_without_execute_stores_initial_state(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        initial_last_polled = now - timedelta(hours=1)

        clock = FakeClock(now)
        pr_source = FakePRSource([])
        polling_state = FakePollingState(initial_last_polled)

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()
        job.teardown()

        assert len(polling_state.stored_values) == 1
        assert polling_state.stored_values[0] == initial_last_polled

    def test_state_continuity_across_job_instances(self) -> None:
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 15, 12, 5, 0, tzinfo=timezone.utc)

        clock1 = FakeClock(t1)
        pr_source1 = FakePRSource([])
        polling_state = FakePollingState(None)

        job1, _, _ = _create_job(
            pr_source1, clock1, polling_state, default_lookback_hours=24
        )
        job1.setup()
        job1.execute_once()
        job1.teardown()

        assert polling_state.current_value == t1

        clock2 = FakeClock(t2)
        pr_source2 = CountingPRSource([])

        job2, _, _ = _create_job(pr_source2, clock2, polling_state)
        job2.setup()
        job2.execute_once()

        assert len(pr_source2.calls) == 1
        assert pr_source2.calls[0] == t1


class TestQueueInteractions:
    def test_queue_receives_correct_message_metadata(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = now - timedelta(hours=1)
        pr = _make_fetched_pr(42, merged_at, "Test PR", owner="myorg", repo="myrepo")

        clock = FakeClock(now)
        pr_source = FakePRSource([pr])
        polling_state = FakePollingState(merged_at - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()
        job.execute_once()

        message = queue.get()
        assert message is not None
        assert message.payload.metadata.identifier.pr_number == 42
        assert message.payload.metadata.identifier.owner == "myorg"
        assert message.payload.metadata.identifier.repo == "myrepo"
        assert message.payload.metadata.title == "Test PR"
        assert message.attempt_count == 1

    def test_queue_message_enqueued_at_uses_clock(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = now - timedelta(hours=1)
        pr = _make_fetched_pr(1, merged_at)

        clock = FakeClock(now)
        pr_source = FakePRSource([pr])
        polling_state = FakePollingState(merged_at - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()
        job.execute_once()

        message = queue.get()
        assert message is not None
        assert message.enqueued_at == now

    def test_queue_full_error_propagates(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = now - timedelta(hours=1)

        prs = [
            _make_fetched_pr(1, merged_at, "PR 1"),
            _make_fetched_pr(2, merged_at + timedelta(minutes=10), "PR 2"),
            _make_fetched_pr(3, merged_at + timedelta(minutes=20), "PR 3"),
        ]

        clock = FakeClock(now)
        pr_source = FakePRSource(prs)
        polling_state = FakePollingState(merged_at - timedelta(hours=1))
        queue = FakeQueue[FetchedPR](clock, max_size=2)

        job, logger, _ = _create_job(pr_source, clock, polling_state, queue=queue)
        job.setup()

        with pytest.raises(QueueFullError):
            job.execute_once()

        assert queue.size() == 2

    def test_queue_full_with_single_item_capacity(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = now - timedelta(hours=1)

        prs = [
            _make_fetched_pr(1, merged_at, "PR 1"),
            _make_fetched_pr(2, merged_at + timedelta(minutes=10), "PR 2"),
        ]

        clock = FakeClock(now)
        pr_source = FakePRSource(prs)
        polling_state = FakePollingState(merged_at - timedelta(hours=1))
        queue = FakeQueue[FetchedPR](clock, max_size=1)

        job, logger, _ = _create_job(pr_source, clock, polling_state, queue=queue)
        job.setup()

        with pytest.raises(QueueFullError):
            job.execute_once()

        assert queue.size() == 1
        message = queue.get()
        assert message is not None
        assert message.payload.metadata.identifier.pr_number == 1

    def test_multiple_prs_enqueue_sequentially(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = now - timedelta(hours=2)

        prs = [
            _make_fetched_pr(1, merged_at),
            _make_fetched_pr(2, merged_at + timedelta(minutes=30)),
            _make_fetched_pr(3, merged_at + timedelta(hours=1)),
        ]

        clock = FakeClock(now)
        pr_source = FakePRSource(prs)
        polling_state = FakePollingState(merged_at - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()
        job.execute_once()

        pr_numbers = []
        while queue.size() > 0:
            message = queue.get()
            if message:
                pr_numbers.append(message.payload.metadata.identifier.pr_number)

        assert pr_numbers == [1, 2, 3]


class TestClockTimeHandling:
    def test_default_lookback_when_no_prior_state(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        lookback_hours = 48

        clock = FakeClock(now)
        pr_source = CountingPRSource([])
        polling_state = FakePollingState(None)

        job, logger, queue = _create_job(
            pr_source, clock, polling_state, default_lookback_hours=lookback_hours
        )
        job.setup()
        job.execute_once()

        expected_since = now - timedelta(hours=lookback_hours)
        assert len(pr_source.calls) == 1
        assert pr_source.calls[0] == expected_since

    def test_different_lookback_values(self) -> None:
        for lookback_hours in [1, 6, 24, 72, 168]:
            now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
            clock = FakeClock(now)
            pr_source = CountingPRSource([])
            polling_state = FakePollingState(None)

            job, _, _ = _create_job(
                pr_source, clock, polling_state, default_lookback_hours=lookback_hours
            )
            job.setup()
            job.execute_once()

            expected_since = now - timedelta(hours=lookback_hours)
            assert pr_source.calls[0] == expected_since

    def test_clock_advances_between_cycles(self) -> None:
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        clock = FakeClock(t1)
        pr_source = FakePRSource([])
        polling_state = FakePollingState(t1 - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()
        job.execute_once()
        job.execute_once()
        job.execute_once()

        assert len(polling_state.stored_values) == 3
        assert all(v == t1 for v in polling_state.stored_values)

    def test_poll_interval_calls_clock_sleep(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        poll_interval = 30.0

        clock = FakeClock(now)
        pr_source = FakePRSource([])
        polling_state = FakePollingState(now - timedelta(hours=1))

        job, logger, queue = _create_job(
            pr_source, clock, polling_state, poll_interval=poll_interval
        )
        job.setup()

        job.execute_once()
        job._sleep(poll_interval)

        assert len(clock.slept) == 1
        assert clock.slept[0] == poll_interval

    def test_timezone_aware_datetimes_preserved(self) -> None:
        utc_now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        pr = _make_fetched_pr(1, merged_at)

        clock = FakeClock(utc_now)
        pr_source = FakePRSource([pr])
        polling_state = FakePollingState(merged_at - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()
        job.execute_once()

        assert polling_state.stored_values[0].tzinfo == timezone.utc


class TestMultiplePollingCycles:
    def test_multiple_cycles_fetch_different_prs(self) -> None:
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        pr1 = _make_fetched_pr(1, t1 - timedelta(minutes=30), "PR 1")
        pr2 = _make_fetched_pr(2, t1 + timedelta(minutes=2), "PR 2")
        pr3 = _make_fetched_pr(3, t1 + timedelta(minutes=10), "PR 3")

        pr_source = DynamicPRSource()
        pr_source.add_prs_for_cycle([pr1])
        pr_source.add_prs_for_cycle([pr2])
        pr_source.add_prs_for_cycle([pr3])

        clock = FakeClock(t1)
        polling_state = FakePollingState(t1 - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()

        job.execute_once()
        assert queue.size() == 1

        job.execute_once()
        assert queue.size() == 2

        job.execute_once()
        assert queue.size() == 3

    def test_consecutive_empty_cycles(self) -> None:
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        clock = FakeClock(t1)
        pr_source = FakePRSource([])
        polling_state = FakePollingState(t1 - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()

        for _ in range(3):
            job.execute_once()

        assert queue.size() == 0
        info_records = [r for r in logger.records if r[0] == "info"]
        cycle_complete = [r for r in info_records if "Polling cycle complete" in r[1]]
        assert len(cycle_complete) == 3
        assert all(r[2]["processed"] == 0 for r in cycle_complete)

    def test_alternating_empty_and_nonempty_cycles(self) -> None:
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        pr1 = _make_fetched_pr(1, t1 - timedelta(minutes=5), "PR 1")
        pr3 = _make_fetched_pr(3, t1 + timedelta(minutes=5), "PR 3")

        pr_source = DynamicPRSource()
        pr_source.add_prs_for_cycle([pr1])
        pr_source.add_prs_for_cycle([])
        pr_source.add_prs_for_cycle([pr3])
        pr_source.add_prs_for_cycle([])

        clock = FakeClock(t1)
        polling_state = FakePollingState(t1 - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()

        for _ in range(4):
            job.execute_once()

        assert queue.size() == 2
        info_records = [r for r in logger.records if r[0] == "info"]
        cycle_complete = [r for r in info_records if "Polling cycle complete" in r[1]]
        processed_counts = [r[2]["processed"] for r in cycle_complete]
        assert processed_counts == [1, 0, 1, 0]

    def test_cycle_count_tracking_in_logs(self) -> None:
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = t1 - timedelta(hours=1)

        prs = [
            _make_fetched_pr(1, merged_at),
            _make_fetched_pr(2, merged_at + timedelta(minutes=10)),
            _make_fetched_pr(3, merged_at + timedelta(minutes=20)),
        ]

        clock = FakeClock(t1)
        pr_source = FakePRSource(prs)
        polling_state = FakePollingState(merged_at - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()
        job.execute_once()

        info_records = [r for r in logger.records if r[0] == "info"]
        cycle_complete = [r for r in info_records if "Polling cycle complete" in r[1]]
        assert len(cycle_complete) == 1
        assert cycle_complete[0][2]["processed"] == 3


class TestErrorRecovery:
    def test_error_on_first_cycle_then_success(self) -> None:
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 15, 12, 5, 0, tzinfo=timezone.utc)
        merged_at = t1 - timedelta(hours=1)

        pr = _make_fetched_pr(1, merged_at, "PR 1")
        identifier = PRIdentifier("github", "owner", "repo", 1)
        error = PRFetchError("Failed", identifier)

        error_source = ErroringPRSource([pr], error_on_call=1, error=error)

        clock = FakeClock([t1, t1])
        polling_state = FakePollingState(merged_at - timedelta(hours=1))

        job, logger, queue = _create_job(error_source, clock, polling_state)
        job.setup()

        with pytest.raises(PRFetchError):
            job.execute_once()

        working_source = FakePRSource([pr])
        job2, logger2, queue2 = _create_job(
            working_source, FakeClock(t2), polling_state
        )
        job2.setup()
        job2.execute_once()

        assert queue2.size() == 1

    def test_state_not_updated_on_error(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        initial_state = now - timedelta(hours=1)
        identifier = PRIdentifier("github", "owner", "repo", 1)
        error = PRFetchError("Failed", identifier)

        clock = FakeClock(now)
        pr_source = ErroringPRSource([], error_on_call=1, error=error)
        polling_state = FakePollingState(initial_state)

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()

        with pytest.raises(PRFetchError):
            job.execute_once()

        assert len(polling_state.stored_values) == 0
        assert polling_state.current_value == initial_state

    def test_recovery_continues_from_last_successful_state(self) -> None:
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 15, 12, 5, 0, tzinfo=timezone.utc)
        t3 = datetime(2024, 1, 15, 12, 10, 0, tzinfo=timezone.utc)
        merged_at = t1 - timedelta(hours=2)

        pr1 = _make_fetched_pr(1, merged_at, "PR 1")
        identifier = PRIdentifier("github", "owner", "repo", 2)
        error = PRFetchError("Failed", identifier)

        class TwoPhaseSource(PRSource):
            def __init__(self) -> None:
                self._call_count = 0

            def fetch_merged_since(self, since: datetime) -> Iterator[FetchedPR]:
                self._call_count += 1
                if self._call_count == 2:
                    raise error
                yield pr1

        clock = FakeClock([t1, t1, t2, t3])
        pr_source = TwoPhaseSource()
        polling_state = FakePollingState(merged_at - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()

        job.execute_once()
        assert len(polling_state.stored_values) == 1
        assert polling_state.stored_values[0] == t1

        with pytest.raises(PRFetchError):
            job.execute_once()

        assert len(polling_state.stored_values) == 1

        job.execute_once()
        assert len(polling_state.stored_values) == 2
        assert polling_state.stored_values[1] == t3

    def test_queue_full_doesnt_update_state(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = now - timedelta(hours=1)
        initial_state = merged_at - timedelta(hours=1)

        prs = [
            _make_fetched_pr(1, merged_at, "PR 1"),
            _make_fetched_pr(2, merged_at + timedelta(minutes=10), "PR 2"),
        ]

        clock = FakeClock(now)
        pr_source = FakePRSource(prs)
        polling_state = FakePollingState(initial_state)
        queue = FakeQueue[FetchedPR](clock, max_size=1)

        job, logger, _ = _create_job(pr_source, clock, polling_state, queue=queue)
        job.setup()

        with pytest.raises(QueueFullError):
            job.execute_once()

        assert len(polling_state.stored_values) == 0
        assert polling_state.current_value == initial_state


class TestBoundaryConditions:
    def test_pr_merged_exactly_at_lookback_time(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        lookback_hours = 24
        lookback_time = now - timedelta(hours=lookback_hours)

        pr = _make_fetched_pr(1, lookback_time, "Boundary PR")

        clock = FakeClock(now)
        pr_source = FakePRSource([pr])
        polling_state = FakePollingState(None)

        job, logger, queue = _create_job(
            pr_source, clock, polling_state, default_lookback_hours=lookback_hours
        )
        job.setup()
        job.execute_once()

        assert queue.size() == 1

    def test_pr_merged_one_second_before_lookback(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        lookback_hours = 24
        lookback_time = now - timedelta(hours=lookback_hours)

        pr = _make_fetched_pr(1, lookback_time - timedelta(seconds=1), "Too Old PR")

        clock = FakeClock(now)
        pr_source = FakePRSource([pr])
        polling_state = FakePollingState(None)

        job, logger, queue = _create_job(
            pr_source, clock, polling_state, default_lookback_hours=lookback_hours
        )
        job.setup()
        job.execute_once()

        assert queue.size() == 0

    def test_pr_merged_one_second_after_lookback(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        lookback_hours = 24
        lookback_time = now - timedelta(hours=lookback_hours)

        pr = _make_fetched_pr(1, lookback_time + timedelta(seconds=1), "Just In PR")

        clock = FakeClock(now)
        pr_source = FakePRSource([pr])
        polling_state = FakePollingState(None)

        job, logger, queue = _create_job(
            pr_source, clock, polling_state, default_lookback_hours=lookback_hours
        )
        job.setup()
        job.execute_once()

        assert queue.size() == 1

    def test_pr_merged_exactly_at_last_polled_time(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        last_polled = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

        pr = _make_fetched_pr(1, last_polled, "Boundary PR")

        clock = FakeClock(now)
        pr_source = FakePRSource([pr])
        polling_state = FakePollingState(last_polled)

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()
        job.execute_once()

        assert queue.size() == 1

    def test_zero_poll_interval(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        clock = FakeClock(now)
        pr_source = FakePRSource([])
        polling_state = FakePollingState(now - timedelta(hours=1))

        job, logger, queue = _create_job(
            pr_source, clock, polling_state, poll_interval=0
        )
        job.setup()
        job.execute_once()

        assert len(clock.slept) == 0

    def test_very_small_poll_interval(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        poll_interval = 0.001

        clock = FakeClock(now)
        pr_source = FakePRSource([])
        polling_state = FakePollingState(now - timedelta(hours=1))

        job, logger, queue = _create_job(
            pr_source, clock, polling_state, poll_interval=poll_interval
        )
        job.setup()
        job.execute_once()
        job._sleep(poll_interval)

        assert clock.slept == [poll_interval]

    def test_very_large_lookback_hours(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        lookback_hours = 8760

        clock = FakeClock(now)
        pr_source = CountingPRSource([])
        polling_state = FakePollingState(None)

        job, logger, queue = _create_job(
            pr_source, clock, polling_state, default_lookback_hours=lookback_hours
        )
        job.setup()
        job.execute_once()

        expected_since = now - timedelta(hours=lookback_hours)
        assert pr_source.calls[0] == expected_since

    def test_minimum_lookback_hours(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        lookback_hours = 1

        clock = FakeClock(now)
        pr_source = CountingPRSource([])
        polling_state = FakePollingState(None)

        job, logger, queue = _create_job(
            pr_source, clock, polling_state, default_lookback_hours=lookback_hours
        )
        job.setup()
        job.execute_once()

        expected_since = now - timedelta(hours=1)
        assert pr_source.calls[0] == expected_since

    def test_pr_with_very_long_title(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = now - timedelta(hours=1)
        long_title = "A" * 10000

        pr = _make_fetched_pr(1, merged_at, long_title)

        clock = FakeClock(now)
        pr_source = FakePRSource([pr])
        polling_state = FakePollingState(merged_at - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()
        job.execute_once()

        assert queue.size() == 1
        message = queue.get()
        assert message is not None
        assert message.payload.metadata.title == long_title

    def test_pr_with_many_files_changed(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = now - timedelta(hours=1)
        files = tuple(f"file{i}.py" for i in range(1000))

        pr = _make_fetched_pr(1, merged_at, "Many Files PR", files=files)

        clock = FakeClock(now)
        pr_source = FakePRSource([pr])
        polling_state = FakePollingState(merged_at - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()
        job.execute_once()

        assert queue.size() == 1
        message = queue.get()
        assert message is not None
        assert len(message.payload.metadata.files_changed) == 1000
        assert len(message.payload.diffs) == 1000


class TestLogging:
    def test_setup_logs_initialization(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        last_polled = now - timedelta(hours=1)

        clock = FakeClock(now)
        pr_source = FakePRSource([])
        polling_state = FakePollingState(last_polled)

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()

        info_records = [r for r in logger.records if r[0] == "info"]
        init_logs = [r for r in info_records if "Polling initialized" in r[1]]
        assert len(init_logs) == 1
        assert "last_polled" in init_logs[0][2]
        assert init_logs[0][2]["last_polled"] == last_polled.isoformat()

    def test_execute_once_logs_cycle_complete_with_count(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = now - timedelta(hours=1)

        prs = [
            _make_fetched_pr(i, merged_at + timedelta(minutes=i * 5), f"PR {i}")
            for i in range(5)
        ]

        clock = FakeClock(now)
        pr_source = FakePRSource(prs)
        polling_state = FakePollingState(merged_at - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()
        job.execute_once()

        info_records = [r for r in logger.records if r[0] == "info"]
        cycle_logs = [r for r in info_records if "Polling cycle complete" in r[1]]
        assert len(cycle_logs) == 1
        assert cycle_logs[0][2]["processed"] == 5
        assert "last_polled" in cycle_logs[0][2]

    def test_log_format_includes_iso_timestamps(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        clock = FakeClock(now)
        pr_source = FakePRSource([])
        polling_state = FakePollingState(now - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)
        job.setup()
        job.execute_once()

        all_last_polled = []
        for record in logger.records:
            if "last_polled" in record[2]:
                all_last_polled.append(record[2]["last_polled"])

        for timestamp in all_last_polled:
            datetime.fromisoformat(timestamp)


class TestJobLifecycle:
    def test_full_lifecycle_setup_execute_teardown(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = now - timedelta(hours=1)
        pr = _make_fetched_pr(1, merged_at, "Test PR")

        clock = FakeClock(now)
        pr_source = FakePRSource([pr])
        polling_state = FakePollingState(merged_at - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)

        job.setup()
        job.execute_once()
        job.teardown()

        assert queue.size() == 1
        assert len(polling_state.stored_values) == 2
        info_records = [r for r in logger.records if r[0] == "info"]
        assert any("Polling initialized" in r[1] for r in info_records)
        assert any("Polling cycle complete" in r[1] for r in info_records)

    def test_stop_method_sets_running_to_false(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        clock = FakeClock(now)
        pr_source = FakePRSource([])
        polling_state = FakePollingState(now - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)

        job._running = True
        job.stop()
        assert job._running is False

    def test_should_continue_returns_true_by_default(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        clock = FakeClock(now)
        pr_source = FakePRSource([])
        polling_state = FakePollingState(now - timedelta(hours=1))

        job, logger, queue = _create_job(pr_source, clock, polling_state)

        assert job.should_continue() is True
