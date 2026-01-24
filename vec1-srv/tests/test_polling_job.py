from datetime import datetime, timedelta, timezone

import pytest

from vec1.core.jobs.polling import PollingJob
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
    title: str = 'Test PR',
) -> FetchedPR:
    identifier = PRIdentifier(
        source='github',
        owner='test-owner',
        repo='test-repo',
        pr_number=pr_number,
    )
    metadata = PRMetadata(
        identifier=identifier,
        title=title,
        description='Test description',
        author='test-author',
        created_at=merged_at - timedelta(hours=1),
        merged_at=merged_at,
        base_branch='main',
        head_branch='feature',
        files_changed=('file1.py',),
    )
    diffs = (
        PRDiff(
            file_path='file1.py',
            patch='@@ -1 +1 @@\n-old\n+new',
            additions=1,
            deletions=1,
        ),
    )
    return FetchedPR(metadata=metadata, diffs=diffs, review_comments=())


class FakePollingState:
    def __init__(self, last_polled: datetime | None = None) -> None:
        self._last_polled = last_polled
        self.stored_values: list[datetime] = []

    def load_last_polled(self) -> datetime | None:
        return self._last_polled

    def store_last_polled(self, value: datetime) -> None:
        self._last_polled = value
        self.stored_values.append(value)


class TestPollingJobSetup:
    def test_setup_loads_last_polled_from_state(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        last_polled = datetime(2024, 1, 14, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        logger = FakeLogger()
        pr_source = FakePRSource([])
        queue = FakeQueue[FetchedPR](clock)
        polling_state = FakePollingState(last_polled)

        job = PollingJob(
            logger=logger,
            poll_interval=60.0,
            pr_source=pr_source,
            output_queue=queue,
            polling_state=polling_state,
            clock=clock,
            default_lookback_hours=24,
        )
        job.setup()

        info_records = [r for r in logger.records if r[0] == 'info']
        assert any('Polling initialized' in r[1] for r in info_records)

    def test_setup_uses_default_lookback_when_no_state(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        logger = FakeLogger()
        pr_source = FakePRSource([])
        queue = FakeQueue[FetchedPR](clock)
        polling_state = FakePollingState(None)

        job = PollingJob(
            logger=logger,
            poll_interval=60.0,
            pr_source=pr_source,
            output_queue=queue,
            polling_state=polling_state,
            clock=clock,
            default_lookback_hours=48,
        )
        job.setup()

        info_records = [r for r in logger.records if r[0] == 'info']
        assert any('Polling initialized' in r[1] for r in info_records)


class TestPollingJobExecuteOnce:
    def test_execute_once_fetches_prs_and_enqueues(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        pr = _make_fetched_pr(1, merged_at)
        clock = FakeClock([now, now])
        logger = FakeLogger()
        pr_source = FakePRSource([pr])
        queue = FakeQueue[FetchedPR](clock)
        polling_state = FakePollingState(merged_at - timedelta(hours=1))

        job = PollingJob(
            logger=logger,
            poll_interval=0,
            pr_source=pr_source,
            output_queue=queue,
            polling_state=polling_state,
            clock=clock,
            default_lookback_hours=24,
        )
        job.setup()
        job.execute_once()

        assert queue.size() == 1
        message = queue.get()
        assert message is not None
        assert message.payload.metadata.identifier.pr_number == 1

    def test_execute_once_updates_polling_state(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        after_execute = datetime(2024, 1, 15, 12, 1, 0, tzinfo=timezone.utc)
        clock = FakeClock(after_execute)
        logger = FakeLogger()
        pr_source = FakePRSource([])
        queue = FakeQueue[FetchedPR](clock)
        polling_state = FakePollingState(now - timedelta(hours=1))

        job = PollingJob(
            logger=logger,
            poll_interval=0,
            pr_source=pr_source,
            output_queue=queue,
            polling_state=polling_state,
            clock=clock,
            default_lookback_hours=24,
        )
        job.setup()
        job.execute_once()

        assert len(polling_state.stored_values) == 1
        assert polling_state.stored_values[0] == after_execute

    def test_execute_once_logs_cycle_complete(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock([now, now])
        logger = FakeLogger()
        pr_source = FakePRSource([])
        queue = FakeQueue[FetchedPR](clock)
        polling_state = FakePollingState(now - timedelta(hours=1))

        job = PollingJob(
            logger=logger,
            poll_interval=0,
            pr_source=pr_source,
            output_queue=queue,
            polling_state=polling_state,
            clock=clock,
            default_lookback_hours=24,
        )
        job.setup()
        job.execute_once()

        info_records = [r for r in logger.records if r[0] == 'info']
        assert any('Polling cycle complete' in r[1] for r in info_records)


class TestPollingJobTeardown:
    def test_teardown_stores_last_polled(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock([now, now, now])
        logger = FakeLogger()
        pr_source = FakePRSource([])
        queue = FakeQueue[FetchedPR](clock)
        polling_state = FakePollingState(now - timedelta(hours=1))

        job = PollingJob(
            logger=logger,
            poll_interval=0,
            pr_source=pr_source,
            output_queue=queue,
            polling_state=polling_state,
            clock=clock,
            default_lookback_hours=24,
        )
        job.setup()
        job.execute_once()
        initial_count = len(polling_state.stored_values)
        job.teardown()

        assert len(polling_state.stored_values) == initial_count + 1


class TestPollingJobMultiplePRs:
    def test_fetches_multiple_prs_in_order(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged1 = datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc)
        merged2 = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        merged3 = datetime(2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc)

        prs = [
            _make_fetched_pr(1, merged1, 'PR 1'),
            _make_fetched_pr(2, merged2, 'PR 2'),
            _make_fetched_pr(3, merged3, 'PR 3'),
        ]
        clock = FakeClock(now)
        logger = FakeLogger()
        pr_source = FakePRSource(prs)
        queue = FakeQueue[FetchedPR](clock)
        polling_state = FakePollingState(merged1 - timedelta(hours=1))

        job = PollingJob(
            logger=logger,
            poll_interval=0,
            pr_source=pr_source,
            output_queue=queue,
            polling_state=polling_state,
            clock=clock,
            default_lookback_hours=24,
        )
        job.setup()
        job.execute_once()

        assert queue.size() == 3
