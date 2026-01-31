from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, call

import pytest

from vec1.core.exceptions import (
    PRFetchError,
    QueueFullError,
    SourceAuthenticationError,
    SourceRateLimitError,
)
from vec1.core.jobs.polling import PollingJob
from vec1.core.schema.pr import (
    FetchedPR,
    PRDiff,
    PRIdentifier,
    PRMetadata,
)


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


@pytest.fixture
def mock_logger():
    logger = Mock()
    logger.info = Mock()
    logger.error = Mock()
    logger.warning = Mock()
    logger.debug = Mock()
    logger.exception = Mock()
    return logger


@pytest.fixture
def mock_clock():
    clock = Mock()
    clock.sleep = Mock()
    return clock


@pytest.fixture
def mock_pr_source():
    return Mock()


@pytest.fixture
def mock_queue():
    queue = Mock()
    queue.put = Mock()
    queue.get = Mock()
    queue.size = Mock(return_value=0)
    return queue


@pytest.fixture
def mock_polling_state():
    state = Mock()
    state.load_last_polled = Mock()
    state.store_last_polled = Mock()
    return state


def _create_job(
    pr_source,
    clock,
    polling_state,
    queue=None,
    logger=None,
    poll_interval: float = 0,
    default_lookback_hours: int = 24,
):
    if logger is None:
        logger = Mock()
        logger.info = Mock()
    if queue is None:
        queue = Mock()
        queue.put = Mock()

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


class TestSetupMethod:
    def test_setup_loads_state_from_polling_state(self, mock_logger, mock_clock, mock_pr_source, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        last_polled = now - timedelta(hours=1)

        mock_polling_state.load_last_polled.return_value = last_polled

        job, _, _ = _create_job(
            mock_pr_source, mock_clock, mock_polling_state,
            queue=mock_queue, logger=mock_logger
        )
        job.setup()

        mock_polling_state.load_last_polled.assert_called_once()
        assert job._last_polled == last_polled

    def test_setup_uses_datetime_now_when_state_returns_none(self, mock_logger, mock_pr_source, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        mock_clock = Mock()
        mock_clock.now.return_value = now

        mock_polling_state.load_last_polled.return_value = None

        job, _, _ = _create_job(
            mock_pr_source, mock_clock, mock_polling_state,
            queue=mock_queue, logger=mock_logger,
            default_lookback_hours=48
        )
        job.setup()

        assert job._last_polled is not None
        assert job._last_polled == now - timedelta(hours=48)

    def test_setup_logs_initialization_with_last_polled(self, mock_clock, mock_pr_source, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        last_polled = now - timedelta(hours=1)

        mock_logger = Mock()
        mock_polling_state.load_last_polled.return_value = last_polled

        job, _, _ = _create_job(
            mock_pr_source, mock_clock, mock_polling_state,
            queue=mock_queue, logger=mock_logger
        )
        job.setup()

        mock_logger.info.assert_called_once_with(
            "Polling initialized",
            last_polled=last_polled.isoformat(),
        )


class TestExecuteOnceMethod:
    def test_execute_once_fetches_prs_from_source(self, mock_logger, mock_clock, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        last_polled = now - timedelta(hours=1)
        merged_at = now - timedelta(minutes=30)

        pr = _make_fetched_pr(1, merged_at)

        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([pr])
        mock_clock.now.return_value = now
        mock_polling_state.load_last_polled.return_value = last_polled

        job, _, _ = _create_job(
            mock_pr_source, mock_clock, mock_polling_state,
            queue=mock_queue, logger=mock_logger
        )
        job.setup()
        job.execute_once()

        mock_pr_source.fetch_merged_since.assert_called_once_with(last_polled)

    def test_execute_once_enqueues_all_fetched_prs(self, mock_logger, mock_clock, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        last_polled = now - timedelta(hours=2)
        merged_time = now - timedelta(hours=1)

        prs = [
            _make_fetched_pr(1, merged_time),
            _make_fetched_pr(2, merged_time + timedelta(minutes=10)),
            _make_fetched_pr(3, merged_time + timedelta(minutes=20)),
        ]

        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter(prs)
        mock_clock.now.return_value = now
        mock_polling_state.load_last_polled.return_value = last_polled

        job, _, _ = _create_job(
            mock_pr_source, mock_clock, mock_polling_state,
            queue=mock_queue, logger=mock_logger
        )
        job.setup()
        job.execute_once()

        assert mock_queue.put.call_count == 3
        for i, pr in enumerate(prs):
            assert mock_queue.put.call_args_list[i] == call(pr)

    def test_execute_once_updates_last_polled_to_current_time(self, mock_logger, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        last_polled = now - timedelta(hours=1)

        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([])

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_polling_state.load_last_polled.return_value = last_polled

        job, _, _ = _create_job(
            mock_pr_source, mock_clock, mock_polling_state,
            queue=mock_queue, logger=mock_logger
        )
        job.setup()
        job.execute_once()

        assert job._last_polled == now
        mock_polling_state.store_last_polled.assert_called_once_with(now)

    def test_execute_once_logs_processed_count(self, mock_logger, mock_clock, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        last_polled = now - timedelta(hours=1)
        merged_at = now - timedelta(minutes=30)

        prs = [
            _make_fetched_pr(1, merged_at),
            _make_fetched_pr(2, merged_at + timedelta(minutes=5)),
        ]

        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter(prs)
        mock_clock.now.return_value = now
        mock_polling_state.load_last_polled.return_value = last_polled

        job, _, _ = _create_job(
            mock_pr_source, mock_clock, mock_polling_state,
            queue=mock_queue, logger=mock_logger
        )
        job.setup()
        job.execute_once()

        info_calls = [c for c in mock_logger.info.call_args_list if "Polling cycle complete" in str(c)]
        assert len(info_calls) == 1
        assert info_calls[0] == call(
            "Polling cycle complete",
            processed=2,
            last_polled=now.isoformat(),
        )

    def test_execute_once_with_zero_prs_logs_zero_count(self, mock_logger, mock_clock, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        last_polled = now - timedelta(hours=1)

        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([])
        mock_clock.now.return_value = now
        mock_polling_state.load_last_polled.return_value = last_polled

        job, _, _ = _create_job(
            mock_pr_source, mock_clock, mock_polling_state,
            queue=mock_queue, logger=mock_logger
        )
        job.setup()
        job.execute_once()

        info_calls = [c for c in mock_logger.info.call_args_list if "Polling cycle complete" in str(c)]
        assert len(info_calls) == 1
        assert info_calls[0][1]["processed"] == 0


class TestTeardownMethod:
    def test_teardown_stores_last_polled_when_not_none(self, mock_logger, mock_clock, mock_pr_source, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        last_polled = now - timedelta(hours=1)

        mock_polling_state.load_last_polled.return_value = last_polled

        job, _, _ = _create_job(
            mock_pr_source, mock_clock, mock_polling_state,
            queue=mock_queue, logger=mock_logger
        )
        job.setup()

        mock_polling_state.store_last_polled.reset_mock()

        job.teardown()

        mock_polling_state.store_last_polled.assert_called_once_with(last_polled)

    def test_teardown_does_not_store_when_last_polled_is_none(self, mock_logger, mock_clock, mock_pr_source, mock_queue, mock_polling_state):
        job, _, _ = _create_job(
            mock_pr_source, mock_clock, mock_polling_state,
            queue=mock_queue, logger=mock_logger
        )

        job._last_polled = None

        job.teardown()

        mock_polling_state.store_last_polled.assert_not_called()

    def test_teardown_after_execute_once_stores_updated_time(self, mock_logger, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        last_polled = now - timedelta(hours=1)

        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([])

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_polling_state.load_last_polled.return_value = last_polled

        job, _, _ = _create_job(
            mock_pr_source, mock_clock, mock_polling_state,
            queue=mock_queue, logger=mock_logger
        )
        job.setup()
        job.execute_once()

        mock_polling_state.store_last_polled.reset_mock()

        job.teardown()

        mock_polling_state.store_last_polled.assert_called_once_with(now)


class TestEndToEndPollingFlow:
    def test_complete_flow_with_multiple_prs_from_multiple_repos(self, mock_logger, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_time = now - timedelta(hours=2)

        prs = [
            _make_fetched_pr(1, merged_time, "PR from repo A", owner="org1", repo="repoA"),
            _make_fetched_pr(2, merged_time + timedelta(minutes=30), "PR from repo B", owner="org1", repo="repoB"),
            _make_fetched_pr(3, merged_time + timedelta(hours=1), "PR from repo C", owner="org2", repo="repoC"),
        ]

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter(prs)
        mock_polling_state.load_last_polled.return_value = merged_time - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()
        job.execute_once()

        assert mock_queue.put.call_count == 3
        repos = {call_args[0][0].metadata.identifier.repo for call_args in mock_queue.put.call_args_list}
        assert repos == {"repoA", "repoB", "repoC"}

    def test_complete_flow_with_prs_from_different_authors(self, mock_logger, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_time = now - timedelta(hours=2)

        prs = [
            _make_fetched_pr(1, merged_time, "PR by Alice", author="alice"),
            _make_fetched_pr(2, merged_time + timedelta(minutes=15), "PR by Bob", author="bob"),
            _make_fetched_pr(3, merged_time + timedelta(minutes=30), "PR by Charlie", author="charlie"),
            _make_fetched_pr(4, merged_time + timedelta(minutes=45), "Another PR by Alice", author="alice"),
        ]

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter(prs)
        mock_polling_state.load_last_polled.return_value = merged_time - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()
        job.execute_once()

        assert mock_queue.put.call_count == 4
        authors = [call_args[0][0].metadata.author for call_args in mock_queue.put.call_args_list]
        assert authors.count("alice") == 2
        assert authors.count("bob") == 1
        assert authors.count("charlie") == 1

    def test_complete_flow_preserves_pr_order(self, mock_logger, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        base_time = now - timedelta(hours=3)

        prs = [
            _make_fetched_pr(i, base_time + timedelta(minutes=i * 10), f"PR {i}")
            for i in range(1, 11)
        ]

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter(prs)
        mock_polling_state.load_last_polled.return_value = base_time - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()
        job.execute_once()

        assert mock_queue.put.call_count == 10
        for expected_number, call_args in enumerate(mock_queue.put.call_args_list, 1):
            assert call_args[0][0].metadata.identifier.pr_number == expected_number

    def test_complete_flow_with_large_pr_batch(self, mock_logger, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        base_time = now - timedelta(hours=6)

        prs = [
            _make_fetched_pr(i, base_time + timedelta(minutes=i), f"PR {i}")
            for i in range(1, 101)
        ]

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter(prs)
        mock_polling_state.load_last_polled.return_value = base_time - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()
        job.execute_once()

        assert mock_queue.put.call_count == 100
        info_calls = [c for c in mock_logger.info.call_args_list if "Polling cycle complete" in str(c)]
        assert len(info_calls) == 1
        assert info_calls[0][1]["processed"] == 100


class TestEdgeCases:
    def test_empty_results_still_updates_state(self, mock_logger, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([])
        mock_polling_state.load_last_polled.return_value = now - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()
        job.execute_once()

        assert mock_queue.put.call_count == 0
        mock_polling_state.store_last_polled.assert_called_once_with(now)
        info_calls = [c for c in mock_logger.info.call_args_list if "Polling cycle complete" in str(c)]
        assert info_calls[0][1]["processed"] == 0

    def test_pr_source_error_is_propagated(self, mock_logger, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        identifier = PRIdentifier("github", "owner", "repo", 1)
        error = PRFetchError("Failed to fetch PR", identifier)

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.side_effect = error
        mock_polling_state.load_last_polled.return_value = now - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()

        with pytest.raises(PRFetchError) as exc_info:
            job.execute_once()
        assert exc_info.value.pr_identifier == identifier

    def test_authentication_error_from_source(self, mock_logger, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        error = SourceAuthenticationError("Invalid token")

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.side_effect = error
        mock_polling_state.load_last_polled.return_value = now - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()

        with pytest.raises(SourceAuthenticationError) as exc_info:
            job.execute_once()
        assert "Invalid token" in str(exc_info.value)

    def test_rate_limit_error_from_source(self, mock_logger, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        retry_after = now + timedelta(minutes=5)
        error = SourceRateLimitError("Rate limited", retry_after)

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.side_effect = error
        mock_polling_state.load_last_polled.return_value = now - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()

        with pytest.raises(SourceRateLimitError) as exc_info:
            job.execute_once()
        assert exc_info.value.retry_after == retry_after

    def test_partial_error_during_iteration(self, mock_logger, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_time = now - timedelta(hours=2)

        prs = [
            _make_fetched_pr(1, merged_time, "PR 1"),
            _make_fetched_pr(2, merged_time + timedelta(minutes=10), "PR 2"),
        ]

        identifier = PRIdentifier("github", "owner", "repo", 3)
        error = PRFetchError("Failed on PR 3", identifier)

        def generator_with_error():
            yield prs[0]
            yield prs[1]
            raise error

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = generator_with_error()
        mock_polling_state.load_last_polled.return_value = merged_time - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()

        with pytest.raises(PRFetchError):
            job.execute_once()

        assert mock_queue.put.call_count == 2

    def test_non_vec1_error_propagates_unchanged(self, mock_logger, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        error = RuntimeError("Unexpected error")

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.side_effect = error
        mock_polling_state.load_last_polled.return_value = now - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()

        with pytest.raises(RuntimeError) as exc_info:
            job.execute_once()
        assert "Unexpected error" in str(exc_info.value)


class TestStatePersistence:
    def test_state_loaded_once_during_setup(self, mock_logger, mock_clock, mock_pr_source, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        mock_polling_state.load_last_polled.return_value = now - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()

        assert mock_polling_state.load_last_polled.call_count == 1

    def test_state_stored_after_each_execute_once(self, mock_logger, mock_queue, mock_polling_state):
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        mock_clock = Mock()
        mock_clock.now.return_value = t1
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([])
        mock_polling_state.load_last_polled.return_value = t1 - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()

        mock_polling_state.store_last_polled.reset_mock()

        job.execute_once()
        job.execute_once()
        job.execute_once()

        assert mock_polling_state.store_last_polled.call_count == 3
        for call_args in mock_polling_state.store_last_polled.call_args_list:
            assert call_args[0][0] == t1

    def test_state_persists_between_cycles(self, mock_logger, mock_queue):
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        initial_state = t1 - timedelta(hours=1)

        mock_clock = Mock()
        mock_clock.now.return_value = t1
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([])
        mock_polling_state = Mock()
        mock_polling_state.load_last_polled.return_value = initial_state

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()
        job.execute_once()
        job.execute_once()

        assert mock_pr_source.fetch_merged_since.call_count == 2
        assert mock_pr_source.fetch_merged_since.call_args_list[0] == call(initial_state)
        assert mock_pr_source.fetch_merged_since.call_args_list[1] == call(t1)

    def test_teardown_stores_final_state(self, mock_logger, mock_queue, mock_polling_state):
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        mock_clock = Mock()
        mock_clock.now.return_value = t1
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([])
        mock_polling_state.load_last_polled.return_value = t1 - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()
        job.execute_once()

        initial_call_count = mock_polling_state.store_last_polled.call_count

        job.teardown()

        assert mock_polling_state.store_last_polled.call_count == initial_call_count + 1
        assert mock_polling_state.store_last_polled.call_args_list[-1] == call(t1)

    def test_teardown_without_execute_stores_initial_state(self, mock_logger, mock_clock, mock_pr_source, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        initial_last_polled = now - timedelta(hours=1)

        mock_polling_state.load_last_polled.return_value = initial_last_polled

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()

        mock_polling_state.store_last_polled.reset_mock()

        job.teardown()

        mock_polling_state.store_last_polled.assert_called_once_with(initial_last_polled)

    def test_state_continuity_across_job_instances(self, mock_logger, mock_queue):
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 15, 12, 5, 0, tzinfo=timezone.utc)

        mock_clock1 = Mock()
        mock_clock1.now.return_value = t1
        mock_pr_source1 = Mock()
        mock_pr_source1.fetch_merged_since.return_value = iter([])
        mock_polling_state = Mock()
        mock_polling_state.load_last_polled.return_value = None

        job1, _, _ = _create_job(
            mock_pr_source1, mock_clock1, mock_polling_state,
            queue=mock_queue, logger=mock_logger,
            default_lookback_hours=24
        )
        job1.setup()
        job1.execute_once()
        job1.teardown()

        stored_time = mock_polling_state.store_last_polled.call_args_list[-1][0][0]
        assert stored_time == t1

        mock_clock2 = Mock()
        mock_clock2.now.return_value = t2
        mock_pr_source2 = Mock()
        mock_pr_source2.fetch_merged_since.return_value = iter([])
        mock_polling_state.load_last_polled.return_value = t1

        job2, _, _ = _create_job(mock_pr_source2, mock_clock2, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job2.setup()
        job2.execute_once()

        mock_pr_source2.fetch_merged_since.assert_called_once_with(t1)


class TestQueueInteractions:
    def test_queue_receives_prs_in_correct_order(self, mock_logger, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = now - timedelta(hours=1)
        pr = _make_fetched_pr(42, merged_at, "Test PR", owner="myorg", repo="myrepo")

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([pr])
        mock_polling_state.load_last_polled.return_value = merged_at - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()
        job.execute_once()

        mock_queue.put.assert_called_once_with(pr)
        assert mock_queue.put.call_args[0][0].metadata.identifier.pr_number == 42
        assert mock_queue.put.call_args[0][0].metadata.identifier.owner == "myorg"
        assert mock_queue.put.call_args[0][0].metadata.identifier.repo == "myrepo"
        assert mock_queue.put.call_args[0][0].metadata.title == "Test PR"

    def test_queue_full_error_propagates(self, mock_logger, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = now - timedelta(hours=1)

        prs = [
            _make_fetched_pr(1, merged_at, "PR 1"),
            _make_fetched_pr(2, merged_at + timedelta(minutes=10), "PR 2"),
            _make_fetched_pr(3, merged_at + timedelta(minutes=20), "PR 3"),
        ]

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter(prs)
        mock_polling_state.load_last_polled.return_value = merged_at - timedelta(hours=1)

        mock_queue = Mock()
        call_count = [0]

        def put_with_limit(item):
            call_count[0] += 1
            if call_count[0] > 2:
                raise QueueFullError("Queue is full")

        mock_queue.put.side_effect = put_with_limit

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()

        with pytest.raises(QueueFullError):
            job.execute_once()

        assert mock_queue.put.call_count == 3

    def test_queue_full_with_single_item_capacity(self, mock_logger, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = now - timedelta(hours=1)

        prs = [
            _make_fetched_pr(1, merged_at, "PR 1"),
            _make_fetched_pr(2, merged_at + timedelta(minutes=10), "PR 2"),
        ]

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter(prs)
        mock_polling_state.load_last_polled.return_value = merged_at - timedelta(hours=1)

        mock_queue = Mock()
        call_count = [0]

        def put_with_single_limit(item):
            call_count[0] += 1
            if call_count[0] > 1:
                raise QueueFullError("Queue is full")

        mock_queue.put.side_effect = put_with_single_limit

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()

        with pytest.raises(QueueFullError):
            job.execute_once()

        assert mock_queue.put.call_count == 2
        assert mock_queue.put.call_args_list[0][0][0].metadata.identifier.pr_number == 1

    def test_multiple_prs_enqueue_sequentially(self, mock_logger, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = now - timedelta(hours=2)

        prs = [
            _make_fetched_pr(1, merged_at),
            _make_fetched_pr(2, merged_at + timedelta(minutes=30)),
            _make_fetched_pr(3, merged_at + timedelta(hours=1)),
        ]

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter(prs)
        mock_polling_state.load_last_polled.return_value = merged_at - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()
        job.execute_once()

        pr_numbers = [call_args[0][0].metadata.identifier.pr_number for call_args in mock_queue.put.call_args_list]
        assert pr_numbers == [1, 2, 3]


class TestClockTimeHandling:
    def test_uses_current_datetime_when_no_prior_state_exists(self, mock_logger, mock_queue):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        expected_lookback = now - timedelta(hours=24)

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([])
        mock_polling_state = Mock()
        mock_polling_state.load_last_polled.return_value = None

        job, _, _ = _create_job(
            mock_pr_source, mock_clock, mock_polling_state,
            queue=mock_queue, logger=mock_logger,
            default_lookback_hours=24
        )
        job.setup()
        job.execute_once()

        assert mock_pr_source.fetch_merged_since.call_count == 1
        call_arg = mock_pr_source.fetch_merged_since.call_args[0][0]
        assert isinstance(call_arg, datetime)
        assert call_arg == expected_lookback

    def test_clock_advances_between_cycles(self, mock_logger, mock_queue, mock_polling_state):
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        mock_clock = Mock()
        mock_clock.now.return_value = t1
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([])
        mock_polling_state.load_last_polled.return_value = t1 - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()

        mock_polling_state.store_last_polled.reset_mock()

        job.execute_once()
        job.execute_once()
        job.execute_once()

        assert mock_polling_state.store_last_polled.call_count == 3
        for call_args in mock_polling_state.store_last_polled.call_args_list:
            assert call_args[0][0] == t1

    def test_poll_interval_calls_clock_sleep(self, mock_logger, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        poll_interval = 30.0

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([])
        mock_polling_state.load_last_polled.return_value = now - timedelta(hours=1)

        job, _, _ = _create_job(
            mock_pr_source, mock_clock, mock_polling_state,
            queue=mock_queue, logger=mock_logger,
            poll_interval=poll_interval
        )
        job.setup()
        job.execute_once()
        job._sleep(poll_interval)

        mock_clock.sleep.assert_called_once_with(poll_interval)

    def test_timezone_aware_datetimes_preserved(self, mock_logger, mock_queue):
        utc_now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        pr = _make_fetched_pr(1, merged_at)

        mock_clock = Mock()
        mock_clock.now.return_value = utc_now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([pr])
        mock_polling_state = Mock()
        mock_polling_state.load_last_polled.return_value = merged_at - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()
        job.execute_once()

        stored_time = mock_polling_state.store_last_polled.call_args[0][0]
        assert stored_time.tzinfo == timezone.utc


class TestMultiplePollingCycles:
    def test_multiple_cycles_fetch_different_prs(self, mock_logger, mock_queue):
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        pr1 = _make_fetched_pr(1, t1 - timedelta(minutes=30), "PR 1")
        pr2 = _make_fetched_pr(2, t1 + timedelta(minutes=2), "PR 2")
        pr3 = _make_fetched_pr(3, t1 + timedelta(minutes=10), "PR 3")

        mock_clock = Mock()
        mock_clock.now.return_value = t1
        mock_pr_source = Mock()

        call_count = [0]

        def fetch_by_cycle(since):
            result = [iter([pr1]), iter([pr2]), iter([pr3])][call_count[0]]
            call_count[0] += 1
            return result

        mock_pr_source.fetch_merged_since.side_effect = fetch_by_cycle
        mock_polling_state = Mock()
        mock_polling_state.load_last_polled.return_value = t1 - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()

        job.execute_once()
        assert mock_queue.put.call_count == 1

        job.execute_once()
        assert mock_queue.put.call_count == 2

        job.execute_once()
        assert mock_queue.put.call_count == 3

    def test_consecutive_empty_cycles(self, mock_logger, mock_queue, mock_polling_state):
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        mock_clock = Mock()
        mock_clock.now.return_value = t1
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([])
        mock_polling_state.load_last_polled.return_value = t1 - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()

        for _ in range(3):
            job.execute_once()

        assert mock_queue.put.call_count == 0
        info_calls = [c for c in mock_logger.info.call_args_list if "Polling cycle complete" in str(c)]
        assert len(info_calls) == 3
        assert all(c[1]["processed"] == 0 for c in info_calls)

    def test_alternating_empty_and_nonempty_cycles(self, mock_logger, mock_queue):
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        pr1 = _make_fetched_pr(1, t1 - timedelta(minutes=5), "PR 1")
        pr3 = _make_fetched_pr(3, t1 + timedelta(minutes=5), "PR 3")

        mock_clock = Mock()
        mock_clock.now.return_value = t1
        mock_pr_source = Mock()

        call_count = [0]

        def fetch_alternating(since):
            result = [iter([pr1]), iter([]), iter([pr3]), iter([])][call_count[0]]
            call_count[0] += 1
            return result

        mock_pr_source.fetch_merged_since.side_effect = fetch_alternating
        mock_polling_state = Mock()
        mock_polling_state.load_last_polled.return_value = t1 - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()

        for _ in range(4):
            job.execute_once()

        assert mock_queue.put.call_count == 2
        info_calls = [c for c in mock_logger.info.call_args_list if "Polling cycle complete" in str(c)]
        processed_counts = [c[1]["processed"] for c in info_calls]
        assert processed_counts == [1, 0, 1, 0]

    def test_cycle_count_tracking_in_logs(self, mock_logger, mock_queue, mock_polling_state):
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = t1 - timedelta(hours=1)

        prs = [
            _make_fetched_pr(1, merged_at),
            _make_fetched_pr(2, merged_at + timedelta(minutes=10)),
            _make_fetched_pr(3, merged_at + timedelta(minutes=20)),
        ]

        mock_clock = Mock()
        mock_clock.now.return_value = t1
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter(prs)
        mock_polling_state.load_last_polled.return_value = merged_at - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()
        job.execute_once()

        info_calls = [c for c in mock_logger.info.call_args_list if "Polling cycle complete" in str(c)]
        assert len(info_calls) == 1
        assert info_calls[0][1]["processed"] == 3


class TestErrorRecovery:
    def test_error_on_first_cycle_then_success(self, mock_logger, mock_queue):
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 15, 12, 5, 0, tzinfo=timezone.utc)
        merged_at = t1 - timedelta(hours=1)

        pr = _make_fetched_pr(1, merged_at, "PR 1")
        identifier = PRIdentifier("github", "owner", "repo", 1)
        error = PRFetchError("Failed", identifier)

        mock_clock1 = Mock()
        mock_clock1.now.return_value = t1
        mock_pr_source1 = Mock()
        mock_pr_source1.fetch_merged_since.side_effect = error
        mock_polling_state = Mock()
        mock_polling_state.load_last_polled.return_value = merged_at - timedelta(hours=1)

        job1, _, queue1 = _create_job(mock_pr_source1, mock_clock1, mock_polling_state, logger=mock_logger)
        job1.setup()

        with pytest.raises(PRFetchError):
            job1.execute_once()

        mock_clock2 = Mock()
        mock_clock2.now.return_value = t2
        mock_pr_source2 = Mock()
        mock_pr_source2.fetch_merged_since.return_value = iter([pr])
        mock_polling_state.load_last_polled.return_value = merged_at - timedelta(hours=1)

        job2, _, queue2 = _create_job(mock_pr_source2, mock_clock2, mock_polling_state, logger=mock_logger)
        job2.setup()
        job2.execute_once()

        queue2.put.assert_called_once()

    def test_state_not_updated_on_error(self, mock_logger, mock_queue):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        initial_state = now - timedelta(hours=1)
        identifier = PRIdentifier("github", "owner", "repo", 1)
        error = PRFetchError("Failed", identifier)

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.side_effect = error
        mock_polling_state = Mock()
        mock_polling_state.load_last_polled.return_value = initial_state

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()

        mock_polling_state.store_last_polled.reset_mock()

        with pytest.raises(PRFetchError):
            job.execute_once()

        mock_polling_state.store_last_polled.assert_not_called()

    def test_recovery_continues_from_last_successful_state(self, mock_logger, mock_queue):
        t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        t3 = datetime(2024, 1, 15, 12, 10, 0, tzinfo=timezone.utc)
        merged_at = t1 - timedelta(hours=2)

        pr1 = _make_fetched_pr(1, merged_at, "PR 1")
        identifier = PRIdentifier("github", "owner", "repo", 2)
        error = PRFetchError("Failed", identifier)

        mock_clock = Mock()
        mock_clock.now.side_effect = [t1, t3]

        mock_pr_source = Mock()
        call_count = [0]

        def fetch_with_error(since):
            call_count[0] += 1
            if call_count[0] == 2:
                raise error
            return iter([pr1])

        mock_pr_source.fetch_merged_since.side_effect = fetch_with_error
        mock_polling_state = Mock()
        mock_polling_state.load_last_polled.return_value = merged_at - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()

        job.execute_once()
        assert mock_polling_state.store_last_polled.call_count == 1
        assert mock_polling_state.store_last_polled.call_args_list[0] == call(t1)

        with pytest.raises(PRFetchError):
            job.execute_once()

        assert mock_polling_state.store_last_polled.call_count == 1

        job.execute_once()
        assert mock_polling_state.store_last_polled.call_count == 2
        assert mock_polling_state.store_last_polled.call_args_list[1] == call(t3)

    def test_queue_full_doesnt_update_state(self, mock_logger):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = now - timedelta(hours=1)
        initial_state = merged_at - timedelta(hours=1)

        prs = [
            _make_fetched_pr(1, merged_at, "PR 1"),
            _make_fetched_pr(2, merged_at + timedelta(minutes=10), "PR 2"),
        ]

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter(prs)
        mock_polling_state = Mock()
        mock_polling_state.load_last_polled.return_value = initial_state

        mock_queue = Mock()
        call_count = [0]

        def put_with_limit(item):
            call_count[0] += 1
            if call_count[0] > 1:
                raise QueueFullError("Queue is full")

        mock_queue.put.side_effect = put_with_limit

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()

        mock_polling_state.store_last_polled.reset_mock()

        with pytest.raises(QueueFullError):
            job.execute_once()

        mock_polling_state.store_last_polled.assert_not_called()


class TestBoundaryConditions:
    def test_pr_merged_exactly_at_lookback_time(self, mock_logger, mock_queue):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        lookback_hours = 24
        lookback_time = now - timedelta(hours=lookback_hours)

        pr = _make_fetched_pr(1, lookback_time, "Boundary PR")

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([pr])
        mock_polling_state = Mock()
        mock_polling_state.load_last_polled.return_value = None

        job, _, _ = _create_job(
            mock_pr_source, mock_clock, mock_polling_state,
            queue=mock_queue, logger=mock_logger,
            default_lookback_hours=lookback_hours
        )
        job.setup()
        job.execute_once()

        mock_queue.put.assert_called_once()

    def test_pr_merged_one_second_before_lookback(self, mock_logger, mock_queue):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        lookback_hours = 24
        lookback_time = now - timedelta(hours=lookback_hours)

        pr = _make_fetched_pr(1, lookback_time - timedelta(seconds=1), "Too Old PR")

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([])
        mock_polling_state = Mock()
        mock_polling_state.load_last_polled.return_value = None

        job, _, _ = _create_job(
            mock_pr_source, mock_clock, mock_polling_state,
            queue=mock_queue, logger=mock_logger,
            default_lookback_hours=lookback_hours
        )
        job.setup()
        job.execute_once()

        mock_queue.put.assert_not_called()

    def test_pr_merged_one_second_after_lookback(self, mock_logger, mock_queue):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        lookback_hours = 24
        lookback_time = now - timedelta(hours=lookback_hours)

        pr = _make_fetched_pr(1, lookback_time + timedelta(seconds=1), "Just In PR")

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([pr])
        mock_polling_state = Mock()
        mock_polling_state.load_last_polled.return_value = None

        job, _, _ = _create_job(
            mock_pr_source, mock_clock, mock_polling_state,
            queue=mock_queue, logger=mock_logger,
            default_lookback_hours=lookback_hours
        )
        job.setup()
        job.execute_once()

        mock_queue.put.assert_called_once()

    def test_pr_merged_exactly_at_last_polled_time(self, mock_logger, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        last_polled = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

        pr = _make_fetched_pr(1, last_polled, "Boundary PR")

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([pr])
        mock_polling_state.load_last_polled.return_value = last_polled

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()
        job.execute_once()

        mock_queue.put.assert_called_once()

    def test_zero_poll_interval(self, mock_logger, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([])
        mock_polling_state.load_last_polled.return_value = now - timedelta(hours=1)

        job, _, _ = _create_job(
            mock_pr_source, mock_clock, mock_polling_state,
            queue=mock_queue, logger=mock_logger,
            poll_interval=0
        )
        job.setup()
        job.execute_once()

        mock_clock.sleep.assert_not_called()

    def test_very_small_poll_interval(self, mock_logger, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        poll_interval = 0.001

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([])
        mock_polling_state.load_last_polled.return_value = now - timedelta(hours=1)

        job, _, _ = _create_job(
            mock_pr_source, mock_clock, mock_polling_state,
            queue=mock_queue, logger=mock_logger,
            poll_interval=poll_interval
        )
        job.setup()
        job.execute_once()
        job._sleep(poll_interval)

        mock_clock.sleep.assert_called_once_with(poll_interval)


    def test_pr_with_very_long_title(self, mock_logger, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = now - timedelta(hours=1)
        long_title = "A" * 10000

        pr = _make_fetched_pr(1, merged_at, long_title)

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([pr])
        mock_polling_state.load_last_polled.return_value = merged_at - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()
        job.execute_once()

        mock_queue.put.assert_called_once()
        assert mock_queue.put.call_args[0][0].metadata.title == long_title

    def test_pr_with_many_files_changed(self, mock_logger, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = now - timedelta(hours=1)
        files = tuple(f"file{i}.py" for i in range(1000))

        pr = _make_fetched_pr(1, merged_at, "Many Files PR", files=files)

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([pr])
        mock_polling_state.load_last_polled.return_value = merged_at - timedelta(hours=1)

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()
        job.execute_once()

        mock_queue.put.assert_called_once()
        enqueued_pr = mock_queue.put.call_args[0][0]
        assert len(enqueued_pr.metadata.files_changed) == 1000
        assert len(enqueued_pr.diffs) == 1000


class TestLogging:
    def test_setup_logs_initialization(self, mock_clock, mock_pr_source, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        last_polled = now - timedelta(hours=1)

        mock_logger = Mock()
        mock_polling_state.load_last_polled.return_value = last_polled

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()

        mock_logger.info.assert_called_once_with(
            "Polling initialized",
            last_polled=last_polled.isoformat(),
        )

    def test_execute_once_logs_cycle_complete_with_count(self, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = now - timedelta(hours=1)

        prs = [
            _make_fetched_pr(i, merged_at + timedelta(minutes=i * 5), f"PR {i}")
            for i in range(5)
        ]

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter(prs)
        mock_polling_state.load_last_polled.return_value = merged_at - timedelta(hours=1)
        mock_logger = Mock()

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()
        job.execute_once()

        info_calls = [c for c in mock_logger.info.call_args_list if "Polling cycle complete" in str(c)]
        assert len(info_calls) == 1
        assert info_calls[0] == call(
            "Polling cycle complete",
            processed=5,
            last_polled=now.isoformat(),
        )

    def test_log_format_includes_iso_timestamps(self, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([])
        mock_polling_state.load_last_polled.return_value = now - timedelta(hours=1)
        mock_logger = Mock()

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job.setup()
        job.execute_once()

        all_last_polled = []
        for call_obj in mock_logger.info.call_args_list:
            if "last_polled" in call_obj[1]:
                all_last_polled.append(call_obj[1]["last_polled"])

        for timestamp in all_last_polled:
            datetime.fromisoformat(timestamp)


class TestJobLifecycle:
    def test_full_lifecycle_setup_execute_teardown(self, mock_queue, mock_polling_state):
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        merged_at = now - timedelta(hours=1)
        pr = _make_fetched_pr(1, merged_at, "Test PR")

        mock_clock = Mock()
        mock_clock.now.return_value = now
        mock_pr_source = Mock()
        mock_pr_source.fetch_merged_since.return_value = iter([pr])
        mock_polling_state.load_last_polled.return_value = merged_at - timedelta(hours=1)
        mock_logger = Mock()

        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)

        job.setup()
        job.execute_once()
        job.teardown()

        mock_queue.put.assert_called_once()
        assert mock_polling_state.store_last_polled.call_count == 2
        assert any("Polling initialized" in str(c) for c in mock_logger.info.call_args_list)
        assert any("Polling cycle complete" in str(c) for c in mock_logger.info.call_args_list)

    def test_stop_method_sets_running_to_false(self, mock_clock, mock_pr_source, mock_queue, mock_polling_state, mock_logger):
        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)

        job._running = True
        job.stop()
        assert job._running is False

    def test_should_continue_returns_true_by_default(self, mock_clock, mock_pr_source, mock_queue, mock_polling_state, mock_logger):
        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)

        assert job.should_continue() is True


class TestAssertionInExecuteOnce:
    def test_execute_once_asserts_last_polled_is_not_none(self, mock_logger, mock_clock, mock_pr_source, mock_queue, mock_polling_state):
        job, _, _ = _create_job(mock_pr_source, mock_clock, mock_polling_state, queue=mock_queue, logger=mock_logger)
        job._last_polled = None

        with pytest.raises(AssertionError):
            job.execute_once()
