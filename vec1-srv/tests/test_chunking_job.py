from datetime import datetime, timedelta, timezone

from redis import Redis

from tests.fakes import FakeClock, FakeLogger
from vec1.core.chunkers import DescriptionChunker, DiffChunker
from vec1.core.jobs.chunking import ChunkingJob
from vec1.core.schema.chunk import Chunk, ChunkType
from vec1.core.schema.pr import (
    FetchedPR,
    PRDiff,
    PRIdentifier,
    PRMetadata,
)
from vec1.infra.queue.memory import MemoryQueue
from vec1.infra.queue.redis import RedisQueue


def _make_fetched_pr(
    pr_number: int = 1,
    title: str = "Test PR",
    description: str = "Test description",
    patch: str = "@@ -1 +1 @@\n-old\n+new",
) -> FetchedPR:
    now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    identifier = PRIdentifier(
        source="github",
        owner="test-owner",
        repo="test-repo",
        pr_number=pr_number,
    )
    metadata = PRMetadata(
        identifier=identifier,
        title=title,
        description=description,
        author="test-author",
        created_at=now - timedelta(hours=1),
        merged_at=now,
        base_branch="main",
        head_branch="feature",
        files_changed=("file1.py",),
    )
    diffs = (
        PRDiff(
            file_path="file1.py",
            patch=patch,
            additions=1,
            deletions=1,
        ),
    )
    return FetchedPR(metadata=metadata, diffs=diffs, review_comments=())


def _simple_hasher(content: str) -> str:
    return f"hash-{len(content)}"


def _simple_token_counter(content: str) -> int:
    return len(content.split())


class TestChunkingJobWithMemoryQueue:
    def test_chunks_pr_from_input_queue(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        logger = FakeLogger()
        input_queue: MemoryQueue[FetchedPR] = MemoryQueue(clock, timeout=0.1)
        output_queue: MemoryQueue[Chunk] = MemoryQueue(clock, timeout=0.1)
        chunkers = [
            DescriptionChunker(_simple_hasher, _simple_token_counter),
            DiffChunker(_simple_hasher, _simple_token_counter),
        ]

        job = ChunkingJob(
            logger=logger,
            clock=clock,
            input_queue=input_queue,
            output_queue=output_queue,
            chunkers=chunkers,
        )

        fetched_pr = _make_fetched_pr()
        input_queue.put(fetched_pr)

        job.setup()
        job.execute_once()

        assert output_queue.size() == 2

    def test_produces_summary_and_diff_chunks(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        logger = FakeLogger()
        input_queue: MemoryQueue[FetchedPR] = MemoryQueue(clock, timeout=0.1)
        output_queue: MemoryQueue[Chunk] = MemoryQueue(clock, timeout=0.1)
        chunkers = [
            DescriptionChunker(_simple_hasher, _simple_token_counter),
            DiffChunker(_simple_hasher, _simple_token_counter),
        ]

        job = ChunkingJob(
            logger=logger,
            clock=clock,
            input_queue=input_queue,
            output_queue=output_queue,
            chunkers=chunkers,
        )

        fetched_pr = _make_fetched_pr()
        input_queue.put(fetched_pr)

        job.setup()
        job.execute_once()

        chunks = []
        while output_queue.size() > 0:
            msg = output_queue.get()
            if msg:
                output_queue.ack(msg)
                chunks.append(msg.payload)

        chunk_types = {c.chunk_type for c in chunks}
        assert chunk_types == {ChunkType.SUMMARY, ChunkType.DIFF}

    def test_acks_message_on_success(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        logger = FakeLogger()
        input_queue: MemoryQueue[FetchedPR] = MemoryQueue(clock, timeout=0.1)
        output_queue: MemoryQueue[Chunk] = MemoryQueue(clock, timeout=0.1)
        chunkers = [
            DescriptionChunker(_simple_hasher, _simple_token_counter),
        ]

        job = ChunkingJob(
            logger=logger,
            clock=clock,
            input_queue=input_queue,
            output_queue=output_queue,
            chunkers=chunkers,
        )

        fetched_pr = _make_fetched_pr()
        input_queue.put(fetched_pr)

        job.setup()
        job.execute_once()

        assert len(input_queue._pending) == 0

    def test_does_nothing_when_queue_empty(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        logger = FakeLogger()
        input_queue: MemoryQueue[FetchedPR] = MemoryQueue(clock, timeout=0.1)
        output_queue: MemoryQueue[Chunk] = MemoryQueue(clock, timeout=0.1)
        chunkers = [
            DescriptionChunker(_simple_hasher, _simple_token_counter),
        ]

        job = ChunkingJob(
            logger=logger,
            clock=clock,
            input_queue=input_queue,
            output_queue=output_queue,
            chunkers=chunkers,
        )

        job.setup()
        job.execute_once()

        assert output_queue.size() == 0

    def test_splits_large_diff_into_hunks(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        logger = FakeLogger()
        input_queue: MemoryQueue[FetchedPR] = MemoryQueue(clock, timeout=0.1)
        output_queue: MemoryQueue[Chunk] = MemoryQueue(clock, timeout=0.1)

        large_patch = (
            "@@ -1,10 +1,10 @@\n"
            + "-old\n+new\n" * 500
            + "\n@@ -100,10 +100,10 @@\n"
            + "-old2\n+new2\n" * 500
        )
        chunkers = [
            DiffChunker(_simple_hasher, _simple_token_counter, max_tokens=100),
        ]

        job = ChunkingJob(
            logger=logger,
            clock=clock,
            input_queue=input_queue,
            output_queue=output_queue,
            chunkers=chunkers,
        )

        fetched_pr = _make_fetched_pr(patch=large_patch)
        input_queue.put(fetched_pr)

        job.setup()
        job.execute_once()

        assert output_queue.size() == 2

    def test_logs_chunked_pr(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        logger = FakeLogger()
        input_queue: MemoryQueue[FetchedPR] = MemoryQueue(clock, timeout=0.1)
        output_queue: MemoryQueue[Chunk] = MemoryQueue(clock, timeout=0.1)
        chunkers = [
            DescriptionChunker(_simple_hasher, _simple_token_counter),
        ]

        job = ChunkingJob(
            logger=logger,
            clock=clock,
            input_queue=input_queue,
            output_queue=output_queue,
            chunkers=chunkers,
        )

        fetched_pr = _make_fetched_pr()
        input_queue.put(fetched_pr)

        job.setup()
        job.execute_once()

        info_records = [r for r in logger.records if r[0] == "info"]
        assert any("Chunked pull request" in r[1] for r in info_records)


class TestChunkingJobWithRedisQueue:
    def test_queue_integration_put_get_ack(self, redis_client: Redis) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: RedisQueue[dict] = RedisQueue(clock, redis_client, "test-queue")

        payload = {"title": "Test PR", "number": 1}
        queue.put(payload)

        msg = queue.get()
        assert msg is not None
        assert msg.payload == payload

        queue.ack(msg)

        pending = redis_client.xpending("vec1:stream:test-queue", "vec1:consumers")
        assert pending["pending"] == 0

    def test_queue_integration_nack_requeue(self, redis_client: Redis) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: RedisQueue[dict] = RedisQueue(clock, redis_client, "test-queue")

        payload = {"title": "Test PR", "number": 1}
        queue.put(payload)

        msg = queue.get()
        assert msg is not None
        queue.nack(msg, requeue=True)

        msg2 = queue.get()
        assert msg2 is not None
        assert msg2.payload == payload
        assert msg2.message_id == msg.message_id

    def test_multiple_consumers_get_different_items(self, redis_client: Redis) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue1: RedisQueue[dict] = RedisQueue(
            clock, redis_client, "test-queue", consumer_name="worker-1"
        )
        queue2: RedisQueue[dict] = RedisQueue(
            clock, redis_client, "test-queue", consumer_name="worker-2"
        )

        queue1.put({"number": 1})
        queue1.put({"number": 2})

        msg1 = queue1.get()
        msg2 = queue2.get()

        assert msg1 is not None and msg2 is not None
        assert msg1.payload["number"] == 1
        assert msg2.payload["number"] == 2

    def test_input_output_queue_flow(self, redis_client: Redis) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        input_queue: RedisQueue[dict] = RedisQueue(clock, redis_client, "input-queue")
        output_queue: RedisQueue[dict] = RedisQueue(clock, redis_client, "output-queue")

        input_queue.put({"pr_number": 1, "title": "Test PR"})

        msg = input_queue.get()
        assert msg is not None

        output_queue.put({"chunk_type": "summary", "content": msg.payload["title"]})
        output_queue.put({"chunk_type": "diff", "content": "diff content"})

        input_queue.ack(msg)

        assert output_queue.size() == 2

        chunk1 = output_queue.get()
        output_queue.ack(chunk1)
        chunk2 = output_queue.get()
        output_queue.ack(chunk2)

        chunk_types = {chunk1.payload["chunk_type"], chunk2.payload["chunk_type"]}
        assert chunk_types == {"summary", "diff"}
