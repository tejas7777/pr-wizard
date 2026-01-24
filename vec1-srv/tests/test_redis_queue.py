import time
from datetime import datetime, timezone
from uuid import uuid4

from redis import Redis

from tests.fakes import FakeClock
from vec1.infra.queue.redis import RedisQueue


class TestRedisQueueInit:
    def test_creates_consumer_group_on_init(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"

        RedisQueue(clock, redis_client, stream_key)

        groups = redis_client.xinfo_groups(f"vec1:stream:{stream_key}")
        assert len(groups) == 1
        assert groups[0]["name"] == b"vec1:consumers"

    def test_reuses_existing_consumer_group(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"

        RedisQueue(clock, redis_client, stream_key)
        RedisQueue(clock, redis_client, stream_key)

        groups = redis_client.xinfo_groups(f"vec1:stream:{stream_key}")
        assert len(groups) == 1

    def test_uses_provided_consumer_name(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"

        queue: RedisQueue[str] = RedisQueue(
            clock, redis_client, stream_key, consumer_name="worker-1"
        )

        assert queue._consumer_name == "worker-1"

    def test_generates_consumer_name_when_not_provided(
        self, redis_client: Redis
    ) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"

        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)

        assert queue._consumer_name is not None
        assert len(queue._consumer_name) == 36


class TestRedisQueuePut:
    def test_put_adds_item_to_stream(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)

        queue.put("test-item")

        assert queue.size() == 1

    def test_put_multiple_items(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)

        queue.put("item-1")
        queue.put("item-2")
        queue.put("item-3")

        assert queue.size() == 3

    def test_put_complex_payload(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[dict] = RedisQueue(clock, redis_client, stream_key)

        payload = {"key": "value", "nested": {"a": 1, "b": [1, 2, 3]}}
        queue.put(payload)

        message = queue.get()
        assert message is not None
        assert message.payload == payload


class TestRedisQueueGet:
    def test_get_returns_item_with_message_wrapper(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)
        queue.put("test-item")

        message = queue.get()

        assert message is not None
        assert message.payload == "test-item"
        assert message.attempt_count == 1

    def test_get_returns_none_when_empty_and_timeout(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)

        message = queue.get()

        assert message is None

    def test_get_returns_items_in_fifo_order(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)
        queue.put("first")
        queue.put("second")
        queue.put("third")

        msg1 = queue.get()
        queue.ack(msg1)
        msg2 = queue.get()
        queue.ack(msg2)
        msg3 = queue.get()

        assert msg1 is not None and msg1.payload == "first"
        assert msg2 is not None and msg2.payload == "second"
        assert msg3 is not None and msg3.payload == "third"

    def test_get_increments_attempt_count(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)
        queue.put("test-item")

        message = queue.get()

        assert message is not None
        assert message.attempt_count == 1

    def test_get_has_uuid_as_message_id(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)
        queue.put("test-item")

        message = queue.get()

        assert message is not None
        assert len(message.message_id) == 36
        assert message.message_id.count("-") == 4


class TestRedisQueueAck:
    def test_ack_removes_from_pending(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)
        queue.put("test-item")
        message = queue.get()

        assert message is not None
        queue.ack(message)

        pending = redis_client.xpending(f"vec1:stream:{stream_key}", "vec1:consumers")
        assert pending["pending"] == 0

    def test_ack_does_not_remove_from_stream(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)
        queue.put("test-item")
        message = queue.get()

        assert message is not None
        queue.ack(message)

        assert queue.size() == 1


class TestRedisQueueNack:
    def test_nack_with_requeue_adds_back_to_stream(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)
        queue.put("test-item")
        message = queue.get()

        assert message is not None
        queue.nack(message, requeue=True)

        assert queue.size() == 2

    def test_nack_with_requeue_preserves_payload(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)
        queue.put("test-item")
        message = queue.get()

        assert message is not None
        queue.nack(message, requeue=True)

        requeued = queue.get()
        assert requeued is not None
        assert requeued.payload == "test-item"

    def test_nack_with_requeue_preserves_message_id(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)
        queue.put("test-item")
        message = queue.get()

        assert message is not None
        original_id = message.message_id
        queue.nack(message, requeue=True)

        requeued = queue.get()
        assert requeued is not None
        assert requeued.message_id == original_id

    def test_nack_with_requeue_preserves_attempt_count(
        self, redis_client: Redis
    ) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)
        queue.put("test-item")
        message = queue.get()

        assert message is not None
        queue.nack(message, requeue=True)

        requeued = queue.get()
        assert requeued is not None
        assert requeued.attempt_count == 2

    def test_nack_without_requeue_removes_from_pending(
        self, redis_client: Redis
    ) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)
        queue.put("test-item")
        message = queue.get()

        assert message is not None
        queue.nack(message, requeue=False)

        pending = redis_client.xpending(f"vec1:stream:{stream_key}", "vec1:consumers")
        assert pending["pending"] == 0

    def test_nack_without_requeue_does_not_add_to_stream(
        self, redis_client: Redis
    ) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)
        queue.put("test-item")
        message = queue.get()

        assert message is not None
        queue.nack(message, requeue=False)

        assert queue.size() == 1


class TestRedisQueueMessageMetadata:
    def test_message_has_enqueued_timestamp(self, redis_client: Redis) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)
        queue.put("test-item")

        message = queue.get()

        assert message is not None
        assert message.enqueued_at == now

    def test_messages_have_unique_stream_ids(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)
        queue.put("item-1")
        queue.put("item-2")

        msg1 = queue.get()
        queue.ack(msg1)
        msg2 = queue.get()

        assert msg1 is not None and msg2 is not None
        assert msg1.message_id != msg2.message_id


class TestRedisQueueMultipleConsumers:
    def test_different_consumers_get_different_messages(
        self, redis_client: Redis
    ) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue1: RedisQueue[str] = RedisQueue(
            clock, redis_client, stream_key, consumer_name="worker-1"
        )
        queue2: RedisQueue[str] = RedisQueue(
            clock, redis_client, stream_key, consumer_name="worker-2"
        )

        queue1.put("item-1")
        queue1.put("item-2")

        msg1 = queue1.get()
        msg2 = queue2.get()

        assert msg1 is not None and msg2 is not None
        assert msg1.payload == "item-1"
        assert msg2.payload == "item-2"

    def test_pending_tracked_per_consumer(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue1: RedisQueue[str] = RedisQueue(
            clock, redis_client, stream_key, consumer_name="worker-1"
        )
        queue2: RedisQueue[str] = RedisQueue(
            clock, redis_client, stream_key, consumer_name="worker-2"
        )

        queue1.put("item-1")
        queue1.put("item-2")
        queue1.get()
        queue2.get()

        pending = redis_client.xpending_range(
            f"vec1:stream:{stream_key}",
            "vec1:consumers",
            min="-",
            max="+",
            count=10,
        )
        consumers = {entry["consumer"] for entry in pending}
        assert consumers == {b"worker-1", b"worker-2"}


class TestRedisQueueClaimStale:
    def test_claim_stale_returns_empty_when_no_pending(
        self, redis_client: Redis
    ) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(
            clock, redis_client, stream_key, stale_threshold_ms=0
        )

        stale = queue._claim_stale()

        assert stale == []

    def test_claim_stale_returns_empty_when_not_idle_enough(
        self, redis_client: Redis
    ) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(
            clock, redis_client, stream_key, stale_threshold_ms=60000
        )
        queue.put("test-item")
        queue.get()

        stale = queue._claim_stale()

        assert stale == []

    def test_claim_stale_claims_idle_messages(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue1: RedisQueue[str] = RedisQueue(
            clock, redis_client, stream_key, consumer_name="dead-worker"
        )
        queue2: RedisQueue[str] = RedisQueue(
            clock,
            redis_client,
            stream_key,
            consumer_name="live-worker",
            stale_threshold_ms=50,
        )

        queue1.put("orphaned-item")
        queue1.get()

        time.sleep(0.1)

        stale = queue2._claim_stale()

        assert len(stale) == 1
        assert stale[0].payload == "orphaned-item"

    def test_claim_stale_changes_ownership(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue1: RedisQueue[str] = RedisQueue(
            clock, redis_client, stream_key, consumer_name="dead-worker"
        )
        queue2: RedisQueue[str] = RedisQueue(
            clock,
            redis_client,
            stream_key,
            consumer_name="live-worker",
            stale_threshold_ms=50,
        )

        queue1.put("orphaned-item")
        queue1.get()

        time.sleep(0.1)

        queue2._claim_stale()

        pending = redis_client.xpending_range(
            f"vec1:stream:{stream_key}",
            "vec1:consumers",
            min="-",
            max="+",
            count=10,
        )
        assert len(pending) == 1
        assert pending[0]["consumer"] == b"live-worker"

    def test_claim_stale_increments_attempt_count(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue1: RedisQueue[str] = RedisQueue(
            clock, redis_client, stream_key, consumer_name="dead-worker"
        )
        queue2: RedisQueue[str] = RedisQueue(
            clock,
            redis_client,
            stream_key,
            consumer_name="live-worker",
            stale_threshold_ms=50,
        )

        queue1.put("orphaned-item")
        queue1.get()

        time.sleep(0.1)

        stale = queue2._claim_stale()

        assert len(stale) == 1
        assert stale[0].attempt_count == 1

    def test_get_claims_stale_before_new_messages(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue1: RedisQueue[str] = RedisQueue(
            clock, redis_client, stream_key, consumer_name="dead-worker"
        )
        queue2: RedisQueue[str] = RedisQueue(
            clock,
            redis_client,
            stream_key,
            consumer_name="live-worker",
            stale_threshold_ms=50,
        )

        queue1.put("orphaned-item")
        queue1.get()

        time.sleep(0.1)

        queue2.put("new-item")

        msg = queue2.get()

        assert msg is not None
        assert msg.payload == "orphaned-item"


class TestRedisQueueSize:
    def test_size_returns_zero_for_empty_stream(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)

        assert queue.size() == 0

    def test_size_returns_total_stream_length(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)

        queue.put("item-1")
        queue.put("item-2")
        queue.put("item-3")

        assert queue.size() == 3

    def test_size_includes_pending_messages(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)

        queue.put("item-1")
        queue.put("item-2")
        queue.get()

        assert queue.size() == 2

    def test_size_includes_acked_messages(self, redis_client: Redis) -> None:
        clock = FakeClock(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc))
        stream_key = f"test-{uuid4()}"
        queue: RedisQueue[str] = RedisQueue(clock, redis_client, stream_key)

        queue.put("item-1")
        msg = queue.get()
        assert msg is not None
        queue.ack(msg)

        assert queue.size() == 1
