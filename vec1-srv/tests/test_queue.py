from datetime import datetime, timezone

import pytest

from vec1.core.exceptions import QueueFullError
from vec1.infra.queue.memory import MemoryQueue
from tests.fakes import FakeClock, FakeQueue


class TestMemoryQueuePut:
    def test_put_adds_item_to_queue(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: MemoryQueue[str] = MemoryQueue(clock)

        queue.put("test-item")

        assert queue.size() == 1

    def test_put_multiple_items(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: MemoryQueue[str] = MemoryQueue(clock)

        queue.put("item-1")
        queue.put("item-2")
        queue.put("item-3")

        assert queue.size() == 3

    def test_put_raises_when_full(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: MemoryQueue[str] = MemoryQueue(clock, max_size=2)

        queue.put("item-1")
        queue.put("item-2")

        with pytest.raises(QueueFullError):
            queue.put("item-3")


class TestMemoryQueueGet:
    def test_get_returns_item_with_message_wrapper(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: MemoryQueue[str] = MemoryQueue(clock)
        queue.put("test-item")

        message = queue.get()

        assert message is not None
        assert message.payload == "test-item"
        assert message.attempt_count == 1

    def test_get_returns_none_when_empty(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: MemoryQueue[str] = MemoryQueue(clock, timeout=0.01)

        message = queue.get()

        assert message is None

    def test_get_returns_items_in_fifo_order(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: MemoryQueue[str] = MemoryQueue(clock)
        queue.put("first")
        queue.put("second")
        queue.put("third")

        msg1 = queue.get()
        msg2 = queue.get()
        msg3 = queue.get()

        assert msg1 is not None and msg1.payload == "first"
        assert msg2 is not None and msg2.payload == "second"
        assert msg3 is not None and msg3.payload == "third"

    def test_get_increments_attempt_count(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: MemoryQueue[str] = MemoryQueue(clock)
        queue.put("test-item")

        message = queue.get()

        assert message is not None
        assert message.attempt_count == 1


class TestMemoryQueueAck:
    def test_ack_removes_message_from_pending(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: MemoryQueue[str] = MemoryQueue(clock)
        queue.put("test-item")
        message = queue.get()

        assert message is not None
        queue.ack(message)

        assert queue.size() == 0


class TestMemoryQueueNack:
    def test_nack_with_requeue_puts_item_back(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: MemoryQueue[str] = MemoryQueue(clock)
        queue.put("test-item")
        message = queue.get()

        assert message is not None
        queue.nack(message, requeue=True)

        assert queue.size() == 1
        requeued = queue.get()
        assert requeued is not None
        assert requeued.payload == "test-item"

    def test_nack_without_requeue_discards_item(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: MemoryQueue[str] = MemoryQueue(clock)
        queue.put("test-item")
        message = queue.get()

        assert message is not None
        queue.nack(message, requeue=False)

        assert queue.size() == 0


class TestMemoryQueueMessageMetadata:
    def test_message_has_enqueued_timestamp(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: MemoryQueue[str] = MemoryQueue(clock)
        queue.put("test-item")

        message = queue.get()

        assert message is not None
        assert message.enqueued_at == now

    def test_message_has_unique_id(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: MemoryQueue[str] = MemoryQueue(clock)
        queue.put("item-1")
        queue.put("item-2")

        msg1 = queue.get()
        msg2 = queue.get()

        assert msg1 is not None and msg2 is not None
        assert msg1.message_id != msg2.message_id


class TestFakeQueuePut:
    def test_put_adds_item_to_queue(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: FakeQueue[str] = FakeQueue(clock)

        queue.put("test-item")

        assert queue.size() == 1

    def test_put_raises_when_full(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: FakeQueue[str] = FakeQueue(clock, max_size=1)

        queue.put("item-1")

        with pytest.raises(QueueFullError):
            queue.put("item-2")


class TestFakeQueueGet:
    def test_get_returns_item_with_message_wrapper(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: FakeQueue[str] = FakeQueue(clock)
        queue.put("test-item")

        message = queue.get()

        assert message is not None
        assert message.payload == "test-item"

    def test_get_returns_none_when_empty(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: FakeQueue[str] = FakeQueue(clock)

        message = queue.get()

        assert message is None

    def test_get_returns_items_in_fifo_order(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: FakeQueue[str] = FakeQueue(clock)
        queue.put("first")
        queue.put("second")

        msg1 = queue.get()
        msg2 = queue.get()

        assert msg1 is not None and msg1.payload == "first"
        assert msg2 is not None and msg2.payload == "second"


class TestFakeQueueAckNack:
    def test_ack_removes_from_pending(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: FakeQueue[str] = FakeQueue(clock)
        queue.put("test-item")
        message = queue.get()

        assert message is not None
        queue.ack(message)

        assert queue.size() == 0

    def test_nack_with_requeue(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: FakeQueue[str] = FakeQueue(clock)
        queue.put("test-item")
        message = queue.get()

        assert message is not None
        queue.nack(message, requeue=True)

        assert queue.size() == 1

    def test_nack_without_requeue(self) -> None:
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = FakeClock(now)
        queue: FakeQueue[str] = FakeQueue(clock)
        queue.put("test-item")
        message = queue.get()

        assert message is not None
        queue.nack(message, requeue=False)

        assert queue.size() == 0
