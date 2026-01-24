from datetime import datetime, timezone
from typing import Generic, Optional, TypeVar

from vec1.core.exceptions import QueueFullError
from vec1.core.ports.clock import Clock
from vec1.core.ports.queue import Queue
from vec1.core.schema.queue import QueueMessage

T = TypeVar("T")


class FakeQueue(Generic[T], Queue[T]):
    def __init__(
        self,
        clock: Clock | None = None,
        max_size: Optional[int] = None,
    ) -> None:
        self._clock = clock
        self._max_size = max_size
        self._items: list[QueueMessage[T]] = []
        self._pending: dict[str, QueueMessage[T]] = {}

    def put(self, item: T) -> None:
        if self._max_size is not None and len(self._items) >= self._max_size:
            raise QueueFullError("Queue is full")
        enqueued_at = self._now()
        message = QueueMessage(
            payload=item,
            enqueued_at=enqueued_at,
            attempt_count=0,
            message_id=self._new_message_id(),
        )
        self._items.append(message)

    def get(self, timeout: Optional[float] = None) -> Optional[QueueMessage[T]]:  # noqa: ARG002
        if not self._items:
            return None
        message = self._items.pop(0)
        incremented = QueueMessage(
            payload=message.payload,
            enqueued_at=message.enqueued_at,
            attempt_count=message.attempt_count + 1,
            message_id=message.message_id,
        )
        self._pending[message.message_id] = incremented
        return incremented

    def ack(self, message: QueueMessage[T]) -> None:  # noqa: ARG002
        self._pending.pop(message.message_id, None)

    def nack(self, message: QueueMessage[T], requeue: bool) -> None:
        self._pending.pop(message.message_id, None)
        if requeue:
            self._items.append(message)

    def size(self) -> int:
        return len(self._items)

    def _now(self) -> datetime:
        if self._clock:
            return self._clock.now()
        return datetime.now(timezone.utc)

    def _new_message_id(self) -> str:
        return f"fake-{len(self._items) + len(self._pending)}"
