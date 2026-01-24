from dataclasses import replace
from queue import Empty, Full, Queue as ThreadQueue
from typing import Generic, Optional, TypeVar
from uuid import uuid4

from vec1.core.exceptions import QueueFullError
from vec1.core.ports.clock import Clock
from vec1.core.ports.queue import Queue
from vec1.core.schema.queue import QueueMessage

T = TypeVar('T')


class MemoryQueue(Generic[T], Queue[T]):
    def __init__(self, clock: Clock, max_size: Optional[int] = None) -> None:
        self._clock = clock
        self._queue: ThreadQueue[QueueMessage[T]] = ThreadQueue(
            maxsize=max_size or 0
        )
        self._pending: dict[str, QueueMessage[T]] = {}

    def put(self, item: T) -> None:
        message = QueueMessage(
            payload=item,
            enqueued_at=self._clock.now(),
            attempt_count=0,
            message_id=str(uuid4()),
        )
        try:
            self._queue.put_nowait(message)
        except Full as error:
            raise QueueFullError('Queue is full') from error

    def get(
        self, timeout: Optional[float] = None
    ) -> Optional[QueueMessage[T]]:
        try:
            message = self._queue.get(timeout=timeout)
        except Empty:
            return None
        inflight = replace(message, attempt_count=message.attempt_count + 1)
        self._pending[inflight.message_id] = inflight
        return inflight

    def ack(self, message: QueueMessage[T]) -> None:
        self._pending.pop(message.message_id, None)

    def nack(self, message: QueueMessage[T], requeue: bool) -> None:
        self._pending.pop(message.message_id, None)
        if requeue:
            self._queue.put_nowait(message)

    def size(self) -> int:
        return self._queue.qsize()
