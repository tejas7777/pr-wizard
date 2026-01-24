from typing import Generic, Protocol, TypeVar, runtime_checkable

from vec1.core.schema.queue import QueueMessage

T = TypeVar("T")


@runtime_checkable
class Queue(Generic[T], Protocol):
    def put(self, item: T) -> None: ...

    def get(self) -> QueueMessage[T] | None: ...

    def ack(self, message: QueueMessage[T]) -> None: ...

    def nack(self, message: QueueMessage[T], requeue: bool) -> None: ...

    def size(self) -> int: ...
