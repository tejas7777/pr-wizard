from dataclasses import dataclass
from datetime import datetime
from typing import Generic, TypeVar


T = TypeVar('T')


@dataclass(frozen=True, slots=True)
class QueueMessage(Generic[T]):
    payload: T
    enqueued_at: datetime
    attempt_count: int
    message_id: str
