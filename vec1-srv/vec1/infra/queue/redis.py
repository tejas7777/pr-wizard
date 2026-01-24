import json
from datetime import datetime
from typing import Generic, TypeVar
from uuid import uuid4

from redis import Redis
from redis.exceptions import ResponseError

from vec1.core.ports.clock import Clock
from vec1.core.ports.queue import Queue
from vec1.core.schema.queue import QueueMessage


T = TypeVar("T")


class RedisQueue(Generic[T], Queue[T]):
    def __init__(
        self,
        clock: Clock,
        redis_client: Redis,
        stream_key: str,
        consumer_name: str | None = None,
        stale_threshold_ms: int = 60000,
    ) -> None:
        self._clock = clock
        self._redis = redis_client
        self._stream_key = stream_key
        self._consumer_name = consumer_name or str(uuid4())
        self._stale_threshold_ms = stale_threshold_ms
        self._init_consumer_group()

    @property
    def _stream(self) -> str:
        return f"vec1:stream:{self._stream_key}"

    @property
    def _group(self) -> str:
        return "vec1:consumers"

    def _init_consumer_group(self) -> None:
        try:
            self._redis.xgroup_create(self._stream, self._group, id="0", mkstream=True)
        except ResponseError as err:
            if "BUSYGROUP" not in str(err):
                raise

    def put(self, item: T) -> None:
        message = QueueMessage(
            payload=item,
            enqueued_at=self._clock.now(),
            attempt_count=0,
            message_id=str(uuid4()),
        )
        self._redis.xadd(self._stream, {"data": self._serialize(message)})

    def get(self) -> QueueMessage[T] | None:
        stale = self._claim_stale()
        if stale:
            return stale[0]
        result = self._redis.xreadgroup(
            self._group,
            self._consumer_name,
            {self._stream: ">"},
            count=1,
        )
        if not result:
            return None
        _, entries = result[0]
        stream_id, fields = entries[0]
        return self._to_inflight(stream_id, fields)

    def ack(self, message: QueueMessage[T]) -> None:
        if message.stream_id:
            self._redis.xack(self._stream, self._group, message.stream_id)

    def nack(self, message: QueueMessage[T], requeue: bool) -> None:
        if requeue:
            requeued = QueueMessage(
                payload=message.payload,
                enqueued_at=message.enqueued_at,
                attempt_count=message.attempt_count,
                message_id=message.message_id,
            )
            self._redis.xadd(self._stream, {"data": self._serialize(requeued)})

        if message.stream_id:
            self._redis.xack(self._stream, self._group, message.stream_id)

    def size(self) -> int:
        return self._redis.xlen(self._stream)

    def _claim_stale(self) -> list[QueueMessage[T]]:
        pending = self._redis.xpending_range(
            self._stream, self._group, min="-", max="+", count=100
        )
        if not pending:
            return []

        stale_ids = [
            entry["message_id"]
            for entry in pending
            if entry["time_since_delivered"] > self._stale_threshold_ms
        ]
        if not stale_ids:
            return []

        claimed = self._redis.xclaim(
            self._stream,
            self._group,
            self._consumer_name,
            min_idle_time=self._stale_threshold_ms,
            message_ids=stale_ids,
        )
        return [self._to_inflight(sid, fields) for sid, fields in claimed]

    def _to_inflight(
        self, stream_id: bytes, fields: dict[bytes, bytes]
    ) -> QueueMessage[T]:
        message = self._deserialize(fields[b"data"])
        return QueueMessage(
            payload=message.payload,
            enqueued_at=message.enqueued_at,
            attempt_count=message.attempt_count + 1,
            message_id=message.message_id,
            stream_id=stream_id.decode(),
        )

    def _serialize(self, message: QueueMessage[T]) -> str:
        return json.dumps(
            {
                "payload": message.payload,
                "enqueued_at": message.enqueued_at.isoformat(),
                "attempt_count": message.attempt_count,
                "message_id": message.message_id,
            }
        )

    def _deserialize(self, data: bytes) -> QueueMessage[T]:
        parsed = json.loads(data.decode())
        return QueueMessage(
            payload=parsed["payload"],
            enqueued_at=datetime.fromisoformat(parsed["enqueued_at"]),
            attempt_count=parsed["attempt_count"],
            message_id=parsed["message_id"],
        )
