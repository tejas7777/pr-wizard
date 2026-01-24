from dataclasses import asdict
from typing import Sequence

from vec1.core.chunkers import BaseChunker
from vec1.core.exceptions import ChunkerError, Vec1Error
from vec1.core.jobs.base import BaseJob
from vec1.core.ports.clock import Clock
from vec1.core.ports.logger import Logger
from vec1.core.ports.queue import Queue
from vec1.core.schema.chunk import Chunk
from vec1.core.schema.pr import FetchedPR, PRIdentifier
from vec1.core.schema.queue import QueueMessage

MAX_CHUNK_ATTEMPTS = 3


class ChunkingJob(BaseJob):
    def __init__(
        self,
        logger: Logger,
        clock: Clock,
        input_queue: Queue[FetchedPR],
        output_queue: Queue[Chunk],
        chunkers: Sequence[BaseChunker],
        *,
        get_timeout: float,
    ) -> None:
        super().__init__(logger, poll_interval=0, clock=clock)
        self._input_queue = input_queue
        self._output_queue = output_queue
        self._chunkers = list(chunkers)
        self._get_timeout = get_timeout

    def setup(self) -> None:
        if not self._chunkers:
            raise ChunkerError('No chunkers configured')

    def execute_once(self) -> None:
        message = self._input_queue.get(timeout=self._get_timeout)
        if message is None:
            return

        fetched_pr = message.payload
        pr_identifier = fetched_pr.metadata.identifier
        chunk_count = 0

        try:
            for chunker in self._chunkers:
                for chunk in chunker.chunk(fetched_pr):
                    self._output_queue.put(chunk)
                    chunk_count += 1
            self._input_queue.ack(message)
            self._logger.info(
                'Chunked pull request',
                pr_identifier=asdict(pr_identifier),
                chunk_count=chunk_count,
            )
        except Vec1Error as error:
            self._handle_chunk_failure(message, pr_identifier, error)
            raise

    def teardown(self) -> None:
        return

    def _handle_chunk_failure(
        self,
        message: QueueMessage[FetchedPR],
        pr_identifier: PRIdentifier,
        error: Vec1Error,
    ) -> None:
        requeue = message.attempt_count < MAX_CHUNK_ATTEMPTS
        self._input_queue.nack(message, requeue=requeue)
        self._logger.exception(
            'Chunking failed',
            pr_identifier=asdict(pr_identifier),
            attempt_count=message.attempt_count,
            requeue=requeue,
            error=str(error),
        )
