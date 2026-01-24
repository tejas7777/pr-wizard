import hashlib
import threading
from typing import Callable, Sequence

from vec1.config import Settings, load_settings
from vec1.core.chunkers import DescriptionChunker, DiffChunker
from vec1.core.jobs import ChunkingJob, PollingJob
from vec1.core.schema.chunk import Chunk
from vec1.core.schema.pr import FetchedPR
from vec1.infra import (
    ConsoleLogger,
    PollingStateStore,
    FileStateStore,
    GitHubClient,
    GitHubPRSource,
    LogfireLogger,
    MemoryQueue,
    SystemClock,
    configure_logfire,
)


def _hash_content(content: str) -> str:
    return hashlib.sha256(content.encode('utf-8')).hexdigest()


def _count_tokens(content: str) -> int:
    return len(content.split())


def main() -> None:
    settings = load_settings()
    logger = _build_logger(settings)
    clock = SystemClock()

    fetched_pr_queue = MemoryQueue[FetchedPR](
        clock,
        max_size=settings.queue.max_size,
    )
    chunk_queue = MemoryQueue[Chunk](clock, max_size=settings.queue.max_size)
    state_store = FileStateStore(settings.state.base_dir)
    polling_state = PollingStateStore(state_store)

    hasher: Callable[[str], str] = _hash_content
    token_counter: Callable[[str], int] = _count_tokens
    chunkers: Sequence = (
        DescriptionChunker(hasher=hasher, token_counter=token_counter),
        DiffChunker(
            hasher=hasher,
            token_counter=token_counter,
            max_tokens=settings.chunker.diff_max_tokens,
        ),
    )

    with GitHubClient(settings.github.token) as github_client:
        pr_source = GitHubPRSource(
            github_client,
            settings.github.owner,
            settings.github.repo,
        )
        polling_job = PollingJob(
            logger=logger,
            poll_interval=settings.jobs.poll_interval,
            pr_source=pr_source,
            output_queue=fetched_pr_queue,
            polling_state=polling_state,
            clock=clock,
            default_lookback_hours=settings.jobs.default_lookback_hours,
        )
        chunking_job = ChunkingJob(
            logger=logger,
            clock=clock,
            input_queue=fetched_pr_queue,
            output_queue=chunk_queue,
            chunkers=chunkers,
            get_timeout=settings.jobs.chunk_get_timeout,
        )
        _run_jobs(logger, polling_job, chunking_job)


def _build_logger(settings: Settings):
    if settings.logging.backend == 'console':
        return ConsoleLogger(settings.logging.name)
    if settings.logging.backend == 'logfire':
        if not settings.logging.logfire_token:
            raise ValueError(
                'Logfire backend selected but VEC1_LOGFIRE_TOKEN is not set'
            )
        configure_logfire(settings.logging.logfire_token)
        return LogfireLogger(settings.logging.name)
    raise ValueError(f'Unknown logging backend {settings.logging.backend}')


def _run_jobs(
    logger,
    polling_job: PollingJob,
    chunking_job: ChunkingJob,
) -> None:
    chunk_thread = threading.Thread(
        target=chunking_job.run,
        name='ChunkingJob',
        daemon=True,
    )
    chunk_thread.start()
    try:
        polling_job.run()
    except KeyboardInterrupt:
        logger.info('Shutdown requested')
    finally:
        polling_job.stop()
        chunking_job.stop()
        chunk_thread.join(timeout=5)
