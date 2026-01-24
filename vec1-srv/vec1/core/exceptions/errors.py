from datetime import datetime

from vec1.core.schema.pr import PRIdentifier


class Vec1Error(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class SourceError(Vec1Error):
    pass


class SourceAuthenticationError(SourceError):
    pass


class SourceRateLimitError(SourceError):
    def __init__(self, message: str, retry_after: datetime) -> None:
        self.retry_after = retry_after
        super().__init__(message)


class SourceNotFoundError(SourceError):
    def __init__(self, message: str, resource: str) -> None:
        self.resource = resource
        super().__init__(message)


class PRFetchError(SourceError):
    def __init__(self, message: str, pr_identifier: PRIdentifier) -> None:
        self.pr_identifier = pr_identifier
        super().__init__(message)


class QueueError(Vec1Error):
    pass


class QueueFullError(QueueError):
    pass


class QueueTimeoutError(QueueError):
    pass


class ChunkerError(Vec1Error):
    pass


class ChunkingFailedError(ChunkerError):
    def __init__(self, message: str, pr_identifier: PRIdentifier) -> None:
        self.pr_identifier = pr_identifier
        super().__init__(message)


class ChunkTooLargeError(ChunkerError):
    def __init__(self, message: str, content_size: int, max_size: int) -> None:
        self.content_size = content_size
        self.max_size = max_size
        super().__init__(message)
