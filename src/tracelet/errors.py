class TraceletError(Exception):
    """Base exception for Tracelet errors."""


class ValidationError(TraceletError, ValueError):
    """An event, result, or evaluator result is invalid."""


class StorageError(TraceletError):
    """A persistence operation failed."""


class WorkerClosedError(TraceletError, RuntimeError):
    """The worker is stopped and cannot accept new work."""
