class QueueError(Exception):
    """Base class for queue-related errors"""


class Empty(QueueError):
    """Raised when get_nowait is called on an empty queue"""


class Full(QueueError):
    """Raised when put_nowait is called on an empty queue"""
