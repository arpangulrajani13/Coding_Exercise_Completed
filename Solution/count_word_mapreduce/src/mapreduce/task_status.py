from enum import Enum

class TaskStatus(Enum):
    """
    Enum for task status
    """
    IN_PROGRESS = 1
    NOT_STARTED = 0
    COMPLETED = 2
    FAILED = 4
    CANCELLED = 5
    UNDEFINED = -1

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name