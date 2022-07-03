from enum import Enum

class TaskType(Enum):
    """
    Enum for task type
    """
    MAP = 0
    REDUCE = 2
    UNDEFINED = -1

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name