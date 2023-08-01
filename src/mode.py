from enum import Enum

class Mode(Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    MERGE = "merge"

    def __str__(self):
        return self.value