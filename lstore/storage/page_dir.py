from typing import Literal

from table import Record

from storage.buffer import Buffer
from storage.disk import Disk

from rid import RID

class PageDirectory:
    """
    """
    def __init__(self, num_columns: int, buffer_size: int) -> None:
        self.num_columns = num_columns

        self.buffer = Buffer(self, buffer_size)

        self.disk = Disk(self) # TODO: Support persistent memory


    def get(self, rid: RID, cols: list[Literal[0, 1]]) -> list[Record]:
        """Returns the projected data given an RID.

        Like Query.select, cols refers to which data columns to return (0=False, 1=True)
        """
        # Check buffer
        output = self.buffer.check(rid, cols)
        if output:
            return output
        
        # Check disk
        output = self.disk.retrieve(rid, cols)
        if output:
            self.buffer.insert(rid, output)
            return output
        
        raise KeyError(f"RID <{rid}> not found in buffer or persistent memory")
