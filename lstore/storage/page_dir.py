from storage.buffer import Buffer
from storage.disk import Disk

from rid import RID

class PageDirectory:
    """
    """
    def __init__(self, buffer_size) -> None:
        self.buffer = Buffer(buffer_size)

        self.disk = Disk() # TODO: Support persistent memory


    def get(self, rid: RID, *columns):
        # Check buffer
        output = self.buffer.check(rid, columns)
        if output:
            return output
        
        # Check disk
        output = self.disk.retrieve(rid, columns)
        if output:
            self.buffer.update(rid, output)
            return output
        
        raise KeyError(f"RID <{rid}> not found in buffer or persistent memory")
