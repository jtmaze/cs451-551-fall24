from storage.buffer import Buffer
from storage.buffer import Disk

class PageDirectory:
    """
    """
    def __init__(self, buffer_size) -> None:
        self.buffer = Buffer(buffer_size)

        self.disk = Disk() # TODO: Support persistent memory


    def get(self, rid, *columns):
        # Check buffer
        output = self.buffer.get(rid, columns)
        if output:
            return output
        
        # Check disk
        output = self.disk.get(rid, columns)
        if output:
            self.buffer.update(rid, output)
            return output
        
        raise KeyError(f"RID <{rid}> not found in buffer or persistent memory")
