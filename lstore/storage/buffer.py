import heapq
from itertools import count

from rid import RID


class Buffer:
    """
    """

    def __init__(self, max_items: int | None) -> None:
        self.max_items = max_items

        self.records = dict()

        # Heap indicating RID staleness (what to replace first)
        # Only used to limit capacity if max_items is specified
        self.staleness_heap = []
        self.heap_counter = count()

    def check(self, rid: RID, *columns):
        """Checks if RID in records and returns result if true.

        May want to move directly to page directory to minimize function call
        overhead.
        """
        # TODO: update staleness queue?
        if rid in self.records:
            return self.records[rid]

        return None

    def update(self, rid: RID, output):
        self.records[rid] = output

        if self.max_items is not None:
            self._update_heap(rid)

    # Helpers --------------------------------------

    def _update_heap(self, rid: RID):
        """Adds element to staleness heap and removes stalest if needed."""
        # Tuple w/ order of insertion and RID, heap will sort on first but have both
        staleness_pair = (next(self.heap_counter), rid)

        if len(self.records) >= self.max_items:
            # Heap push + pop but slightly faster
            stale_rid = heapq.heapreplace(self.staleness_heap, staleness_pair)

            # Lazily free space in hash map
            del self.records[stale_rid[1]]
        else:
            heapq.heappush(self.staleness_heap, staleness_pair)
