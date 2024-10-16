import heapq
from itertools import count

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

    def get(self, rid, *columns):
        # Check buffer
        # TODO: update staleness queue?
        if rid in self.records:
            return self.records[rid]

        return None

    def update(self, rid, output):
        self.records[rid] = output

        if self.max_items is not None:
            self._update_heap(rid)

    # Helpers --------------------------------------

    def _update_heap(self, rid):
        """Adds element to staleness heap and removes stalest if needed."""
        pair = (next(self.heap_counter), rid)

        if len(self.records) >= self.max_items:
            # Heap push + pop but slightly faster
            stale_rid = heapq.heapreplace(self.staleness_heap, pair)
            del self.records[stale_rid[1]]
        else:
            heapq.heappush(self.staleness_heap, pair)
