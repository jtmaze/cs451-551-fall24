import heapq
from itertools import count

from storage.rid import RID
from page import Page


class Buffer:
    """Where pages live once in memory."""

    def __init__(self, max_items: int | None) -> None:
        self.max_items = max_items

        # Hash table for mapping RID.page_id to pages in memory
        self.pages: dict[int, Page] = dict()

        # Heap indicating RID staleness (what to replace first)
        # Only used to limit capacity if max_items is specified
        self.staleness_heap = []
        self.heap_counter = count()

    def check(self, rid: RID, cols):
        """Checks if RID in records and returns result if true.

        May want to move directly to page directory to minimize function call
        overhead.
        """
        # TODO: update staleness queue?
        page = self.pages.get(rid.page, None)

        return page

    def insert(self, rid: RID, output):
        self.pages[rid] = output

        if self.max_items is not None:
            self._update_heap(rid)

    # Helpers --------------------------------------

    def _update_heap(self, rid: RID):
        """Adds element to staleness heap and removes stalest if needed."""
        # Tuple w/ order of insertion and RID, heap will sort on first but have both
        staleness_pair = (next(self.heap_counter), rid)

        if len(self.pages) >= self.max_items:
            # Heap push + pop but slightly faster
            stale_rid = heapq.heapreplace(self.staleness_heap, staleness_pair)

            # Lazily free space in hash map
            del self.pages[stale_rid[1]]
        else:
            heapq.heappush(self.staleness_heap, staleness_pair)
