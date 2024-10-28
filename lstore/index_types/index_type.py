"""
Abstract (blueprint) class for different types of indexes.
"""

from abc import ABC, abstractmethod

class IndexType(ABC):
    @abstractmethod
    def get(self, val):
        pass

    @abstractmethod
    def get_range(self, begin, end):
        pass

    @abstractmethod
    def insert(self, val, rid):
        pass

    @abstractmethod
    def delete(self, val):
        pass
