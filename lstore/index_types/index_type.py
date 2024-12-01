"""
Abstract (blueprint) class for different types of indexes.
"""

from abc import ABC, abstractmethod

class IndexType(ABC):
    @abstractmethod
    def get(self, val):
        raise NotImplementedError()

    @abstractmethod
    def get_range_key(self, begin, end):
        raise NotImplementedError()

    @abstractmethod
    def get_range_val(self, begin, end):
        raise NotImplementedError()

    @abstractmethod
    def insert(self, val, rid):
        raise NotImplementedError()

    @abstractmethod
    def delete(self, val, rid):
        raise NotImplementedError()

    @abstractmethod
    def clear(self):
        raise NotImplementedError()
