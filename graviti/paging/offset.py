#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Paging list offset related class."""

from bisect import bisect_right
from itertools import accumulate, chain
from typing import Iterable, List, Tuple, TypeVar

_O = TypeVar("_O", bound="Offsets")


class Offsets:
    """The offsets manager of the paging list.

    Arguments:
        total_count: The total count of the elements in the paging list.
        limit: The size of each page.

    """

    _offsets: List[int]

    def __init__(self, total_count: int, limit: int) -> None:
        self.total_count = total_count
        self._limit = limit

    def _get_offsets(self) -> List[int]:
        if not hasattr(self, "_offsets"):
            self._offsets = (
                list(range(0, self.total_count, self._limit)) if self._limit != 0 else []
            )

        return self._offsets

    def update(self, start: int, stop: int, lengths: Iterable[int]) -> None:
        """Update the offsets when setting or deleting paging list items.

        Arguments:
            start: The start index.
            stop: The stop index.
            lengths: The length of the set values.

        """
        offsets = self._get_offsets()
        partial_offsets = list(accumulate(chain((offsets[start],), lengths)))
        try:
            last_offset = offsets[stop + 1]
        except IndexError:
            last_offset = self.total_count

        diff = partial_offsets.pop() - last_offset
        if diff != 0:
            self.total_count += diff
            for i in range(start + 1, len(offsets)):
                offsets[i] += diff

        offsets[start : stop + 1] = partial_offsets

    def get_coordinate(self, index: int) -> Tuple[int, int]:
        """Get the page coordinate of the elements.

        Arguments:
            index: The index of the element in paging list.

        Returns:
            The page number and the index of the page.

        """
        if not hasattr(self, "_offsets"):
            try:
                return divmod(index, self._limit)
            except ZeroDivisionError:
                return 0, index

        i = bisect_right(self._offsets, index) - 1
        return i, index - self._offsets[i]

    def extend(self, lengths: Iterable[int]) -> None:
        """Update the offsets when extending the paging list.

        Arguments:
            lengths: The lengths of the extended pages.

        """
        offsets = self._get_offsets()
        offsets.extend(accumulate(chain((self.total_count,), lengths)))
        self.total_count = offsets.pop()

    def copy(self: _O) -> _O:
        """Return a copy of the Offsets.

        Returns:
            A copy of the Offsets.

        """
        obj = self.__class__(self.total_count, self._limit)
        if hasattr(self, "_offsets"):
            obj._offsets = self._offsets.copy()  # pylint: disable=protected-access

        return obj
