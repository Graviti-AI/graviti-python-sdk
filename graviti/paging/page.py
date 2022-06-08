#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Page related class."""

from typing import Any, Callable, Iterator, Optional, Union, overload

import pyarrow as pa


class Page:
    """Page is an array wrapper and represents a page in paging list.

    Arguments:
        array: The pyarrow array.

    """

    def __init__(self, array: pa.Array):
        self._ranging = range(len(array))
        self._patch(array)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._ranging})"

    def __len__(self) -> int:
        return len(self._ranging)

    def __iter__(self) -> Iterator[Any]:
        return self._iter()

    @overload
    def __getitem__(self, index: int) -> Any:
        ...

    @overload
    def __getitem__(self, index: slice) -> "Page":
        ...

    def __getitem__(self, index: Union[int, slice]) -> Union[Any, "Page"]:
        if isinstance(index, slice):
            return self.get_slice(index.start, index.stop, index.step)

        return self.get_item(index)

    def _patch(self, array: pa.Array) -> None:
        self._array = array
        # https://github.com/python/mypy/issues/708
        # https://github.com/python/mypy/issues/2427
        self._iter = array.__iter__  # type: ignore[assignment]
        self.get_item = array.__getitem__  # type: ignore[assignment]

    def _iter(self) -> Iterator[Any]:  # pylint: disable=method-hidden
        return self.get_array().__iter__()  # type: ignore[no-any-return]

    def get_item(self, index: int) -> Any:  # pylint: disable=method-hidden
        """Return the item at the given index.

        Arguments:
            index: Position of the mutable sequence.

        Returns:
            The item at the given index.

        """
        return self.get_array().__getitem__(index)

    def get_slice(
        self, start: Optional[int] = None, stop: Optional[int] = None, step: Optional[int] = None
    ) -> "Page":
        """Return a sliced page according to the given start and stop index.

        Arguments:
            start: The start index.
            stop: The stop index.
            step: The slice step.

        Returns:
            A sliced page according to the given start and stop index.

        """
        return SlicedPage(self._ranging[start:stop:step], self._array)

    def get_array(self) -> pa.array:
        """Get the array inside the page.

        Returns:
            The array inside the page.

        """
        return self._array


class SlicedPage(Page):
    """SlicedPage is an array wrapper and represents a sliced page in paging list.

    Arguments:
        ranging: The range instance of this page.
        array: The pyarrow array.

    """

    def __init__(  # pylint: disable=super-init-not-called
        self, ranging: range, array: pa.Array
    ) -> None:
        self._ranging = ranging
        self._array = None
        self._source_array = array

    def get_array(self) -> pa.array:
        """Get the array inside the page.

        Returns:
            The array inside the page.

        """
        if self._array is None:
            ranging = self._ranging
            array = self._source_array[ranging.start : ranging.stop : ranging.step]
            self._patch(array)

        return self._array


class LazyPage(Page):
    """LazyPage is a placeholder when the paging list page is not loaded yet.

    Arguments:
        pos: The page number.
        ranging: The range instance of this page.
        parent: The parent paging list.

    """

    def __init__(  # pylint: disable=super-init-not-called
        self, ranging: range, pos: int, array_getter: Callable[[int], pa.Array]
    ) -> None:
        self._ranging = ranging
        self._pos = pos
        self._array_getter = array_getter
        self._array = None

    def get_slice(
        self, start: Optional[int] = None, stop: Optional[int] = None, step: Optional[int] = None
    ) -> "LazySlicedPage":
        """Return a lazy sliced page according to the given start and stop index.

        Arguments:
            start: The start index.
            stop: The stop index.
            step: The slice step.

        Returns:
            A sliced page according to the given start and stop index.

        """
        return LazySlicedPage(self._ranging[start:stop:step], self._pos, self._array_getter)

    def get_array(self) -> pa.Array:
        """Get the array inside the page.

        Returns:
            The array inside the page.

        """
        if self._array is None:
            array = self._array_getter(self._pos)
            self._patch(array)

        return self._array


class LazySlicedPage(LazyPage):
    """LazySlicedPage is a placeholder when the sliced paging list page is not loaded yet."""

    def get_array(self) -> pa.Array:
        """Get the array inside the page.

        Returns:
            The array inside the page.

        """
        if self._array is None:
            ranging = self._ranging
            array = self._array_getter(self._pos)[ranging.start : ranging.stop : ranging.step]

            self._patch(array)

        return self._array
