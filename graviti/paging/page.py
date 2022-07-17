#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Page related class."""

from typing import Any, Callable, Iterator, Optional, Sequence, TypeVar, Union, overload

_T = TypeVar("_T")


class PageBase(Sequence[_T]):
    """PageBase is the base class of array wrapper and represents a page in paging list."""

    _ranging: range

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._ranging})"

    def __len__(self) -> int:
        return len(self._ranging)

    def __iter__(self) -> Iterator[_T]:
        return self._iter()

    @overload
    def __getitem__(self, index: int) -> _T:
        ...

    @overload
    def __getitem__(self, index: slice) -> "PageBase[_T]":
        ...

    def __getitem__(self, index: Union[int, slice]) -> Union[_T, "PageBase[_T]"]:
        if isinstance(index, slice):
            return self.get_slice(index.start, index.stop, index.step)

        return self.get_item(index)

    def _patch(self, array: Sequence[_T]) -> None:
        # https://github.com/python/mypy/issues/708
        # https://github.com/python/mypy/issues/2427
        self._iter = array.__iter__  # type: ignore[assignment]
        self.get_item = array.__getitem__  # type: ignore[assignment]

    def _iter(self) -> Iterator[_T]:  # pylint: disable=method-hidden
        return self.get_array().__iter__()

    def get_item(self, index: int) -> _T:  # pylint: disable=method-hidden
        """Return the item at the given index.

        Arguments:
            index: Position of the mutable sequence.

        Returns:
            The item at the given index.

        """
        return self.get_array().__getitem__(index)

    def get_slice(
        self, start: Optional[int] = None, stop: Optional[int] = None, step: Optional[int] = None
    ) -> "PageBase[_T]":
        """Return a sliced page according to the given start and stop index.

        Arguments:
            start: The start index.
            stop: The stop index.
            step: The slice step.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError

    def get_array(self) -> Sequence[_T]:
        """Get the array inside the page.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError


class Page(PageBase[_T]):
    """Page is an array wrapper and represents a page in paging list.

    Arguments:
        array: The internal sequence of page.

    """

    def __init__(self, array: Sequence[_T]):
        self._ranging = range(len(array))
        self._array = array
        self._patch(array)

    def get_slice(
        self, start: Optional[int] = None, stop: Optional[int] = None, step: Optional[int] = None
    ) -> "LazySlicedPage[_T]":
        """Return a sliced page according to the given start and stop index.

        Arguments:
            start: The start index.
            stop: The stop index.
            step: The slice step.

        Returns:
            A sliced page according to the given start and stop index.

        """
        return LazySlicedPage(self._ranging[start:stop:step], self.get_array)

    def get_array(self) -> Sequence[_T]:
        """Get the array inside the page.

        Returns:
            The array inside the page.

        """
        return self._array


class LazyPage(PageBase[_T]):
    """LazyPage is a placeholder when the paging list page is not loaded yet.

    Arguments:
        ranging: The range instance of this page.
        array_getter: A callable object to get the source array.

    """

    _array: Optional[Sequence[_T]] = None

    def __init__(self, ranging: range, array_getter: Callable[[], Sequence[_T]]) -> None:
        self._ranging = ranging
        self._array_getter = array_getter

    def get_slice(
        self, start: Optional[int] = None, stop: Optional[int] = None, step: Optional[int] = None
    ) -> "LazySlicedPage[_T]":
        """Return a lazy sliced page according to the given start and stop index.

        Arguments:
            start: The start index.
            stop: The stop index.
            step: The slice step.

        Returns:
            A sliced page according to the given start and stop index.

        """
        return LazySlicedPage(self._ranging[start:stop:step], self._array_getter)

    def get_array(self) -> Sequence[_T]:
        """Get the array inside the page.

        Returns:
            The array inside the page.

        """
        array = self._array
        if array is None:
            array = self._array_getter()
            self._array = array
            self._patch(array)

        return array


class LazySlicedPage(LazyPage[_T]):
    """LazySlicedPage is a placeholder when the sliced paging list page is not loaded yet."""

    def get_array(self) -> Sequence[_T]:
        """Get the array inside the page.

        Returns:
            The array inside the page.

        """
        array = self._array
        if array is None:
            ranging = self._ranging
            array = self._array_getter()[ranging.start : ranging.stop : ranging.step]

            self._array = array
            self._patch(array)

        return array


class MappedLazyPage(PageBase[_T]):
    """LazyPage with a mapper for converting every item in the source array.

    Arguments:
        ranging: The range instance of this page.
        array_getter: A callable object to get the source array.
        mapper: A callable object to convert every item in the source array.

    """

    _array: Optional[Sequence[_T]] = None

    def __init__(
        self,
        ranging: range,
        array_getter: Callable[[], Sequence[_T]],
        mapper: Callable[[Any], Any],
    ) -> None:
        self._ranging = ranging
        self._array_getter = array_getter
        self._mapper = mapper

    def get_slice(
        self, start: Optional[int] = None, stop: Optional[int] = None, step: Optional[int] = None
    ) -> "Union[MappedLazySlicedPage[_T], LazySlicedPage[_T]]":
        """Return a lazy sliced page according to the given start and stop index.

        Arguments:
            start: The start index.
            stop: The stop index.
            step: The slice step.

        Returns:
            A sliced page according to the given start and stop index.

        """
        if self._array is not None:
            return LazySlicedPage(self._ranging[start:stop:step], self.get_array)

        return MappedLazySlicedPage(
            self._ranging[start:stop:step], self._array_getter, self._mapper
        )

    def get_array(self) -> Sequence[_T]:
        """Get the array inside the page.

        Returns:
            The array inside the page.

        """
        array = self._array
        if array is None:
            array = self._array_getter()
            array = tuple(map(self._mapper, array))
            self._array = array
            self._patch(array)

        return array


class MappedLazySlicedPage(MappedLazyPage[_T]):
    """LazySlicedPage with a mapper for converting every item in the source array."""

    def get_array(self) -> Sequence[_T]:
        """Get the array inside the page.

        Returns:
            The array inside the page.

        """
        array = self._array
        if array is None:
            ranging = self._ranging
            array = self._array_getter()[ranging.start : ranging.stop : ranging.step]
            array = tuple(map(self._mapper, array))

            self._array = array
            self._patch(array)

        return array
