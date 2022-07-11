#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Paging list related class."""

from functools import partial
from itertools import chain, repeat
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

import pyarrow as pa

from graviti.paging.offset import Offsets
from graviti.paging.page import LazyPage, MappedLazyPage, Page, PageBase

if TYPE_CHECKING:
    from graviti.paging.factory import LazyFactory

_PLB = TypeVar("_PLB", bound="PagingListBase")
_PL = TypeVar("_PL", bound="PagingList")
_PPL = TypeVar("_PPL", bound="PyArrowPagingList")


class PagingListBase:
    """PagingListBase is the base class of the paging list related classes.

    Arguments:
        array: The input sequence.

    """

    _array_creator = tuple
    _pages: List[PageBase[Any]]
    _offsets: Offsets

    def __init__(self, iterable: Iterable[Any]) -> None:
        array = self._array_creator(iterable)
        self._init(array)

    def __len__(self) -> int:
        return self._offsets.total_count

    def __iter__(self) -> Iterator[Any]:
        return chain(*self._pages)

    @overload
    def __setitem__(self, index: int, value: Any) -> None:
        ...

    @overload
    def __setitem__(self: _PLB, index: slice, value: Union[Iterable[Any], _PLB]) -> None:
        ...

    def __setitem__(
        self: _PLB, index: Union[int, slice], value: Union[Any, Iterable[Any], _PLB]
    ) -> None:
        if isinstance(index, int):
            self.set_item(index, value)
        elif isinstance(value, self.__class__):
            self.set_slice(index, value)
        else:
            self.set_slice_iterable(index, value)

    def __delitem__(self, index: Union[int, slice]) -> None:
        if isinstance(index, slice):
            start, stop, step = index.indices(self.__len__())

            if step == 1:
                pass
            elif step == -1:
                start, stop = stop + 1, start + 1
            else:
                ranging: Any = range(start, stop, step)
                if step > 0:
                    ranging = reversed(ranging)

                for i in ranging:
                    self._update_pages(i, i + 1)

                return

        else:
            index = self._make_index_nonnegative(index)
            start = index
            stop = index + 1

        self._update_pages(start, stop)

    def __getitem__(self, index: int) -> Any:
        index = self._make_index_nonnegative(index)
        return self._getitem(index)

    def __iadd__(self: _PLB, values: Union[_PLB, Iterable[Any]]) -> _PLB:
        if isinstance(values, self.__class__):
            self.extend(values)
        else:
            self.extend_iterable(values)

        return self

    def _init(self, array: Sequence[Any]) -> None:
        length = len(array)
        self._pages = [Page(array)] if length != 0 else []
        self._offsets = Offsets(length, length)

    def _make_index_nonnegative(self, index: int) -> int:
        return index if index >= 0 else self.__len__() + index

    def _getitem(self, index: int) -> Any:
        i, j = self._offsets.get_coordinate(index)
        return self._pages[i].get_item(j)

    def _update_pages(
        self, start: int, stop: int, pages: Optional[Sequence[PageBase[Any]]] = None
    ) -> None:
        if start >= stop and not pages:
            return

        stop = max(start, stop)

        start_i, start_j = self._offsets.get_coordinate(start)
        stop_i, stop_j = self._offsets.get_coordinate(stop - 1)

        update_pages: List[PageBase[Any]] = []
        update_lengths = []

        left_page = self._pages[start_i].get_slice(stop=start_j)
        left_length = len(left_page)
        if left_length:
            update_pages.append(left_page)
            update_lengths.append(left_length)

        if pages:
            update_pages.extend(pages)
            update_lengths.extend(map(len, pages))

        right_page = self._pages[stop_i].get_slice(stop_j + 1)
        right_length = len(right_page)
        if right_length:
            update_pages.append(right_page)
            update_lengths.append(right_length)

        self._pages[start_i : stop_i + 1] = update_pages
        self._offsets.update(start_i, stop_i, update_lengths)

    def _update_pages_with_step(self: _PLB, start: int, stop: int, step: int, values: _PLB) -> None:
        length = len(values)
        ranging: Any = range(start, stop, step)
        indices: Any = range(length)

        if length != len(ranging):
            raise ValueError(
                f"attempt to assign sequence of size {length} "
                f"to extended slice of size {len(ranging)}"
            )

        if step > 0:
            ranging = reversed(ranging)
            indices = reversed(indices)

        offsets = values._offsets  # pylint: disable=protected-access
        pages = values._pages  # pylint: disable=protected-access

        for i, j in zip(ranging, indices):
            x, y = offsets.get_coordinate(j)
            self._update_pages(i, i + 1, [pages[x][y : y + 1]])

    def set_item(self, index: int, value: Any) -> None:
        """Update the element value in PagingList at the given index.

        Arguments:
            index: The element index.
            value: The value needs to be set into the PagingList.

        """
        index = self._make_index_nonnegative(index)
        page = Page(self._array_creator((value,)))
        self._update_pages(index, index + 1, [page])

    def set_slice(self: _PLB, index: slice, values: _PLB) -> None:
        """Update the element values at the given slice with input PagingList.

        Arguments:
            index: The element slice.
            values: The PagingList which contains the elements to be set.

        Raises:
            ValueError: When the input size mismatches with the slice size (when step != 1).

        """
        start, stop, step = index.indices(self.__len__())

        if step == 1:
            self._update_pages(start, stop, values._pages)  # pylint: disable=protected-access
            return

        if step == -1:
            start, stop = stop + 1, max(start, stop) + 1
            if len(values) != stop - start:
                raise ValueError(
                    f"attempt to assign sequence of size {len(values)} "
                    f"to extended slice of size {stop - start}"
                )

            self._update_pages(
                start,
                stop,
                [
                    page.get_slice(step=-1)
                    for page in reversed(values._pages)  # pylint: disable=protected-access
                ],
            )
            return

        self._update_pages_with_step(start, stop, step, values)

    def set_slice_iterable(self, index: slice, values: Iterable[Any]) -> None:
        """Update the element values in PagingList at the given slice with iterable object.

        Arguments:
            index: The element slice.
            values: The iterable object which contains the elements to be set.

        Raises:
            ValueError: When the assign input size mismatches with the slice size (when step != 1).

        """
        start, stop, step = index.indices(self.__len__())

        if step == 1:
            array = self._array_creator(values)
            self._update_pages(start, stop, [Page(array)] if len(array) != 0 else None)
            return

        if step == -1:
            try:
                values = reversed(values)  # type: ignore[call-overload]
            except TypeError:
                values = reversed(list(values))

            array = self._array_creator(values)

            start, stop = stop + 1, max(start, stop) + 1
            if len(array) != stop - start:
                raise ValueError(
                    f"attempt to assign sequence of size {len(array)} "
                    f"to extended slice of size {stop - start}"
                )

            self._update_pages(start, stop, [Page(array)] if len(array) != 0 else None)
            return

        self._update_pages_with_step(start, stop, step, self.__class__(self._array_creator(values)))

    def extend(self: _PLB, values: _PLB) -> None:
        """Extend PagingList by appending elements from another PagingList.

        Arguments:
            values: The PagingList which contains the elements to be extended.

        """
        pages = values._pages  # pylint: disable=protected-access
        self._offsets.extend(map(len, pages))
        self._pages.extend(pages)

    def extend_iterable(self, values: Iterable[Any]) -> None:
        """Extend PagingList by appending elements from the iterable.

        Arguments:
            values: Elements to be extended into the PagingList.

        """
        page = Page(self._array_creator(values))
        self._offsets.extend((len(page),))
        self._pages.append(page)

    def extend_nulls(self, size: int) -> None:
        """Extend PagingList by appending nulls.

        Arguments:
            size: The size of the nulls to be extended.

        """
        page = Page(self._array_creator(repeat(None, size)))
        self._offsets.extend((len(page),))
        self._pages.append(page)

    def copy(self: _PLB) -> _PLB:
        """Return a copy of the paging list.

        Returns:
            A copy of the paging list.

        """
        obj: _PLB = object.__new__(self.__class__)
        # pylint: disable=protected-access
        obj._pages = self._pages.copy()
        obj._offsets = self._offsets.copy()
        return obj


class PagingList(PagingListBase):
    """PagingList is a list composed of multiple lists (pages)."""

    @classmethod
    def from_factory(
        cls: Type[_PL],
        factory: "LazyFactory",
        keys: Tuple[str, ...],
        mapper: Callable[[Any], Any],
    ) -> _PL:
        """Create PagingList from LazyFactory.

        Arguments:
            factory: The parent :class:`LazyFactory` instance.
            keys: The keys to access the array from factory.
            mapper: A callable object to convert every item in the pyarrow array.

        Returns:
            The PagingList instance created from given factory.

        """
        obj: _PL = object.__new__(cls)

        array_getter = partial(factory.get_array, keys=keys)

        obj._pages = [
            MappedLazyPage(ranging, pos, array_getter, mapper)
            for pos, ranging in enumerate(factory.get_page_ranges())
        ]
        obj._offsets = factory.get_offsets()

        return obj


class PyArrowPagingList(PagingListBase):
    """PyArrowPagingList is a list composed of multiple pyarrow arrays (pages).

    Arguments:
        array: The input pyarrow array.

    """

    _array_creator = pa.array
    _patype: pa.DataType

    def _init(self, array: pa.Array) -> None:
        super()._init(array)
        self._patype = array.type
        self._array_creator = partial(pa.array, type=array.type)

    @classmethod
    def from_pyarrow(cls: Type[_PPL], array: pa.Array) -> _PPL:
        """Create PyArrowPagingList from pyarrow array.

        Arguments:
            array: The input pyarrow array.

        Returns:
            The PyArrowPagingList instance created from given pyarrow array.

        """
        obj: _PPL = object.__new__(cls)
        obj._init(array)

        return obj

    @classmethod
    def from_factory(
        cls: Type[_PPL], factory: "LazyFactory", keys: Tuple[str, ...], patype: pa.DataType
    ) -> _PPL:
        """Create PyArrowPagingList from LazyFactory.

        Arguments:
            factory: The parent :class:`LazyFactory` instance.
            keys: The keys to access the array from factory.
            patype: The pyarrow DataType of the elements in the list.

        Returns:
            The PyArrowPagingList instance created from given factory.

        """
        obj: _PPL = object.__new__(cls)
        array_getter = partial(factory.get_array, keys=keys)
        obj._pages = [
            LazyPage(ranging, pos, array_getter)
            for pos, ranging in enumerate(factory.get_page_ranges())
        ]
        obj._offsets = factory.get_offsets()
        obj._patype = patype

        return obj

    def set_slice(self: _PPL, index: slice, values: _PPL) -> None:
        """Update the element values at the given slice with input PyArrowPagingList.

        Arguments:
            index: The element slice.
            values: The PyArrowPagingList which contains the elements to be set.

        Raises:
            ArrowTypeError: When two pyarrow types mismatch.

        """
        # pylint: disable=protected-access
        if values._patype != self._patype:
            raise pa.ArrowTypeError(
                f"Can not set a '{self._patype}' list with a '{values._patype}' list"
            )

        super().set_slice(index, values)

    def extend(self: _PPL, values: _PPL) -> None:
        """Extend PyArrowPagingList by appending elements from another PyArrowPagingList.

        Arguments:
            values: The PyArrowPagingList which contains the elements to be extended.

        Raises:
            ArrowTypeError: When two pyarrow types mismatch.

        """
        # pylint: disable=protected-access
        if values._patype != self._patype:
            raise pa.ArrowTypeError(
                f"Can not extend a '{self._patype}' list with a '{values._patype}' list"
            )

        super().extend(values)

    def extend_nulls(self, size: int) -> None:
        """Extend PyArrowPagingList by appending nulls.

        Arguments:
            size: The size of the nulls to be extended.

        """
        page = Page(pa.nulls(size, self._patype))
        self._offsets.extend((len(page),))
        self._pages.append(page)

    def copy(self: _PPL) -> _PPL:
        """Return a copy of the paging list.

        Returns:
            A copy of the paging list.

        """
        obj = super().copy()
        # pylint: disable=protected-access
        obj._array_creator = self._array_creator
        obj._patype = self._patype
        return obj

    def to_pyarrow(self) -> pa.ChunkedArray:
        """Convert the paging list to pyarrow ChunkedArray.

        Returns:
            The pyarrow ChunkedArray.

        """
        return pa.chunked_array((page.get_array() for page in self._pages), self._patype)
