#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Paging list related class."""

from functools import partial
from itertools import chain, repeat
from math import ceil
from typing import (
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
from graviti.paging.page import LazyPage, Page, PageBase
from graviti.utility import NestedDict

_PL = TypeVar("_PL", bound="PagingList")
_PPL = TypeVar("_PPL", bound="PyArrowPagingList")


class PagingList:
    """PagingList is a list composed of multiple lists (pages).

    Arguments:
        array: The input sequence.

    """

    _array_creator: Callable[[Iterable[Any]], Sequence[Any]] = tuple

    def __init__(self, array: Sequence[Any]) -> None:
        length = len(array)
        self._pages: List[PageBase[Any]] = [Page(array)] if length != 0 else []
        self._offsets = Offsets(length, length)

    def __len__(self) -> int:
        return self._offsets.total_count

    def __iter__(self) -> Iterator[Any]:
        return chain(*self._pages)

    @overload
    def __setitem__(self, index: int, value: Any) -> None:
        ...

    @overload
    def __setitem__(self: _PL, index: slice, value: Union[Iterable[Any], _PL]) -> None:
        ...

    def __setitem__(
        self: _PL, index: Union[int, slice], value: Union[Any, Iterable[Any], _PL]
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

    def __iadd__(self: _PL, values: Union[_PL, Iterable[Any]]) -> _PL:
        if isinstance(values, self.__class__):
            self.extend(values)
        else:
            self.extend_iterable(values)

        return self

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

    def _update_pages_with_step(self: _PL, start: int, stop: int, step: int, values: _PL) -> None:
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

    # @classmethod
    # def from_factory(cls: Type[_B], factory: "LazyFactory", keys: Tuple[str, ...]) -> _B:
    #     """Create PagingList from LazyFactory.

    #     Arguments:
    #         factory: The parent :class:`LazyFactory` instance.
    #         keys: The keys to access the array from factory.

    #     Returns:
    #         The PagingList instance created from given factory.

    #     """
    #     obj: _B = object.__new__(cls)
    #     array_getter = partial(factory.get_array, keys=keys)
    #     obj._pages = [
    #         LazyPage(ranging, pos, array_getter)
    #         for pos, ranging in enumerate(factory.get_page_ranges())
    #     ]
    #     obj._offsets = Offsets(
    #         factory._total_count, factory._limit  # pylint: disable=protected-access
    #     )

    #     return obj

    def set_item(self, index: int, value: Any) -> None:
        """Update the element value in PagingList at the given index.

        Arguments:
            index: The element index.
            value: The value needs to be set into the PagingList.

        """
        index = self._make_index_nonnegative(index)
        # https://github.com/python/mypy/issues/5485
        page = Page(self._array_creator((value,)))  # type: ignore[call-arg]
        self._update_pages(index, index + 1, [page])

    def set_slice(self: _PL, index: slice, values: _PL) -> None:
        """Update the element values at the given slice with input PagingList.

        Arguments:
            index: The element slice.
            values: The PagingList which contains the elements to be set.

        Raises:
            ValueError: When the input size mismatches with the slice size (when step != 1).

        """
        start, stop, step = index.indices(self.__len__())

        if step == 1:
            self._update_pages(start, stop, values._pages)
            return

        if step == -1:
            start, stop = stop + 1, max(start, stop) + 1
            if len(values) != stop - start:
                raise ValueError(
                    f"attempt to assign sequence of size {len(values)} "
                    f"to extended slice of size {stop - start}"
                )

            self._update_pages(
                start, stop, [page.get_slice(step=-1) for page in reversed(values._pages)]
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
            # https://github.com/python/mypy/issues/5485
            array = self._array_creator(values)  # type: ignore[call-arg]
            self._update_pages(start, stop, [Page(array)] if len(array) != 0 else None)
            return

        if step == -1:
            try:
                values = reversed(values)  # type: ignore[call-overload]
            except TypeError:
                values = reversed(list(values))

            # https://github.com/python/mypy/issues/5485
            array = self._array_creator(values)  # type: ignore[call-arg]

            start, stop = stop + 1, max(start, stop) + 1
            if len(array) != stop - start:
                raise ValueError(
                    f"attempt to assign sequence of size {len(array)} "
                    f"to extended slice of size {stop - start}"
                )

            self._update_pages(start, stop, [Page(array)] if len(array) != 0 else None)
            return

        # https://github.com/python/mypy/issues/5485
        self._update_pages_with_step(
            start,
            stop,
            step,
            self.__class__(self._array_creator(values)),  # type: ignore[call-arg]
        )

    def extend(self: _PL, values: _PL) -> None:
        """Extend PagingList by appending elements from another PagingList.

        Arguments:
            values: The PagingList which contains the elements to be extended.

        """
        pages = values._pages
        self._offsets.extend(map(len, pages))
        self._pages.extend(pages)

    def extend_iterable(self, values: Iterable[Any]) -> None:
        """Extend PagingList by appending elements from the iterable.

        Arguments:
            values: Elements to be extended into the PagingList.

        """
        # https://github.com/python/mypy/issues/5485
        page = Page(self._array_creator(values))  # type: ignore[call-arg]
        self._offsets.extend((len(page),))
        self._pages.append(page)

    def extend_nulls(self, size: int) -> None:
        """Extend PagingList by appending nulls.

        Arguments:
            size: The size of the nulls to be extended.

        """
        # https://github.com/python/mypy/issues/5485
        page = Page(self._array_creator(repeat(None, size)))  # type: ignore[call-arg]
        self._offsets.extend((len(page),))
        self._pages.append(page)

    def copy(self: _PL) -> _PL:
        """Return a copy of the paging list.

        Returns:
            A copy of the paging list.

        """
        obj: _PL = object.__new__(self.__class__)
        # pylint: disable=protected-access
        obj._pages = self._pages.copy()
        obj._offsets = self._offsets.copy()
        return obj


class PyArrowPagingList(PagingList):
    """PyArrowPagingList is a list composed of multiple pyarrow arrays (pages).

    Arguments:
        array: The input pyarrow array.

    """

    def __init__(self, array: pa.Array) -> None:
        super().__init__(array)
        self._patype = array.type
        # https://github.com/python/mypy/issues/708
        # https://github.com/python/mypy/issues/2427
        self._array_creator = partial(pa.array, type=array.type)  # type: ignore[assignment]

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
        obj._offsets = Offsets(
            factory._total_count, factory._limit  # pylint: disable=protected-access
        )
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
        # https://github.com/python/mypy/issues/708
        # https://github.com/python/mypy/issues/2427
        obj._array_creator = self._array_creator  # type: ignore[assignment]
        obj._patype = self._patype
        return obj

    def to_pyarrow(self) -> pa.ChunkedArray:
        """Convert the paging list to pyarrow ChunkedArray.

        Returns:
            The pyarrow ChunkedArray.

        """
        return pa.chunked_array((page.get_array() for page in self._pages), self._patype)


PagingLists = NestedDict[str, PagingList]


class LazyFactory:
    """LazyFactory is a factory for creating paging lists.

    Arguments:
        total_count: The total count of the elements in the paging lists.
        limit: The size of each lazy load page.
        getter: A callable object to get the source data.
        patype: The pyarrow DataType of the data in the factory.

    Examples:
        >>> import pyarrow as pa
        >>> patype = pa.struct(
        ...     {
        ...         "remotePath": pa.string(),
        ...         "label": pa.struct({"CLASSIFICATION": pa.struct({"category": pa.string()})}),
        ...     }
        ... )
        >>> TOTAL_COUNT = 1000
        >>> def getter(offset: int, limit: int) -> List[Dict[str, Any]]:
        ...     stop = min(offset + limit, TOTAL_COUNT)
        ...     return [
        ...         {
        ...             "remotePath": f"{i:06}.jpg",
        ...             "label": {"CLASSIFICATION": {"category": "cat" if i % 2 else "dog"}},
        ...         }
        ...         for i in range(offset, stop)
        ...     ]
        ...
        >>> factory = LazyFactory(TOTAL_COUNT, 128, getter, patype)
        >>> paths = factory.create_list(("remotePath",))
        >>> categories = factory.create_list(("label", "CLASSIFICATION", "category"))
        >>> len(paths)
        1000
        >>> list(paths)
        [<pyarrow.StringScalar: '000000.jpg'>,
         <pyarrow.StringScalar: '000001.jpg'>,
         <pyarrow.StringScalar: '000002.jpg'>,
         <pyarrow.StringScalar: '000003.jpg'>,
         <pyarrow.StringScalar: '000004.jpg'>,
         <pyarrow.StringScalar: '000005.jpg'>,
         ...
         ...
         <pyarrow.StringScalar: '000999.jpg'>]
        >>> len(categories)
        1000
        >>> list(categories)
        [<pyarrow.StringScalar: 'dog'>,
         <pyarrow.StringScalar: 'cat'>,
         <pyarrow.StringScalar: 'dog'>,
         <pyarrow.StringScalar: 'cat'>,
         <pyarrow.StringScalar: 'dog'>,
         ...
         ...
         <pyarrow.StringScalar: 'cat'>]


    """

    def __init__(
        self, total_count: int, limit: int, getter: Callable[[int, int], Any], patype: pa.DataType
    ) -> None:
        self._getter = getter
        self._total_count = total_count
        self._limit = limit
        self._patype = patype
        self._pages: List[Optional[pa.StructArray]] = [None] * ceil(total_count / limit)

    def get_array(self, pos: int, keys: Tuple[str, ...]) -> pa.Array:
        """Get the array from the factory.

        Arguments:
            pos: The page number.
            keys: The keys to access the array from factory.

        Returns:
            The requested pyarrow array.

        """
        array = self._pages[pos]
        if array is None:
            array = pa.array(self._getter(pos * self._limit, self._limit), type=self._patype)
            self._pages[pos] = array

        for key in keys:
            array = array.field(key)

        return array

    def create_list(self, keys: Tuple[str, ...]) -> PagingList:
        """Create a paging list from the factory.

        Arguments:
            keys: The keys to access the array from factory.

        Returns:
            A paging list created by the given keys.

        """
        patype = self._patype
        for key in keys:
            patype = patype[key].type

        return PyArrowPagingList.from_factory(self, keys, patype)

    def create_lists(self, keys: List[Tuple[str, ...]]) -> PagingLists:
        """Create a dict of PagingList from the given keys.

        Arguments:
            keys: A list of keys to create the paging lists.

        Returns:
            The created paging lists.

        """
        paging_lists: PagingLists = {}
        for key in keys:
            position = paging_lists
            for subkey in key[:-1]:
                position = position.setdefault(subkey, {})  # type: ignore[assignment]
            position[key[-1]] = self.create_list(key)

        return paging_lists

    def get_page_ranges(self) -> Iterator[range]:
        """A Generator which generates the range of the pages in the factory.

        Yields:
            The page ranges.

        """
        div, mod = divmod(self._total_count, self._limit)
        yield from repeat(range(self._limit), div)
        if mod != 0:
            yield range(mod)
