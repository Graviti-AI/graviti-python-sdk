#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Paging list related class."""

from bisect import bisect_right
from functools import partial
from itertools import accumulate, chain, repeat
from math import ceil
from typing import (
    Any,
    Callable,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

import pyarrow as pa

_P = TypeVar("_P", bound="PagingList")
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
            self._offsets = list(range(0, self.total_count, self._limit))

        return self._offsets

    def update(self, start: int, stop: int, lengths: Iterable[int]) -> None:
        """Update the offsets when setting or deleting paging list items.

        Arguments:
            start: The start index.
            stop: The stop index.
            lengths: The length of the set values.

        """
        offsets = self._get_offsets()
        partial_offsets = list(accumulate(lengths, initial=offsets[start]))
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
            return divmod(index, self._limit)

        i = bisect_right(self._offsets, index) - 1
        return i, index - self._offsets[i]

    def extend(self, lengths: Iterable[int]) -> None:
        """Update the offsets when extending the paging list.

        Arguments:
            lengths: The lengths of the extended pages.

        """
        offsets = self._get_offsets()
        offsets.extend(accumulate(lengths, initial=self.total_count))
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


class PagingList:
    """PagingList is a list composed of multiple lists (pages).

    Arguments:
        array: The input pyarrow array.

    """

    def __init__(self, array: pa.Array) -> None:
        length = len(array)
        self._pages = [Page(array)] if length != 0 else []
        self._offsets = Offsets(length, length)
        self._patype = array.type

    def __len__(self) -> int:
        return self._offsets.total_count

    def __iter__(self) -> Iterator[Any]:
        return chain(*self._pages)

    @overload
    def __setitem__(self, index: int, value: Any) -> None:
        ...

    @overload
    def __setitem__(self, index: slice, value: Iterable[Any]) -> None:
        ...

    def __setitem__(self, index: Union[int, slice], value: Union[Any, Iterable[Any]]) -> None:
        if isinstance(index, slice):
            start, stop, step = index.indices(self.__len__())

            if step == 1:
                page = Page(pa.array(value, self._patype))
            elif step == -1:
                start, stop = stop + 1, start + 1
                try:
                    reversed_values = reversed(value)  # type: ignore[arg-type]
                except TypeError:
                    reversed_values = reversed(list(value))

                page = Page(pa.array(reversed_values, self._patype))
                if len(page) != stop - start:
                    raise ValueError(
                        f"attempt to assign sequence of size {len(page)} "
                        f"to extended slice of size {stop - start}"
                    )
            else:
                page = Page(pa.array(value, self._patype))
                ranging: Any = range(start, stop, step)
                indices: Any = range(len(page))
                if len(page) != len(ranging):
                    raise ValueError(
                        f"attempt to assign sequence of size {len(page)} "
                        f"to extended slice of size {len(ranging)}"
                    )

                if step > 0:
                    ranging = reversed(ranging)
                    indices = reversed(indices)

                for i, j in zip(ranging, indices):
                    self._update_pages(i, i + 1, [page[j : j + 1]])

                return

        else:
            index = self._make_index_nonnegative(index)
            start = index
            stop = index + 1
            page = Page(pa.array((value,), self._patype))

        if len(page) == 0:
            return

        self._update_pages(start, stop, [page])

    def __delitem__(self, index: Union[int, slice]) -> None:
        if isinstance(index, slice):
            start, stop, step = index.indices(self.__len__())
            if start == stop:
                return

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

    def __iadd__(self: _P, values: Union["PagingList", Iterable[Any]]) -> _P:
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

    def _update_pages(self, start: int, stop: int, pages: Optional[List["Page"]] = None) -> None:
        start_i, start_j = self._offsets.get_coordinate(start)
        stop_i, stop_j = self._offsets.get_coordinate(stop - 1)

        update_pages = []
        lengths = []

        left_page = self._pages[start_i].get_slice(stop=start_j)
        left_length = len(left_page)
        if left_length:
            update_pages.append(left_page)
            lengths.append(left_length)

        if pages is not None:
            update_pages.extend(pages)
            lengths.extend(map(len, pages))

        right_page = self._pages[stop_i].get_slice(stop_j + 1)
        right_length = len(right_page)
        if right_length:
            update_pages.append(right_page)
            lengths.append(right_length)

        self._pages[start_i : stop_i + 1] = update_pages
        self._offsets.update(start_i, stop_i, lengths)

    @classmethod
    def from_factory(
        cls: Type[_P], factory: "LazyFactory", keys: Tuple[str, ...], patype: pa.DataType
    ) -> _P:
        """Create PagingList from LazyFactory.

        Arguments:
            factory: The parent :class:`LazyFactory` instance.
            keys: The keys to access the array from factory.
            patype: The pyarrow DataType of the elements in the list.

        Returns:
            The PagingList instance created from given factory.

        """
        obj: _P = object.__new__(cls)
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

    def extend(self, values: "PagingList") -> None:
        """Extend PagingList by appending elements from another PagingList.

        Arguments:
            values: The PagingList which contains the elements to be extended.

        Raises:
            ArrowTypeError: When the pyarrow type mismatch.

        """
        # pylint: disable=protected-access
        if values._patype != self._patype:
            raise pa.ArrowTypeError(
                f"Can not extend a '{self._patype}' list with a '{values._patype}' list"
            )

        pages = values._pages
        self._offsets.extend(map(len, pages))
        self._pages.extend(pages)

    def extend_iterable(self, values: Iterable[Any]) -> None:
        """Extend PagingList by appending elements from the iterable.

        Arguments:
            values: Elements to be extended into the PagingList.

        """
        page = Page(pa.array(values, self._patype))
        self._offsets.extend((len(page),))
        self._pages.append(page)

    def copy(self: _P) -> _P:
        """Return a copy of the paging list.

        Returns:
            A copy of the paging list.

        """
        obj: _P = object.__new__(self.__class__)
        # pylint: disable=protected-access
        obj._pages = self._pages.copy()
        obj._offsets = self._offsets.copy()
        obj._patype = self._patype
        return obj

    def to_pyarrow(self) -> pa.ChunkedArray:
        """Convert the paging list to pyarrow ChunkedArray.

        Returns:
            The pyarrow ChunkedArray.

        """
        return pa.chunked_array(page.get_array() for page in self._pages)


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
            return self.get_slice(index.start, index.stop)

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

    def get_slice(self, start: Optional[int] = None, stop: Optional[int] = None) -> "Page":
        """Return a sliced page according to the given start and stop index.

        Arguments:
            start: The start index.
            stop: The stop index.

        Returns:
            A sliced page according to the given start and stop index.

        """
        return SlicedPage(self._ranging[start:stop], self._array)

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
        self, start: Optional[int] = None, stop: Optional[int] = None
    ) -> "LazySlicedPage":
        """Return a lazy sliced page according to the given start and stop index.

        Arguments:
            start: The start index.
            stop: The stop index.

        Returns:
            A sliced page according to the given start and stop index.

        """
        return LazySlicedPage(self._ranging[start:stop], self._pos, self._array_getter)

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

        return PagingList.from_factory(self, keys, patype)

    def get_page_ranges(self) -> Iterator[range]:
        """A Generator which generates the range of the pages in the factory.

        Yields:
            The page ranges.

        """
        div, mod = divmod(self._total_count, self._limit)
        yield from repeat(range(self._limit), div)
        if mod != 0:
            yield range(mod)
