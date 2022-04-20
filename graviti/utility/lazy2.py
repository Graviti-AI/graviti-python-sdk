#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Lazy list related class."""

from bisect import bisect_right
from itertools import accumulate, chain, repeat
from math import ceil
from typing import Any, Callable, Iterable, Iterator, List, Optional, Tuple, Union, overload

import pyarrow as pa


class Offsets:
    """The offsets manager of the lazy list.

    Arguments:
        total_count: The total count of the elements in the lazy list.
        limit: The size of each lazy load page.

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
        """Update the offsets when setting or deleting lazy list items.

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
        """Get the lazy page coordinate of the elements.

        Arguments:
            index: The index of the element in lazy list.

        Returns:
            The page number and the index of the page.

        """
        if not hasattr(self, "_offsets"):
            return divmod(index, self._limit)

        i = bisect_right(self._offsets, index) - 1
        return i, index - self._offsets[i]

    def extend(self, length: int) -> None:
        """Update the offsets when extending the lazy list.

        Arguments:
            length: The length of the extended values.

        """
        offsets = self._get_offsets()
        offsets.append(self.total_count)
        self.total_count += length


class LazyList:
    """LazyList is a lazy load list.

    Arguments:
        factory: The parent :class:`LazyFactory` instance.
        keys: The keys to access the array from factory.
        patype: The pyarrow DataType of the elements in the list.

    """

    def __init__(
        self,
        factory: "LazyFactory",
        keys: Tuple[str, ...],
        patype: pa.DataType,
    ) -> None:
        self._keys = keys
        self._pages: List[Union[LazyPage, pa.Array]] = [
            LazyPage(pos, range(size), self) for pos, size in enumerate(factory.get_page_sizes())
        ]
        self._factory = factory
        self._offsets = Offsets(factory._total_count, factory._limit)
        self._patype = patype

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
                array = pa.array(value, self._patype)
            elif step == -1:
                start, stop = stop + 1, start + 1
                try:
                    reversed_values = reversed(value)  # type: ignore[arg-type]
                except TypeError:
                    reversed_values = reversed(list(value))

                array = pa.array(reversed_values, self._patype)
                if len(array) != stop - start:
                    raise ValueError(
                        f"attempt to assign sequence of size {len(array)} "
                        f"to extended slice of size {stop - start}"
                    )
            else:
                array = pa.array(value, self._patype)
                ranging: Any = range(start, stop, step)
                indices: Any = range(len(array))
                if len(array) != len(ranging):
                    raise ValueError(
                        f"attempt to assign sequence of size {len(array)} "
                        f"to extended slice of size {len(ranging)}"
                    )

                if step > 0:
                    ranging = reversed(ranging)
                    indices = reversed(indices)

                for i, j in zip(ranging, indices):
                    self._update_pages(i, i + 1, array[j : j + 1])

                return

        else:
            index = self._make_index_nonnegative(index)
            start = index
            stop = index + 1
            array = pa.array((value,), self._patype)

        if len(array) == 0:
            return

        self._update_pages(start, stop, array)

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

    def _make_index_nonnegative(self, index: int) -> int:
        return index if index >= 0 else self.__len__() + index

    def _getitem(self, index: int) -> Any:
        i, j = self._offsets.get_coordinate(index)
        return self._pages[i][j]

    def _update_pages(self, start: int, stop: int, array: Optional[pa.Array] = None) -> None:
        start_i, start_j = self._offsets.get_coordinate(start)
        stop_i, stop_j = self._offsets.get_coordinate(stop - 1)

        pages = []
        lengths = []

        left_page = self._pages[start_i][:start_j]
        left_length = len(left_page)
        if left_length:
            pages.append(left_page)
            lengths.append(left_length)

        if array is not None:
            pages.append(array)
            lengths.append(len(array))

        right_page = self._pages[stop_i][stop_j + 1 :]
        right_length = len(right_page)
        if right_length:
            pages.append(right_page)
            lengths.append(right_length)

        old_pages_length = len(self._pages)
        self._pages[start_i : stop_i + 1] = pages
        self._offsets.update(start_i, stop_i, lengths)

        if old_pages_length != len(self._pages):
            self._update_parent_pos(start_i + 1)

    def _update_parent_pos(self, start: int) -> None:
        for i, page in enumerate(self._pages[start:], start):
            if isinstance(page, LazyPage):
                page._parent_pos = i  # pylint: disable=protected-access

    def extend(self, values: Iterable[Any]) -> None:
        """Extend LazyList by appending elements from the iterable.

        Arguments:
            values: Elements to be extended into the LazyList.

        """
        page = pa.array(values, self._patype)
        self._offsets.extend(len(page))
        self._pages.append(page)


class LazyPage:
    """LazyPage is a placeholder when the lazy list page is not loaded yet.

    Arguments:
        pos: The page number.
        ranging: The range instance of this page.
        parent: The parent lazy list.

    """

    def __init__(self, pos: int, ranging: range, parent: LazyList) -> None:
        self._factory_pos = pos
        self._parent_pos = pos
        self._parent = parent
        self._ranging = ranging

    def _get_page(self) -> pa.Array:
        # pylint: disable=protected-access
        array = self._parent._factory.get_array(self._factory_pos, self._parent._keys)
        self._parent._pages[self._parent_pos] = array
        return array

    def __len__(self) -> int:
        return self._ranging.__len__()

    @overload
    def __getitem__(self, index: int) -> Any:
        ...

    @overload
    def __getitem__(self, index: slice) -> "LazySlicedPage":
        ...

    def __getitem__(self, index: Union[int, slice]) -> Union[Any, "LazySlicedPage"]:
        if isinstance(index, slice):
            return LazySlicedPage(self._factory_pos, self._ranging[index], self._parent)

        return self._get_page().__getitem__(index)

    def __iter__(self) -> Iterator[Any]:
        return self._get_page().__iter__()  # type: ignore[no-any-return]


class LazySlicedPage(LazyPage):
    """LazySlicedPage is a placeholder when the sliced lazy list page is not loaded yet."""

    def _get_page(self) -> pa.Array:
        # pylint: disable=protected-access
        array = self._parent._factory.get_array(self._factory_pos, self._parent._keys)

        ranging = self._ranging
        array = array[ranging.start : ranging.stop : ranging.step]

        self._parent._pages[self._parent_pos] = array
        return array


class LazyFactory:
    """LazyFactory is a factory for creating lazy lists.

    Arguments:
        total_count: The total count of the elements in the lazy lists.
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

        page_number = ceil(total_count / limit)
        self._pages: List[Optional[pa.StructArray]] = [None] * page_number
        self._page_number = page_number

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

    def create_list(self, keys: Tuple[str, ...]) -> LazyList:
        """Create a lazy list from the factory.

        Arguments:
            keys: The keys to access the array from factory.

        Returns:
            A lazy list created by the given keys.

        """
        patype = self._patype
        for key in keys:
            patype = patype[key].type

        return LazyList(self, keys, patype)

    def get_page_sizes(self) -> Iterator[int]:
        """A Generator which generates the size of the pages in the factory.

        Yields:
            The page sizes.

        """
        div, mod = divmod(self._total_count, self._limit)
        yield from chain(repeat(self._limit, div), (mod,))
