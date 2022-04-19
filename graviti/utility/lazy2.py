#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Lazy list related class."""

from bisect import bisect_right
from itertools import chain
from math import ceil
from typing import Any, Callable, Iterable, Iterator, List, Optional, Tuple, Union

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

    def _init_offsets(self) -> None:
        self._offsets = list(range(0, self.total_count, self._limit))

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
        if not hasattr(self, "_offsets"):
            self._init_offsets()

        self._offsets.append(self.total_count)
        self.total_count += length


class LazyList:
    """LazyList is a lazy load list.

    Arguments:
        factory: The parent :class:`LazyFactory` instance.
        keys: The keys to access the array from factory.

    """

    def __init__(
        self,
        factory: "LazyFactory",
        keys: Tuple[str, ...],
    ) -> None:
        self._keys = keys
        self._pages: List[Union[LazyPage, pa.Array]] = [
            LazyPage(pos, self) for pos in range(factory._page_number)
        ]
        self._factory = factory
        self._offsets = Offsets(factory._total_count, factory._limit)

    def __len__(self) -> int:
        return self._offsets.total_count

    def __iter__(self) -> Iterator[Any]:
        return chain(*self._pages)

    def __getitem__(self, index: int) -> Any:
        index = index if index >= 0 else self._offsets.total_count + index
        return self._getitem(index)

    def _getitem(self, index: int) -> Any:
        i, j = self._offsets.get_coordinate(index)
        return self._pages[i][j]

    def extend(self, values: Iterable[Any]) -> None:
        """Extend LazyList by appending elements from the iterable.

        Arguments:
            values: Elements to be extended into the LazyList.

        """
        page = pa.array(values)
        self._offsets.extend(len(page))
        self._pages.append(page)


class LazyPage:
    """LazyPage is a placeholder of the pages in the lazy list when the page is not loaded yet.

    Arguments:
        pos: The page number.
        parent: The parent lazy list.

    """

    def __init__(self, pos: int, parent: LazyList) -> None:
        self._pos = pos
        self._parent = parent

    def _get_page(self) -> pa.Array:
        # pylint: disable=protected-access
        array = self._parent._factory.get_array(self._pos, self._parent._keys)
        self._parent._pages[self._pos] = array
        return array

    def __getitem__(self, index: int) -> Any:
        return self._get_page().__getitem__(index)

    def __iter__(self) -> Iterator[Any]:
        return self._get_page().__iter__()  # type: ignore[no-any-return]


class LazyFactory:
    """LazyFactory is a factory for creating lazy lists.

    Arguments:
        total_count: The total count of the elements in the lazy lists.
        limit: The size of each lazy load page.
        getter: A callable object to get the source data.

    Examples:
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
        >>> factory = LazyFactory(TOTAL_COUNT, 128, getter)
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

    def __init__(self, total_count: int, limit: int, getter: Callable[[int, int], Any]) -> None:
        self._getter = getter
        self._total_count = total_count
        self._limit = limit

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
            array = pa.array(self._getter(pos * self._limit, self._limit))
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
        return LazyList(self, keys)
