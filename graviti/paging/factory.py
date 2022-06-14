#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Paging list related class."""

from itertools import repeat
from math import ceil
from typing import Any, Callable, Iterator, List, Optional, Tuple

import pyarrow as pa

from graviti.paging.lists import PyArrowPagingList


class LazyFactoryBase:
    """LazyFactoryBase is the base class of the lazy facotry."""

    _patype: pa.DataType

    def __getitem__(self, key: str) -> "LazySubFactory":
        raise NotImplementedError

    def __contains__(self, key: str) -> bool:
        try:
            self._patype.__getitem__(key)
            return True
        except KeyError:
            return False

    def create_list(self) -> PyArrowPagingList:
        """Create a paging list from the factory.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError


class LazyFactory(LazyFactoryBase):
    """LazyFactory is a factory for requesting source data and creating paging lists.

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
        >>> paths = factory["remotePath"].create_list()
        >>> categories = factory["label"]["CLASSIFICATION"]["category"].create_list()
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

    def __getitem__(self, key: str) -> "LazySubFactory":
        return LazySubFactory(self, (key,), self._patype[key].type)

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

    def create_list(self) -> PyArrowPagingList:
        """Create a paging list from the factory.

        Returns:
            A paging list created from the factory.

        """
        return PyArrowPagingList.from_factory(self, (), self._patype)

    def get_page_ranges(self) -> Iterator[range]:
        """A Generator which generates the range of the pages in the factory.

        Yields:
            The page ranges.

        """
        div, mod = divmod(self._total_count, self._limit)
        yield from repeat(range(self._limit), div)
        if mod != 0:
            yield range(mod)


class LazySubFactory(LazyFactoryBase):
    """LazySubFactory is a factory for creating paging lists.

    Arguments:
        factory: The source LazyFactory instance.
        keys: The keys to access the array from the source LazyFactory.
        patype: The pyarrow DataType of the data in the sub-factory.

    """

    def __init__(self, factory: LazyFactory, keys: Tuple[str, ...], patype: pa.DataType) -> None:
        self._factory = factory
        self._keys = keys
        self._patype = patype

    def __getitem__(self, key: str) -> "LazySubFactory":
        return LazySubFactory(self._factory, self._keys + (key,), self._patype[key].type)

    def create_list(self) -> PyArrowPagingList:
        """Create a paging list from the factory.

        Returns:
            A paging list created from the factory.

        """
        return PyArrowPagingList.from_factory(self._factory, self._keys, self._patype)
