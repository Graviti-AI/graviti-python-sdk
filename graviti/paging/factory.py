#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Paging list related class."""

from itertools import repeat
from math import ceil
from typing import Any, Callable, Iterator, List, Optional, Tuple, TypeVar

import pyarrow as pa

from graviti.paging.lists import MappedPagingList, PagingList, PyArrowPagingList
from graviti.paging.offset import Offsets
from graviti.paging.wrapper import StructArrayWrapper

_T = TypeVar("_T")


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

    def create_list(self, mapper: Callable[[Any], _T]) -> PagingList[_T]:
        """Create a paging list from the factory.

        Arguments:
            mapper: A callable object to convert every item in the pyarrow array.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError

    def create_mapped_list(self, mapper: Callable[[Any], _T]) -> MappedPagingList[_T]:
        """Create a paging list from the factory.

        Arguments:
            mapper: A callable object to convert every item in the pyarrow array.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError

    def create_pyarrow_list(self) -> PyArrowPagingList[Any]:
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
        >>> paths = factory["remotePath"].create_pyarrow_list()
        >>> categories = factory["label"]["CLASSIFICATION"]["category"].create_pyarrow_list()
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

    def create_list(self, mapper: Callable[[Any], _T]) -> PagingList[_T]:
        """Create a paging list from the factory.

        Arguments:
            mapper: A callable object to convert every item in the pyarrow array.

        Returns:
            A paging list created from the factory.

        """
        return PagingList.from_factory(self, (), mapper)

    def create_mapped_list(self, mapper: Callable[[Any], _T]) -> MappedPagingList[_T]:
        """Create a paging list from the factory.

        Arguments:
            mapper: A callable object to convert every item in the pyarrow array.

        Returns:
            A paging list created from the factory.

        """
        return MappedPagingList.from_factory(self, (), mapper)

    def create_pyarrow_list(self) -> PyArrowPagingList[Any]:
        """Create a paging list from the factory.

        Returns:
            A paging list created from the factory.

        """
        return PyArrowPagingList.from_factory(self, (), self._patype)

    def get_page_lengths(self) -> Iterator[int]:
        """A Generator which generates the length of the pages in the factory.

        Yields:
            The page lengths.

        """
        div, mod = divmod(self._total_count, self._limit)
        yield from repeat(self._limit, div)
        if mod != 0:
            yield mod

    def get_offsets(self) -> Offsets:
        """Get the Offsets instance created by the total_count and limit of this factory.

        Returns:
            The Offsets instance created by the total_count and limit of this factory.

        """
        return Offsets(self._total_count, self._limit)


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

    def create_list(self, mapper: Callable[[Any], _T]) -> PagingList[_T]:
        """Create a paging list from the factory.

        Arguments:
            mapper: A callable object to convert every item in the pyarrow array.

        Returns:
            A paging list created from the factory.

        """
        return PagingList.from_factory(self._factory, self._keys, mapper)

    def create_mapped_list(self, mapper: Callable[[Any], _T]) -> MappedPagingList[_T]:
        """Create a paging list from the factory.

        Arguments:
            mapper: A callable object to convert every item in the pyarrow array.

        Returns:
            A paging list created from the factory.

        """
        return MappedPagingList.from_factory(self._factory, self._keys, mapper)

    def create_pyarrow_list(self) -> PyArrowPagingList[Any]:
        """Create a paging list from the factory.

        Returns:
            A paging list created from the factory.

        """
        return PyArrowPagingList.from_factory(self._factory, self._keys, self._patype)


class LazyLowerCaseFactory(LazyFactory):
    """LazyLowerCaseFactory is a factory to handle the case insensitive data from graviti back-end.

    Arguments:
        total_count: The total count of the elements in the paging lists.
        limit: The size of each lazy load page.
        getter: A callable object to get the source data.
        patype: The pyarrow DataType of the data in the factory.

    """

    def __init__(
        self, total_count: int, limit: int, getter: Callable[[int, int], Any], patype: pa.DataType
    ) -> None:
        super().__init__(total_count, limit, getter, self._lower_patype(patype))

    def __getitem__(self, key: str) -> "LazyLowerCaseSubFactory":
        lower_key = key.lower()
        return LazyLowerCaseSubFactory(self, (lower_key,), self._patype[lower_key].type)

    def _lower_patype(self, patype: pa.DataType) -> pa.DataType:
        if isinstance(patype, pa.StructType):
            return pa.struct(
                {field.name.lower(): self._lower_patype(field.type) for field in patype}
            )

        if isinstance(patype, pa.ListType):
            return pa.list_(self._lower_patype(patype.value_type))

        return patype

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
            array = StructArrayWrapper(array)
            self._pages[pos] = array

        for key in keys:
            array = array.field(key)

        return array


class LazyLowerCaseSubFactory(LazySubFactory):
    """LazyLowerCaseSubFactory is a sub-factory to handle the case insensitive data.

    Arguments:
        factory: The source LazyFactory instance.
        keys: The keys to access the array from the source LazyFactory.
        patype: The pyarrow DataType of the data in the sub-factory.

    """

    def __getitem__(self, key: str) -> "LazyLowerCaseSubFactory":
        lower_key = key.lower()
        return LazyLowerCaseSubFactory(
            self._factory, self._keys + (lower_key,), self._patype[lower_key].type
        )
