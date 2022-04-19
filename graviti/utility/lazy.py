#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Lazy list related class."""

from bisect import bisect_right
from itertools import chain
from math import ceil
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    TypeVar,
    Union,
    overload,
)

import pyarrow as pa

from graviti.utility.pyarrow import GravitiExtension
from graviti.utility.repr import ReprMixin, ReprType

_T = TypeVar("_T")


class LazyList(Sequence[_T], ReprMixin):
    """LazyList is a lazy load list which follows Sequence protocol.

    Arguments:
        total_count: The total count of the elements in the lazy list.
        limit: The size of each lazy load page.
        fetcher: A callable object to fetch the data and load it to the lazy list.
        extractor: A callable object to make the source data to an iterable object.
        dtype: The pyarrow data type of the elements in the lazy list.

    Attributes:
        pages: A list of pyarrow arrays that contains the data in the lazy list.

    """

    _repr_type = ReprType.SEQUENCE

    def __init__(
        self,
        total_count: int,
        limit: int,
        fetcher: Callable[[int], None],
        extractor: Callable[[Any], Iterable[Any]],
        *,
        dtype: Optional[pa.DataType] = None,
    ) -> None:
        self.pages: List[Union[LazyPage[_T], pa.Array]] = [
            LazyPage(i, fetcher, self) for i in range(ceil(total_count / limit))
        ]
        self._limit = limit
        self._total_count = total_count
        self._extractor = extractor
        self._dtype = dtype

    def __len__(self) -> int:
        return self._total_count

    @overload
    def __getitem__(self, index: int) -> _T:
        ...

    @overload
    def __getitem__(self, index: slice) -> pa.Array:
        ...

    def __getitem__(self, index: Union[int, slice]) -> Union[_T, pa.Array]:
        if isinstance(index, slice):
            return pa.array([self._getitem(i) for i in range(*index.indices(self._total_count))])

        index = index if index >= 0 else self._total_count + index
        return self._getitem(index)

    def __iter__(self) -> Iterator[_T]:
        return chain(*self.pages)

    def _getitem(self, index: int) -> _T:
        """Get item from the lazy list.

        Arguments:
            index: The index of the item which needs to be non-negative.

        Returns:
            The item at the index position.

        """
        div, mod = divmod(index, self._limit)
        return self.pages[div][mod]

    def _create_array(self, values: Iterable[_T]) -> pa.Array:
        if isinstance(self._dtype, GravitiExtension):
            storage_array = pa.array(values, type=self._dtype.storage_type)
            return pa.ExtensionArray.from_storage(self._dtype, storage_array)

        return pa.array(values, type=self._dtype)

    def update(self, pos: int, data: Any) -> None:
        """Update one page by the given data.

        Arguments:
            pos: The page number.
            data: The source data which needs to be input to the extractor.

        """
        self.pages[pos] = self._create_array(self._extractor(data))


class MutableLazyList(LazyList[_T]):
    """MutableLazyList is a lazy load list which follows Sequence protocol.

    It supports extend method to add items into the list.

    Arguments:
        total_count: The total count of the elements in the lazy list.
        limit: The size of each lazy load page.
        fetcher: A callable object to fetch the data and load it to the lazy list.
        extractor: A callable object to make the source data to an iterable object.
        dtype: The pyarrow data type of the elements in the lazy list.

    Attributes:
        pages: A list of pyarrow arrays that contains the data in the lazy list.

    """

    _repr_type = ReprType.SEQUENCE

    def __init__(
        self,
        total_count: int,
        limit: int,
        fetcher: Callable[[int], None],
        extractor: Callable[[Any], Iterable[Any]],
        *,
        dtype: Optional[pa.DataType] = None,
    ) -> None:
        super().__init__(total_count, limit, fetcher, extractor, dtype=dtype)
        self._offsets = list(range(0, total_count, limit))

    def _getitem(self, index: int) -> _T:
        """Get item from the lazy list.

        Arguments:
            index: The index of the item which needs to be non-negative.

        Returns:
            The item at the index position.

        """
        page_number = bisect_right(self._offsets, index) - 1
        offset = self._offsets[page_number]
        return self.pages[page_number][index - offset]

    def extend(self, values: Iterable[_T]) -> None:
        """Extend mutable sequence by appending elements from the iterable.

        Arguments:
            values: Elements to be Extended into the mutable sequence.

        """
        page = self._create_array(values)
        self._offsets.append(self._total_count)
        self._total_count += len(page)
        self.pages.append(page)


class LazyPage(Generic[_T]):
    """LazyPage is a placeholder of the pages in the lazy list when the page is not loaded yet.

    Arguments:
        pos: The page number.
        fetcher: A callable object to fetch the data and load it to the lazy list.
        parent: The parent lazy list.

    """

    def __init__(self, pos: int, fetcher: Callable[[int], None], parent: LazyList[_T]) -> None:
        self._pos = pos
        self._fetcher = fetcher
        self._parent = parent

    def __getitem__(self, index: int) -> _T:
        self._fetcher(self._pos)
        return self._parent.pages[self._pos].__getitem__(index)

    def __iter__(self) -> Iterator[_T]:
        self._fetcher(self._pos)
        return self._parent.pages[self._pos].__iter__()


class LazyFactory:
    """LazyFactory is a factory for creating lazy lists.

    Arguments:
        total_count: The total count of the elements in the lazy lists.
        limit: The size of each lazy load page.
        getter: A callable object to get the source data.

    Examples:
        >>> TOTAL_COUNT = 1000
        >>> def getter(offset: int, limit: int) -> Dict[str, Any]:
        ...     stop = min(offset + limit, TOTAL_COUNT)
        ...     data = [
        ...         {
        ...             "remotePath": f"{i:06}.jpg",
        ...             "label": {"CLASSIFICATION": {"category": "cat" if i % 2 else "dog"}},
        ...         }
        ...         for i in range(offset, stop)
        ...     ]
        ...
        ...     return {
        ...         "data": data,
        ...         "offset": offset,
        ...         "recordSize": len(data),
        ...         "totalCount": TOTAL_COUNT,
        ...     }
        >>> factory = LazyFactory(TOTAL_COUNT, 128, getter)
        >>> paths = factory.create_list(
        ...     lambda data: (item["remotePath"] for item in data["data"]), dtype="<U10"
        ... )
        >>> categories = factory.create_list(
        ...     lambda data: (item["label"]["CLASSIFICATION"]["category"] for item in data["data"]),
        ...     dtype="<U3",
        ... )
        >>> paths
        LazyList [
          '000000.jpg',
          '000001.jpg',
          '000002.jpg',
          '000003.jpg',
          '000004.jpg',
          '000005.jpg',
          '000006.jpg',
          '000007.jpg',
          '000008.jpg',
          '000009.jpg',
          '000010.jpg',
          '000011.jpg',
          '000012.jpg',
          '000013.jpg',
          ... (985 items are folded),
          '000999.jpg'
        ]
        >>> categories
        LazyList [
          'dog',
          'cat',
          'dog',
          'cat',
          'dog',
          'cat',
          'dog',
          'cat',
          'dog',
          'cat',
          'dog',
          'cat',
          'dog',
          'cat',
          ... (985 items are folded),
          'cat'
        ]

    """

    def __init__(self, total_count: int, limit: int, getter: Callable[[int, int], Any]) -> None:
        self._getter = getter
        self._total_count = total_count
        self._limit = limit
        self._lists: List[LazyList[Any]] = []

    def create_list(
        self, extractor: Callable[[Any], Iterable[Any]], dtype: Optional[pa.DataType] = None
    ) -> LazyList[Any]:
        """Create a lazy list from the factory.

        Arguments:
            extractor: A callable object to make the source data to an iterable object.
            dtype: The pyarrow data type of the elements in the lazy list.

        Returns:
            A lazy list created by the given extractor and dtype.

        """
        lazy_list: LazyList[Any] = LazyList(
            self._total_count, self._limit, self.fetch, extractor, dtype=dtype
        )
        self._lists.append(lazy_list)
        return lazy_list

    def create_mutable_list(
        self, extractor: Callable[[Any], Iterable[Any]], dtype: Optional[pa.DataType] = None
    ) -> LazyList[Any]:
        """Create a mutable lazy list from the factory.

        Arguments:
            extractor: A callable object to make the source data to an iterable object.
            dtype: The pyarrow data type of the elements in the lazy list.

        Returns:
            A lazy list created by the given extractor and dtype.

        """
        lazy_list: LazyList[Any] = MutableLazyList(
            self._total_count, self._limit, self.fetch, extractor, dtype=dtype
        )
        self._lists.append(lazy_list)
        return lazy_list

    def fetch(self, pos: int) -> None:
        """Fetch the source data and load the data to all lazy lists.

        Arguments:
            pos: The page number.

        """
        data = self._getter(self._limit * pos, self._limit)
        for lazy_list in self._lists:
            lazy_list.update(pos, data)
