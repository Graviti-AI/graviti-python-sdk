#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Attr related class."""

from itertools import chain
from typing import (
    Any,
    Dict,
    Generic,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    TypeVar,
    Union,
    overload,
)

from graviti.utility.repr import ReprMixin, ReprType

_T = TypeVar("_T")
_D = TypeVar("_D")


class _AttrDict(Generic[_T], ReprMixin):

    _repr_type = ReprType.MAPPING

    def __init__(self) -> None:
        # An external node is one without child branches,
        # while an internal node has at least one child branch.
        self._internals: Dict[str, _AttrDict[_T]] = {}
        self._externals: Dict[str, _T] = {}

    def __len__(self) -> int:
        return sum(map(len, self._internals)) + len(self._externals)

    def __getitem__(self, key: str) -> _T:
        try:
            return self._getitem(key)
        except KeyError:
            return self.__missing__(key)

    def __setitem__(self, key: str, value: _T) -> None:
        try:
            prefix, suffix = key.split(".", 1)
        except ValueError:
            if key in self._internals:
                raise KeyError("Key prefix conflict") from None

            self._externals.__setitem__(key, value)
            return

        if prefix in self._externals:
            raise KeyError("Key prefix conflict")

        module = self._internals.get(prefix)
        if not module:
            module = _AttrDict()
            self._internals.__setitem__(prefix, module)

        module.__setitem__(suffix, value)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False

        try:
            self._getitem(key)
        except KeyError:
            return False

        return True

    def __missing__(self, key: str) -> _T:
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        for key, value in self._internals.items():
            for suffix in value:
                yield ".".join((key, suffix))

        yield from self._externals

    def __getattr__(self, key: str) -> Any:
        try:
            return self._externals.__getitem__(key)
        except KeyError:
            pass

        try:
            return self._internals.__getitem__(key)
        except KeyError:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{key}'"
            ) from None

    def __dir__(self) -> Iterable[str]:
        return chain(self._externals, self._internals, super().__dir__())

    def _getitem(self, key: str) -> _T:
        try:
            prefix, suffix = key.split(".", 1)
        except ValueError:
            return self._externals.__getitem__(key)

        return self._internals.__getitem__(prefix).__getitem__(suffix)

    def _repr_head(self) -> str:
        return AttrDict.__name__


class AttrDict(_AttrDict[_T], Mapping[str, _T]):
    """A dict which allows for attr-style access of values."""

    def _repr_head(self) -> str:
        return self.__class__.__name__

    @overload
    def get(self, key: str) -> Optional[_T]:  # pylint: disable=arguments-differ
        ...

    @overload
    def get(self, key: str, default: _D = ...) -> Union[_D, _T]:
        ...

    def get(self, key: str, default: Any = None) -> Any:
        """Return the value for the key if it is in the dict, else default.

        Arguments:
            key: The key for dict, which can be any immutable type.
            default: The value to be returned if key is not in the dict.

        Returns:
            The value for the key if it is in the dict, else default.

        """
        try:
            return self._getitem(key)
        except KeyError:
            return default
