#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Common tools."""

from collections import defaultdict
from functools import wraps
from threading import Lock
from typing import Any, Callable, DefaultDict, TypeVar

_CallableWithoutReturnValue = TypeVar("_CallableWithoutReturnValue", bound=Callable[..., None])
_T = TypeVar("_T")

locks: DefaultDict[int, Lock] = defaultdict(Lock)


def locked(func: _CallableWithoutReturnValue) -> _CallableWithoutReturnValue:
    """The decorator to add threading lock for methods.

    Arguments:
        func: The method needs to add threading lock.

    Returns:
        The method with theading locked.

    """

    @wraps(func)
    def wrapper(self: Any, *arg: Any, **kwargs: Any) -> None:
        key = id(self)
        lock = locks[key]
        acquire = lock.acquire(blocking=False)
        try:
            if acquire:
                func(self, *arg, **kwargs)
                del locks[key]
            else:
                lock.acquire()
        finally:
            lock.release()

    return wrapper  # type: ignore[return-value]


def shorten(origin: str) -> str:
    """Return the first 7 characters of the original string.

    Arguments:
        origin: The string needed to be shortened.

    Returns:
        A string of length 7.

    """
    return origin[:7]
