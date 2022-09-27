#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Common tools."""

from collections import defaultdict
from datetime import datetime
from functools import wraps
from threading import Lock
from typing import Any, Callable, DefaultDict, TypeVar

_CallableWithoutReturnValue = TypeVar("_CallableWithoutReturnValue", bound=Callable[..., None])
_WEEKS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

locks: DefaultDict[int, Lock] = defaultdict(Lock)


def urlnorm(url: str) -> str:
    """Normalized the input url by removing the trailing slash.

    Arguments:
        url: the url needs to be normalized.

    Returns:
        The normalized url.

    """
    return url.rstrip("/")


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


def convert_iso_to_datetime(iso: str) -> datetime:
    """Convert iso 8601 format string to datetime format time with local timezone.

    Arguments:
        iso: The iso 8601 format string.

    Returns:
        The datetime format time with local timezone.

    """
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone()


def convert_datetime_to_gmt(utctime: datetime) -> str:
    """Convert datetime to gmt format string.

    Arguments:
        utctime: The datetime with utc timezone.

    Returns:
        The gmt format string.

    """
    return (
        f"{_WEEKS[utctime.weekday()]}, {utctime.day:02d} {_MONTHS[utctime.month - 1]}"
        f" {utctime.year:04d} {utctime.hour:02d}:{utctime.minute:02d}:{utctime.second:02d} GMT"
    )


class ImageMocker:
    """Raise import PIL error for data loader."""

    def __init__(self, message: str) -> None:
        self._message = message

    def __getattribute__(self, name: str) -> Any:
        raise ModuleNotFoundError(self._message)
