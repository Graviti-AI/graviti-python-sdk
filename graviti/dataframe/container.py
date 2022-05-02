#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""The table-structured data container related classes."""


from typing import Any, List, Type, TypeVar

import pyarrow as pa

from graviti.portex import EXTERNAL_TYPE_TO_CONTAINER, PortexType, packages

_T = TypeVar("_T", bound="Container")


class Container:
    """The base class for the table-structured data container."""

    def __len__(self) -> int:
        raise NotImplementedError

    @classmethod
    def _from_paging(cls: Type[_T], paging: pa.Array, schema: PortexType) -> _T:
        raise NotImplementedError

    @classmethod
    def _from_pyarrow(cls: Type[_T], array: pa.Array, schema: PortexType) -> _T:
        raise NotImplementedError

    def _extend(self: _T, values: _T) -> None:
        raise NotImplementedError

    # TODO: Defines a base indexer for the iloc return type.
    @property
    def iloc(self) -> Any:
        """Purely integer-location based indexing for selection by position.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError

    def to_pylist(self) -> List[Any]:
        """Convert the container to a python list.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError

    def copy(self: _T) -> _T:
        """Get a copy of the container.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError


class ContainerRegister:
    """The class decorator to connect portex type and the data container.

    Arguments:
        portex_types: The portex types needs to be connected.

    """

    def __init__(self, *portex_types: Type[PortexType]) -> None:
        self._portex_types = portex_types

    def __call__(self, container: Type[_T]) -> Type[_T]:
        """Connect data container with the portex types.

        Arguments:
            container: The data container needs to be connected.

        Returns:
            The input container class unchanged.

        """
        for portex_type in self._portex_types:
            portex_type.container = container

        return container


class ExternalContainerRegister:
    """The class decorator to connect portex external type and the data container.

    Arguments:
        url: The git repo url of the external package.
        revision: The git repo revision (tag/commit) of the external package.
        name: The portex external type name.

    """

    def __init__(self, url: str, revision: str, name: str) -> None:
        self._url = url
        self._revision = revision
        self._name = name

    def __call__(self, container: Type[_T]) -> Type[_T]:
        """Connect data container with the portex external types.

        Arguments:
            container: The data container needs to be connected.

        Returns:
            The input container class unchanged.

        """
        EXTERNAL_TYPE_TO_CONTAINER[self._url, self._revision, self._name] = container

        try:
            package = packages.externals[self._url, self._revision]
            class_ = package[self._name]
        except KeyError:
            pass
        else:
            class_.container = container

        return container
