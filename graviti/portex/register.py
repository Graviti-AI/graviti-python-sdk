#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""The portex type register related classes."""


from typing import TYPE_CHECKING, ClassVar, Dict, Tuple, Type, TypeVar

from graviti.portex.package import packages

if TYPE_CHECKING:
    from graviti.dataframe import Container
    from graviti.portex.base import PortexType

_P = TypeVar("_P", bound="PortexType")
_C = TypeVar("_C", bound="Container")


class ContainerRegister:
    """The class decorator to connect portex type and the data container.

    Arguments:
        portex_types: The portex types needs to be connected.

    """

    def __init__(self, *portex_types: Type["PortexType"]) -> None:
        self._portex_types = portex_types

    def __call__(self, container: Type[_C]) -> Type[_C]:
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

    EXTERNAL_TYPE_TO_CONTAINER: ClassVar[Dict[Tuple[str, str, str], Type["Container"]]] = {}

    def __init__(self, url: str, revision: str, *names: str) -> None:
        self._url = url
        self._revision = revision
        self._names = names

    def __call__(self, container: Type[_C]) -> Type[_C]:
        """Connect data container with the portex external types.

        Arguments:
            container: The data container needs to be connected.

        Returns:
            The input container class unchanged.

        """
        package = packages.externals.get((self._url, self._revision))

        for name in self._names:
            self.EXTERNAL_TYPE_TO_CONTAINER[self._url, self._revision, name] = container

            if package:
                package[name].container = container

        return container


class PyArrowConversionRegister:
    """Register the Portex type to set the conversion from PyArrow to Portex.

    Arguments:
        pyarrow_type_ids: The id of the corresponding PyArrow types.

    """

    PYARROW_TYPE_ID_TO_PORTEX_TYPE: ClassVar[Dict[int, Type["PortexType"]]] = {}

    def __init__(self, *pyarrow_type_ids: int) -> None:
        self._pyarrow_type_ids = pyarrow_type_ids

    def __call__(self, portex_type: Type[_P]) -> Type[_P]:
        """Register the Portex type and return it back.

        Arguments:
            portex_type: The Portex type to register.

        Returns:
            The original Portex type.

        """
        mapping = self.PYARROW_TYPE_ID_TO_PORTEX_TYPE
        for pyarrow_type_id in self._pyarrow_type_ids:
            mapping[pyarrow_type_id] = portex_type

        return portex_type
