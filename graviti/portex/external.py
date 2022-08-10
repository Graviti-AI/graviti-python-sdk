#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Portex external base class."""


from inspect import Signature
from typing import TYPE_CHECKING, Any, ClassVar

import pyarrow as pa

from graviti.portex.base import PortexType
from graviti.portex.register import ExternalContainerRegister

if TYPE_CHECKING:
    from graviti.portex.builtin import PortexBuiltinType
    from graviti.portex.factory import TypeFactory
    from graviti.portex.package import ExternalPackage

EXTERNAL_TYPE_TO_CONTAINER = ExternalContainerRegister.EXTERNAL_TYPE_TO_CONTAINER


class PortexExternalType(PortexType):  # pylint: disable=abstract-method
    """The base class of Portex external type."""

    _signature: ClassVar[Signature]
    _factory: ClassVar["TypeFactory"]

    package: ClassVar["ExternalPackage"]

    def __init_subclass__(cls) -> None:
        cls._signature = cls.params.get_signature()

        container = EXTERNAL_TYPE_TO_CONTAINER.get(
            (cls.package.url, cls.package.revision, cls.__name__)
        ) or getattr(cls._factory.class_, "container", None)

        if container:
            cls.container = container

        search_container = getattr(cls._factory.class_, "search_container", None)
        if search_container:
            cls.search_container = search_container

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        bound_arguments = self._signature.bind(*args, **kwargs)
        bound_arguments.apply_defaults()
        arguments = bound_arguments.arguments

        for key, value in arguments.items():
            arguments[key] = self.params[key].check(value)

        self.__dict__.update(arguments)

    def _get_column_count(self) -> int:
        """Get the total column count of the portex type.

        Returns:
            The total column count.

        """
        return self.to_builtin()._get_column_count()  # pylint: disable=protected-access

    @property
    def internal_type(self) -> PortexType:
        """Get the internal type of the PortexExternalType.

        Returns:
            The internal type of the PortexExternalType.

        """
        return self._factory({name: getattr(self, name) for name in self.params})

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return self.internal_type.to_pyarrow()

    def to_builtin(self) -> "PortexBuiltinType":
        """Expand the top level of the Portex external type to Portex builtin type.

        Returns:
            The expanded Portex builtin type.

        """
        return self.internal_type.to_builtin()
