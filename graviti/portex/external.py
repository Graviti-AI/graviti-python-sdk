#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Portex external base class."""


from inspect import Signature
from typing import TYPE_CHECKING, Any, ClassVar, List, Optional, Tuple

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

    package: ClassVar["ExternalPackage"]
    factory: ClassVar["TypeFactory"]

    def __init_subclass__(cls) -> None:
        cls._signature = cls.params.get_signature()

        container = EXTERNAL_TYPE_TO_CONTAINER.get(
            (cls.package.url, cls.package.revision, cls.__name__)
        )
        if container:
            cls.container = container

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        bound_arguments = self._signature.bind(*args, **kwargs)
        bound_arguments.apply_defaults()
        arguments = bound_arguments.arguments

        for key, value in arguments.items():
            arguments[key] = self.params[key].check(value)

        self.__dict__.update(arguments)

        if not hasattr(self.__class__, "container"):
            self.container = self.internal_type.container

    @property
    def internal_type(self) -> PortexType:
        """Get the internal type of the PortexExternalType.

        Returns:
            The internal type of the PortexExternalType.

        """
        arguments = {name: getattr(self, name) for name in self.params}
        return self.factory(**arguments)

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
        return self.internal_type.to_builtin()  # type: ignore[attr-defined, no-any-return]

    def get_keys(self, type_name: Optional[str] = None) -> List[Tuple[str, ...]]:
        """Get the keys to locate all data, or only get keys of one type if type_name is given.

        Arguments:
            type_name: The name of the target PortexType.

        Returns:
            A list of keys to locate the data.

        """
        if not self.container.has_keys:
            return []

        return self.to_builtin().get_keys(type_name)
