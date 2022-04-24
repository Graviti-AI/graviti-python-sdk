#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Template base class."""


from typing import Any, ClassVar, Dict, Set, Type

import pyarrow as pa

import graviti.schema.ptype as PTYPE
from graviti.schema.base import PortexType
from graviti.schema.factory import Dynamic, Factory, type_factory_creator
from graviti.schema.package import ExternalPackage, Imports, Package, packages
from graviti.schema.param import Param, Params


class PortexExternalType(PortexType):  # pylint: disable=abstract-method
    """The base class of Portex external type."""

    nullable: bool
    dependences: ClassVar[Set[PortexType]]
    package: ClassVar[ExternalPackage]
    factory: ClassVar[Factory]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        arguments = self._bind_arguments(*args, **kwargs)

        for key, value in arguments.items():
            param = self.params[key]
            if isinstance(param.ptype, Dynamic):
                ptype = param.ptype(**arguments)
                param = Param(param.name, param.default, param.options, ptype)

            arguments[key] = param.check(value)

        super().__init__(**arguments)

    @property
    def internal_type(self) -> PortexType:
        """Get the internal type of the PortexExternalType.

        Returns:
            The internal type of the PortexExternalType.

        """
        arguments = {name: getattr(self, name) for name in self.params}
        return self.factory(**arguments)  # type: ignore[no-any-return]

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return self.internal_type.to_pyarrow()


def template(
    name: str, content: Dict[str, Any], package: Package[Any] = packages.locals
) -> Type[PortexExternalType]:
    """Generate a Portex external type according to the input template.

    Arguments:
        name: The class name of the Portex external type.
        content: A dict indicates a Portex template.
        package: The package this template belongs to.

    Returns:
        A subclass of ``PortexExternalType``.

    Examples:
        >>> vector_template = {
        ...     "type": "template",
        ...     "params": {
        ...         "dtype": {
        ...             "required": False,
        ...             "default": "int32",
        ...         },
        ...         "dimension": {
        ...             "required": False,
        ...             "default": "2D",
        ...             "options": ["2D", "3D"],
        ...         },
        ...     },
        ...     "declaration": {
        ...         "type": "record",
        ...         "fields": [
        ...             {
        ...                 "name": "x",
        ...                 "type": "$params.dtype"
        ...             },
        ...             {
        ...                 "name": "y",
        ...                 "type": "$params.dtype"
        ...             },
        ...             {
        ...                 "name": "z",
        ...                 "exist_if": "$params.dimension == '3D'",
        ...                 "type": "$params.dtype",
        ...             }
        ...         ]
        ...     }
        ... }
        >>> Vector = template("Vector", vector_template)
        >>> vector2d_int = Vector()
        >>> vector2d_int
        Vector
        >>> vector2d_int.internal_type
        record(
          fields={
            'x': int32(),
            'y': int32(),
          },
        )
        >>>
        >>> vector2d_float = Vector("float32")
        >>> vector2d_float
        Vector(
          dtype='float32',
        )
        >>> vector2d_float.internal_type
        record(
          fields={
            'x': float32(),
            'y': float32(),
          },
        )
        >>>
        >>> vector3d = Vector(dimension="3D")
        >>> vector3d
        Vector(
          dimension='3D',
        )
        >>> vector3d.internal_type
        record(
          fields={
            'x': int32(),
            'y': int32(),
            'z': int32(),
          },
        )

    """
    params_pyobj = content.get("params", {})
    decl = content["declaration"]

    imports = Imports.from_pyobj(content.get("imports", []))
    imports.update_base_package(package)

    factory = type_factory_creator(decl, imports)

    keys = factory.keys
    params = Params.from_pyobj(params_pyobj)

    for key, value in params.items():
        value.ptype = keys.get(key, PTYPE.Any)

    params.add(Param("nullable", False, ptype=PTYPE.Boolean))

    type_ = type(
        name,
        (PortexExternalType,),
        {"params": params, "dependences": factory.dependences, "factory": factory},
    )
    package[name] = type_
    return type_
