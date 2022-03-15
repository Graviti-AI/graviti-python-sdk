#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Template base class."""


from inspect import Signature
from typing import Any, ClassVar, Dict, Set, Type

from graviti.schema.base import Param, PortexType
from graviti.schema.builtin import Fields
from graviti.schema.factory import Dynamic, type_factory_creator
from graviti.schema.package import ExternalPackage, Imports, Package, packages


class PortexExternalType(PortexType):
    """The base class of Portex external type."""

    internal_type: PortexType
    dependences: ClassVar[Set[PortexType]]
    package: ClassVar[ExternalPackage]


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
        ...                 "existIf": "$params.dimension == '3D'",
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
    try:
        params = content["params"]
        decl = content["declaration"]
    except KeyError:
        params = {}
        decl = content

    factory = type_factory_creator(decl, Imports(package))
    keys = factory.keys

    parameters = []
    for key, value in params.items():
        parameters.append(
            Param(
                key,
                value.get("default", Param.empty),
                value.get("options", Param.empty),
                annotation=keys.get(key, Param.empty),
            )
        )

    signature = Signature(parameters)

    def __init__(self: PortexExternalType, *args: Any, **kwargs: Any) -> None:
        bound_arguments = signature.bind(*args, **kwargs)
        bound_arguments.apply_defaults()
        arguments = bound_arguments.arguments

        self.internal_type = factory(**arguments)

        for key, value in factory.keys.items():
            type_ = value if not isinstance(value, Dynamic) else value(**arguments)
            if type_ == Fields and arguments[key] is not None:
                arguments[key] = Fields(arguments[key])

        self.__dict__.update(arguments)

    type_ = type(
        name,
        (PortexExternalType,),
        {"params": parameters, "dependences": factory.dependences, "__init__": __init__},
    )
    package[name] = type_
    return type_
