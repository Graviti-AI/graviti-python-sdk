#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Template base class."""


from inspect import Signature
from typing import Any, Dict, Type

from graviti.schema.base import Param, PortexType, TypeRegister
from graviti.schema.builtin import Fields
from graviti.schema.factory import Dynamic, type_factory_creator


class PortexExternalType(PortexType):
    """The base class of Portex external type."""

    internal_type: PortexType


def template(name: str, content: Dict[str, Any]) -> Type[PortexType]:
    """Generate a Portex external type according to the input template.

    Arguments:
        name: The class name of the Portex external type.
        content: A dict indicates a Portex template.

    Returns:
        A subclass of ``PortexExternalType``.

    Examples:
        >>> vector2d_template = {
        ...     "type": "template",
        ...     "params": {
        ...         "dtype": {
        ...             "required": False,
        ...             "default": "int",
        ...         }
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
        ...             }
        ...         ]
        ...     }
        ... }
        >>> Vector2D = template("Vector2D", vector2d_template)
        >>> vector2d_int = Vector2D()
        >>> vector2d_int
        Vector2D
        >>> vector2d_int.internal_type
        record(
          fields={
            'x': int_(),
            'y': int_(),
          },
        )
        >>>
        >>> vector2d_float = Vector2D("float")
        Vector2D(
          dtype='float',
        )
        >>> vector2d_float.internal_type
        record(
          fields={
            'x': float_(),
            'y': float_(),
          },
        )

    """
    params = content["params"]
    decl = content["declaration"]

    factory = type_factory_creator(decl)
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
            if type_ == Fields:
                arguments[key] = Fields(arguments[key])

        self.__dict__.update(arguments)

    type_ = type(name, (PortexExternalType,), {"params": parameters, "__init__": __init__})
    TypeRegister(name)(type_)
    return type_
