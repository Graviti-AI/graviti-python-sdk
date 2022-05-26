#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Template base class."""


from inspect import Signature
from typing import Any, ClassVar, Dict, List, Optional, Tuple, Type

import pyarrow as pa

import graviti.portex.ptype as PTYPE
from graviti.portex.base import PortexType
from graviti.portex.builtin import PortexBuiltinType
from graviti.portex.factory import Factory, type_factory_creator
from graviti.portex.package import ExternalPackage, Imports, Package, packages
from graviti.portex.param import Param, Params
from graviti.portex.register import ExternalContainerRegister

EXTERNAL_TYPE_TO_CONTAINER = ExternalContainerRegister.EXTERNAL_TYPE_TO_CONTAINER


class PortexExternalType(PortexType):  # pylint: disable=abstract-method
    """The base class of Portex external type."""

    _signature: ClassVar[Signature]

    package: ClassVar[ExternalPackage]
    factory: ClassVar[Factory]

    def __init_subclass__(cls) -> None:
        cls._signature = cls.params.get_signature()

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
        return self.factory(**arguments)  # type: ignore[no-any-return]

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return self.internal_type.to_pyarrow()

    def to_builtin(self) -> PortexBuiltinType:
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
        >>> import graviti.portex as pt
        >>> from graviti.portex.template import template
        >>>
        >>> vector_template = {
        ...     "type": "template",
        ...     "parameters": [
        ...         {
        ...             "name": "coords",
        ...             "default": {"type": "int32"},
        ...         },
        ...         {
        ...             "name": "labels",
        ...             "default": None,
        ...         },
        ...     ],
        ...     "declaration": {
        ...         "type": "record",
        ...         "fields": [
        ...             {
        ...                 "name": "x",
        ...                 "+": "$coords",
        ...             },
        ...             {
        ...                 "name": "y",
        ...                 "+": "$coords",
        ...             },
        ...             {
        ...                 "name": "label",
        ...                 "exist_if": "$labels",
        ...                 "type": "enum",
        ...                 "values": "$labels",
        ...             },
        ...         ],
        ...     },
        ... }
        >>> Vector = template("Vector", vector_template)
        >>> vector2d_int = Vector()
        >>> vector2d_int
        Vector()
        >>> vector2d_int.internal_type
        record(
          fields={
            'x': int32(),
            'y': int32(),
          },
        )
        >>>
        >>> vector2d_float = Vector(pt.float32())
        >>> vector2d_float
        Vector(
          coords=float32(),
        )
        >>> vector2d_float.internal_type
        record(
          fields={
            'x': float32(),
            'y': float32(),
          },
        )
        >>>
        >>> labeled_vector = Vector(labels=["visble", "occluded"])
        >>> labeled_vector
        Vector(
          labels=['visble', 'occluded'],
        )
        >>> labeled_vector.internal_type
        record(
          fields={
            'x': int32(),
            'y': int32(),
            'label': enum(
              values=['visble', 'occluded'],
            ),
          },
        )

    """
    params_pyobj = content.get("parameters", [])
    decl = content["declaration"]

    imports = Imports.from_pyobj(content.get("imports", []))
    imports.update_base_package(package)

    factory = type_factory_creator(decl, imports)

    keys = factory.keys
    params = Params.from_pyobj(params_pyobj)

    for key, value in params.items():
        value.ptype = keys.get(key, PTYPE.Any)

    params.add(Param("nullable", False, ptype=PTYPE.Boolean))

    type_: Type[PortexExternalType] = type(
        name,
        (PortexExternalType,),
        {"params": params, "factory": factory},
    )

    if isinstance(package, ExternalPackage):
        container = EXTERNAL_TYPE_TO_CONTAINER.get((package.url, package.revision, name))
        if container:
            type_.container = container

    package[name] = type_

    return type_
