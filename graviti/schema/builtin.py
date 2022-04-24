#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""The Portex builtin types."""


from functools import reduce
from operator import mul
from typing import Any, Iterable, List, Mapping, Optional, Tuple, Union

import pyarrow as pa

import graviti.schema.ptype as PTYPE
from graviti.schema.base import PortexType
from graviti.schema.field import Fields
from graviti.schema.package import packages
from graviti.schema.param import Param, Params, param

builtins = packages.builtins

_PYTHON_TYPE_TO_PYARROW_TYPE = {
    int: pa.int64(),
    float: pa.float64(),
    bool: pa.bool_(),
    str: pa.string(),
    bytes: pa.binary(),
}


class PortexBuiltinType(PortexType):  # pylint: disable=abstract-method
    """The base class of Portex builtin type."""

    params = Params()

    def __init_subclass__(cls) -> None:
        params = Params(cls.params)
        for name in getattr(cls, "__annotations__", {}):
            parameter = getattr(cls, name, None)
            if isinstance(parameter, tuple):
                params.add(Param(name, *parameter))
                delattr(cls, name)

        cls.params = params
        builtins[cls.__name__] = cls

    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            kwargs[key] = self.params[key].check(value)

        super().__init__(**kwargs)


class PortexNumericType(PortexBuiltinType):  # pylint: disable=abstract-method
    """The base class of the Portex numeric types.

    Arguments:
        minimum: The minimum value.
        maximum: The maximum value.
        nullable: Whether it is a nullable type.

    """

    minimum: Optional[float] = param(None, ptype=PTYPE.Number)
    maximum: Optional[float] = param(None, ptype=PTYPE.Number)
    nullable: bool = param(False, ptype=PTYPE.Boolean)

    def __init__(
        self,
        minimum: Optional[float] = None,
        maximum: Optional[float] = None,
        nullable: bool = False,
    ) -> None:
        super().__init__(minimum=minimum, maximum=maximum, nullable=nullable)


class string(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex primitive type ``string``.

    Arguments:
        nullable: Whether it is a nullable type.

    Examples:
        >>> t = string()
        >>> t
        string()

    """

    nullable: bool = param(False, ptype=PTYPE.Boolean)

    def __init__(self, nullable: bool = False) -> None:
        super().__init__(nullable=nullable)

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return pa.string()


class binary(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex primitive type ``binary``.

    Arguments:
        nullable: Whether it is a nullable type.

    Examples:
        >>> t = binary()
        >>> t
        binary()

    """

    nullable: bool = param(False, ptype=PTYPE.Boolean)

    def __init__(self, nullable: bool = False) -> None:
        super().__init__(nullable=nullable)

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return pa.binary()


class boolean(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex primitive type ``boolean``.

    Arguments:
        nullable: Whether it is a nullable type.

    Examples:
        >>> t = boolean()
        >>> t
        boolean()

    """

    nullable: bool = param(False, ptype=PTYPE.Boolean)

    def __init__(self, nullable: bool = False) -> None:
        super().__init__(nullable=nullable)

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return pa.bool_()


class int32(PortexNumericType):  # pylint: disable=invalid-name
    """Portex primitive type ``int32``.

    Examples:
        >>> t = int32(0, 100)
        >>> t
        int32(
          minimum=0,
          maximum=100,
        )

    """

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return pa.int32()


class int64(PortexNumericType):  # pylint: disable=invalid-name
    """Portex primitive type ``int64``.

    Examples:
        >>> t = int64(0, 100)
        >>> t
        int64(
          minimum=0,
          maximum=100,
        )

    """

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return pa.int64()


class float32(PortexNumericType):  # pylint: disable=invalid-name
    """Portex primitive type ``float32``.

    Examples:
        >>> t = float32(0, 100)
        >>> t
        float32(
          minimum=0,
          maximum=100,
        )

    """

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return pa.float32()


class float64(PortexNumericType):  # pylint: disable=invalid-name
    """Portex primitive type ``float64``.

    Examples:
        >>> t = float64(0, 100)
        >>> t
        float64(
          minimum=0,
          maximum=100,
        )

    """

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return pa.float64()


class record(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex complex type ``record``.

    Arguments:
        fields: The fields of the record.
        nullable: Whether it is a nullable type.

    Examples:
        Create a record by dict:

        >>> t = record({"f0": int32(), "f1": float32(0, 100)})
        >>> t
        record(
          fields={
            'f0': int32(),
            'f1': float32(
              minimum=0,
              maximum=100,
            ),
          },
        )

        Create a record by tuple list:

        >>> t = record([("f0", string()), ("f1", enum(["v0", "v1"]))])
        >>> t
        record(
          fields={
            'f0': string(),
            'f1': enum(
              values=['v0', 'v1'],
            ),
          },
        )

    """

    fields: Fields = param(ptype=PTYPE.Fields)
    nullable: bool = param(False, ptype=PTYPE.Boolean)

    def __init__(
        self,
        fields: Union[Iterable[Tuple[str, PortexType]], Mapping[str, PortexType]],
        nullable: bool = False,
    ) -> None:
        super().__init__(fields=fields, nullable=nullable)

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return pa.struct(self.fields.to_pyarrow())  # pylint: disable=no-member


class enum(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex complex type ``enum``.

    Arguments:
        values: The values of the enum members.
        nullable: Whether it is a nullable type.

    Examples:
        >>> t = enum(["v0", "v1"])
        >>> t
        enum(
          values=['v0', 'v1'],
        )

    """

    values: List[Any] = param(ptype=PTYPE.Array)
    nullable: bool = param(False, ptype=PTYPE.Boolean)

    def __init__(self, values: List[Any], nullable: bool = False) -> None:
        super().__init__(values=values, nullable=nullable)

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        pytypes = {type(value) for value in self.values}

        patype = (
            _PYTHON_TYPE_TO_PYARROW_TYPE[pytypes.pop()]
            if len(pytypes) == 1
            else pa.union(
                (
                    pa.field(str(index), _PYTHON_TYPE_TO_PYARROW_TYPE[pytype])
                    for index, pytype in enumerate(pytypes)
                ),
                "sparse",
            )
        )
        return pa.dictionary(pa.int32(), patype)


class array(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex complex type ``array``.

    Arguments:
        items: The item type of the array.
        length: The length of the array.
        nullable: Whether it is a nullable type.

    Examples:
        >>> t = array(int32(0), 100)
        >>> t
        array(
          items=int32(
            minimum=0,
          ),
          length=100,
        )

    """

    items: PortexType = param(ptype=PTYPE.PortexType)
    length: Optional[int] = param(None, ptype=PTYPE.Integer)
    nullable: bool = param(False, ptype=PTYPE.Boolean)

    def __init__(
        self, items: PortexType, length: Optional[int] = None, nullable: bool = False
    ) -> None:
        super().__init__(items=items, length=length, nullable=nullable)

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        list_size = self.length if self.length else -1
        return pa.list_(self.items.to_pyarrow(), list_size)  # pylint: disable=no-member


class tensor(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex complex type ``tensor``.

    Arguments:
        shape: The shape of the tensor.
        dtype: The dtype of the tensor.
        nullable: Whether it is a nullable type.

    Examples:
        >>> t = tensor((3, 3), "float64")
        >>> t
        tensor(
          shape=(3, 3),
          dtype='float64',
        )

    """

    shape: Tuple[Optional[int], ...] = param(ptype=PTYPE.Array)
    dtype: str = param(ptype=PTYPE.TypeName)
    nullable: bool = param(False, ptype=PTYPE.Boolean)

    def __init__(self, shape: Iterable[Optional[int]], dtype: str, nullable: bool = False) -> None:
        super().__init__(shape=shape, dtype=dtype, nullable=nullable)
        self.shape = tuple(shape)

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        try:
            list_size = reduce(mul, self.shape)
        except TypeError:
            list_size = -1
        return pa.list_(self.imports[self.dtype]().to_pyarrow(), list_size=list_size)
