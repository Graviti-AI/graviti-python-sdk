#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""The Portex builtin types."""


from functools import reduce
from operator import mul
from typing import Any, Iterable, List, Mapping, Optional, Tuple, Type, TypeVar, Union

import pyarrow as pa

import graviti.portex.ptype as PTYPE
from graviti.portex.base import PortexType
from graviti.portex.field import Fields
from graviti.portex.package import packages
from graviti.portex.param import Param, Params, param
from graviti.portex.register import PyArrowConversionRegister

builtins = packages.builtins

_PYTHON_TYPE_TO_PYARROW_TYPE = {
    int: pa.int64(),
    float: pa.float64(),
    bool: pa.bool_(),
    str: pa.string(),
    bytes: pa.binary(),
    None: pa.null(),
}


class PortexBuiltinType(PortexType):  # pylint: disable=abstract-method
    """The base class of Portex builtin type."""

    _T = TypeVar("_T", bound="PortexBuiltinType")

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

    @classmethod
    def _from_pyarrow(cls: Type[_T], pyarrow_type: pa.DataType) -> _T:
        return cls()

    def to_builtin(self: _T) -> _T:
        """Expand the top level type to Portex builtin type.

        Returns:
            The expanded Portex builtin type.

        """
        return self


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


@PyArrowConversionRegister(pa.lib.Type_STRING)  # pylint: disable=c-extension-no-member
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


@PyArrowConversionRegister(pa.lib.Type_BINARY)  # pylint: disable=c-extension-no-member
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


@PyArrowConversionRegister(pa.lib.Type_BOOL)  # pylint: disable=c-extension-no-member
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


@PyArrowConversionRegister(pa.lib.Type_INT32)  # pylint: disable=c-extension-no-member
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


@PyArrowConversionRegister(pa.lib.Type_INT64)  # pylint: disable=c-extension-no-member
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


@PyArrowConversionRegister(pa.lib.Type_FLOAT)  # pylint: disable=c-extension-no-member
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


@PyArrowConversionRegister(pa.lib.Type_DOUBLE)  # pylint: disable=c-extension-no-member
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


@PyArrowConversionRegister(pa.lib.Type_STRUCT)  # pylint: disable=c-extension-no-member
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

    _T = TypeVar("_T", bound="record")

    fields: Fields = param(ptype=PTYPE.Fields)
    nullable: bool = param(False, ptype=PTYPE.Boolean)

    def __init__(
        self,
        fields: Union[Iterable[Tuple[str, PortexType]], Mapping[str, PortexType]],
        nullable: bool = False,
    ) -> None:
        super().__init__(fields=fields, nullable=nullable)

    @classmethod
    def _from_pyarrow(cls: Type[_T], pyarrow_type: pa.StructType) -> _T:
        return cls((field.name, cls.from_pyarrow(field.type)) for field in pyarrow_type)

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return pa.struct(self.fields.to_pyarrow())  # pylint: disable=no-member

    def get_keys(self, type_name: Optional[str] = None) -> List[Tuple[str, ...]]:
        """Get the keys to locate all data, or only get keys of one type if type_name is given.

        Arguments:
            type_name: The name of the target PortexType.

        Returns:
            A list of keys to locate the data.

        """
        keys: List[Tuple[str, ...]] = []
        for name, portex_type in self.fields.items():  # pylint: disable=no-member
            key = (name,)
            if type_name and portex_type.__class__.__name__ == type_name:
                keys.append(key)
                continue

            subkeys = portex_type.get_keys(type_name)
            if subkeys:
                for subkey in subkeys:
                    keys.append(key + subkey)
                continue

            if not type_name:
                keys.append(key)

        return keys


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

        Raises:
            NotImplementedError: When the values have more than one types.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        pytype = None
        for value in self.values:
            if value is None:
                self.nullable = True
                continue

            value_type = type(value)
            if value_type != pytype and pytype is not None:
                raise NotImplementedError("Values with more than one type is not supported yet")

            pytype = value_type

        return pa.dictionary(pa.int32(), _PYTHON_TYPE_TO_PYARROW_TYPE[pytype])


@PyArrowConversionRegister(
    pa.lib.Type_LIST, pa.lib.Type_FIXED_SIZE_LIST  # pylint: disable=c-extension-no-member
)
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

    _T = TypeVar("_T", bound="array")

    items: PortexType = param(ptype=PTYPE.PortexType)
    length: Optional[int] = param(None, ptype=PTYPE.Integer)
    nullable: bool = param(False, ptype=PTYPE.Boolean)

    def __init__(
        self, items: PortexType, length: Optional[int] = None, nullable: bool = False
    ) -> None:
        super().__init__(items=items, length=length, nullable=nullable)

    @classmethod
    def _from_pyarrow(cls: Type[_T], pyarrow_type: Union[pa.ListType, pa.FixedSizeListType]) -> _T:
        return cls(
            cls.from_pyarrow(pyarrow_type.value_type),
            getattr(pyarrow_type, "list_size", None),
        )

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
