#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""The Portex builtin types."""


from typing import Any, Iterable, List, Mapping, Optional, Sequence, Tuple, Union, overload

import graviti.schema.ptype as PTYPE
from graviti.schema.base import PortexType
from graviti.schema.field import Field, Fields
from graviti.schema.package import packages
from graviti.schema.param import Param, Params, param

builtins = packages.builtins


class PortexBuiltinType(PortexType):
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

    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            kwargs[key] = self.params[key].check(value)

        super().__init__(**kwargs)


class PortexNumericType(PortexBuiltinType):
    """The base class of the Portex numeric types.

    Arguments:
        minimum: The minimum value.
        maximum: The maximum value.

    """

    minimum: Optional[float] = param(None, ptype=PTYPE.Number)
    maximum: Optional[float] = param(None, ptype=PTYPE.Number)

    def __init__(self, minimum: Optional[float] = None, maximum: Optional[float] = None) -> None:
        super().__init__(minimum=minimum, maximum=maximum)


@builtins("string")
class string(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex primitive type ``string``.

    Examples:
        >>> t = string()
        >>> t
        string()

    """


@builtins("bytes")
class bytes_(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex primitive type ``bytes``.

    Examples:
        >>> t = bytes_()
        >>> t
        bytes_()

    """


@builtins("boolean")
class boolean(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex primitive type ``boolean``.

    Examples:
        >>> t = boolean()
        >>> t
        boolean()

    """


@builtins("int32")
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


@builtins("int64")
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


@builtins("float32")
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


@builtins("float64")
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


@builtins("record")
class record(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex complex type ``record``.

    Arguments:
        fields: The fields of the record.

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

        Create a record by :class:`Field` list:

        >>> t = record([Field("f0", float64(0)), Field("f1", array(int64()))])
        >>> t
        record(
          fields={
            'f0': float64(
              minimum=0,
            ),
            'f1': array(
              items=int64(),
            ),
          },
        )

    """

    fields: Fields = param(ptype=PTYPE.Fields)

    def __init__(
        self,
        fields: Union[Sequence[Union[Field, Tuple[str, PortexType]]], Mapping[str, PortexType]],
    ) -> None:
        super().__init__(fields=fields)

    @overload
    def __getitem__(self, index: Union[int, str]) -> PortexType:
        ...

    @overload
    def __getitem__(self, index: slice) -> List[PortexType]:
        ...

    def __getitem__(self, index: Union[int, str, slice]) -> Union[PortexType, List[PortexType]]:
        if isinstance(index, slice):
            return [field.type for field in self.fields[index]]

        if isinstance(index, str):
            for field in self.fields:
                if index == field.name:
                    return field.type

            raise KeyError(index)

        return self.fields[index].type


@builtins("enum")
class enum(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex complex type ``enum``.

    Arguments:
        values: The values of the enum members.

    Examples:
        >>> t = enum(["v0", "v1"])
        >>> t
        enum(
          values=['v0', 'v1'],
        )

    """

    values: List[Any] = param(ptype=PTYPE.Array)

    def __init__(self, values: List[Any]) -> None:
        super().__init__(values=values)


@builtins("array")
class array(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex complex type ``array``.

    Arguments:
        items: The item type of the array.
        length: The length of the array.

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

    def __init__(self, items: PortexType, length: Optional[int] = None) -> None:
        super().__init__(items=items, length=length)


@builtins("tensor")
class tensor(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex complex type ``tensor``.

    Arguments:
        shape: The shape of the tensor.
        dtype: The dtype of the tensor.

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

    def __init__(self, shape: Iterable[Optional[int]], dtype: str) -> None:
        super().__init__(shape=shape, dtype=dtype)
        self.shape = tuple(shape)
