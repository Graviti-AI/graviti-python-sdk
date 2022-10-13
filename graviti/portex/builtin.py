#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
# pylint: disable=c-extension-no-member

"""The Portex builtin types."""


from typing import Iterable, Mapping, Optional, Tuple, Type, TypeVar, Union

import pyarrow as pa

from graviti.portex import ptype as PTYPE
from graviti.portex.base import PortexRecordBase, PortexType
from graviti.portex.enum import EnumValues
from graviti.portex.factory import ConnectedFieldsFactory
from graviti.portex.field import Fields
from graviti.portex.package import packages
from graviti.portex.param import Param, Params, param
from graviti.portex.register import PyArrowConversionRegister
from graviti.utility import ModuleMocker

try:
    # pylint: disable=import-error
    from zoneinfo import ZoneInfo as tz_checker
except ModuleNotFoundError:
    try:
        # pylint: disable=import-error
        from pytz import timezone as tz_checker  # type: ignore[import]
    except ModuleNotFoundError:
        tz_checker = ModuleMocker(
            "'pytz' package or 'zoneinfo' module (builtin module after python 3.9) is needed "
            "to support timezone"
        )


builtins = packages.builtins
_E = Union[int, float, str, bool, None]


class PortexBuiltinType(PortexType):  # pylint: disable=abstract-method
    """The base class of Portex builtin type."""

    _T = TypeVar("_T", bound="PortexBuiltinType")

    params = Params()
    packages = builtins

    def __init_subclass__(cls) -> None:
        params = Params(cls.params)
        for name in getattr(cls, "__annotations__", {}):
            parameter = getattr(cls, name, None)
            if isinstance(parameter, tuple):
                params.add(Param(name, *parameter))
                delattr(cls, name)

        cls.params = params

        builtins[cls.__name__] = cls

    def __init__(self, nullable: bool = False) -> None:
        self.nullable = PTYPE.Boolean.check(nullable)

    @classmethod
    def _from_pyarrow(cls: Type[_T], paarray: pa.Array) -> _T:
        return cls()

    def to_builtin(self: _T) -> _T:
        """Expand the top level type to Portex builtin type.

        Returns:
            The expanded Portex builtin type.

        """
        return self


@PyArrowConversionRegister(pa.lib.Type_STRING)
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

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return pa.string()


@PyArrowConversionRegister(pa.lib.Type_BINARY)
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

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return pa.binary()


@PyArrowConversionRegister(pa.lib.Type_BOOL)
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

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return pa.bool_()


@PyArrowConversionRegister(pa.lib.Type_INT32)
class int32(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex primitive type ``int32``.

    Arguments:
        nullable: Whether it is a nullable type.

    Examples:
        >>> t = int32()
        >>> t
        int32()

    """

    nullable: bool = param(False, ptype=PTYPE.Boolean)

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return pa.int32()


@PyArrowConversionRegister(pa.lib.Type_INT64)
class int64(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex primitive type ``int64``.

    Arguments:
        nullable: Whether it is a nullable type.

    Examples:
        >>> t = int64()
        >>> t
        int64()

    """

    nullable: bool = param(False, ptype=PTYPE.Boolean)

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return pa.int64()


@PyArrowConversionRegister(pa.lib.Type_FLOAT)
class float32(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex primitive type ``float32``.

    Arguments:
        nullable: Whether it is a nullable type.

    Examples:
        >>> t = float32()
        >>> t
        float32()

    """

    nullable: bool = param(False, ptype=PTYPE.Boolean)

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return pa.float32()


@PyArrowConversionRegister(pa.lib.Type_DOUBLE)
class float64(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex primitive type ``float64``.

    Arguments:
        nullable: Whether it is a nullable type.

    Examples:
        >>> t = float64()
        >>> t
        float64()

    """

    nullable: bool = param(False, ptype=PTYPE.Boolean)

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return pa.float64()


@PyArrowConversionRegister(pa.lib.Type_STRUCT)
class record(PortexBuiltinType, PortexRecordBase):  # pylint: disable=invalid-name
    """Portex complex type ``record``.

    Arguments:
        fields: The fields of the record.
        nullable: Whether it is a nullable type.

    Examples:
        Create a record by dict:

        >>> t = record({"f0": int32(), "f1": float32()})
        >>> t
        record(
          fields={
            'f0': int32(),
            'f1': float32(),
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

    _fields_factory = ConnectedFieldsFactory.from_parameter_name("fields")

    fields: Fields = param(ptype=PTYPE.Fields)
    nullable: bool = param(False, ptype=PTYPE.Boolean)

    def __init__(
        self,
        fields: Union[Iterable[Tuple[str, PortexType]], Mapping[str, PortexType]],
        nullable: bool = False,
    ) -> None:
        self.fields = PTYPE.Fields.check(fields)
        super().__init__(nullable=nullable)

    @classmethod
    def _from_pyarrow(cls: Type[_T], paarray: pa.StructArray) -> _T:
        return cls(
            (field.name, cls.from_pyarrow(paarray.field(field.name))) for field in paarray.type
        )

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow struct DataType.

        """
        return pa.struct(self.fields.to_pyarrow())  # pylint: disable=no-member


@PyArrowConversionRegister(pa.lib.Type_DICTIONARY)
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

    _T = TypeVar("_T", bound="enum")

    values: EnumValues = param(ptype=PTYPE.Enum)
    nullable: bool = param(False, ptype=PTYPE.Boolean)

    def __init__(self, values: Iterable[_E], nullable: bool = False) -> None:
        self.values = PTYPE.Enum.check(values)
        super().__init__(nullable=nullable)

    @classmethod
    def _from_pyarrow(cls: Type[_T], paarray: pa.DictionaryArray) -> _T:
        return cls(paarray.dictionary.to_pylist())

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        min_index, max_index = self.values.index_scope
        if min_index >= -128 and max_index <= 127:
            return pa.int8()

        if min_index >= -32768 and max_index <= 32767:
            return pa.int16()

        if min_index >= -2147483648 and max_index <= 2147483647:
            return pa.int32()

        return pa.int64()


@PyArrowConversionRegister(pa.lib.Type_LIST, pa.lib.Type_FIXED_SIZE_LIST)
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
        self.items = PTYPE.PortexType.check(items)
        self.length = PTYPE.Integer.check(length) if length is not None else None
        super().__init__(nullable=nullable)

    @classmethod
    def _from_pyarrow(cls: Type[_T], paarray: Union[pa.ListArray, pa.FixedSizeListArray]) -> _T:
        patype = paarray.type
        return cls(
            cls.from_pyarrow(paarray.values),
            getattr(patype, "list_size", None),
        )

    def _get_column_count(self) -> int:
        """Get the total column count of the portex type.

        Returns:
            The total column count.

        """
        return self.items._get_column_count()  # pylint: disable=protected-access

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        list_size = self.length if self.length else -1
        return pa.list_(self.items.to_pyarrow(), list_size)  # pylint: disable=no-member


@PyArrowConversionRegister(pa.lib.Type_DATE32)
class date(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex temporal type ``date``.

    Arguments:
        nullable: Whether it is a nullable type.

    Examples:
        >>> t = date()
        >>> t
        date()

    """

    _T = TypeVar("_T", bound="date")

    nullable: bool = param(False, ptype=PTYPE.Boolean)

    def __init__(self, nullable: bool = False) -> None:
        super().__init__(nullable=nullable)

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return pa.date32()


@PyArrowConversionRegister(pa.lib.Type_TIME32, pa.lib.Type_TIME64)
class time(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex temporal type ``time``.

    Arguments:
        unit: The unit of the time, supports 's', 'ms', 'us' and 'ns'.
        nullable: Whether it is a nullable type.

    Examples:
        >>> t = time("ms")
        >>> t
        times(
          unit='ms',
        )

    """

    _T = TypeVar("_T", bound="time")

    _UNIT_TO_TYPE = {
        "s": pa.time32("s"),
        "ms": pa.time32("ms"),
        "us": pa.time64("us"),
        "ns": pa.time64("ns"),
    }
    _TYPE_TO_UNIT = {value: key for key, value in _UNIT_TO_TYPE.items()}

    unit: str = param(ptype=PTYPE.String, options=["s", "ms", "us", "ns"])
    nullable: bool = param(False, ptype=PTYPE.Boolean)

    def __init__(self, unit: str, nullable: bool = False) -> None:
        self.unit = self.params["unit"].check(unit)
        super().__init__(nullable=nullable)

    @classmethod
    def _from_pyarrow(cls: Type[_T], paarray: Union[pa.Time32Array, pa.Time64Array]) -> _T:
        return cls(cls._TYPE_TO_UNIT[paarray.type])

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return self._UNIT_TO_TYPE[self.unit]


@PyArrowConversionRegister(pa.lib.Type_TIMESTAMP)
class timestamp(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex temporal type ``timestamp``.

    Arguments:
        unit: The unit of the timestamp, supports 's', 'ms', 'us' and 'ns'.
        tz: The name of the timezone, ``None`` indicates the timestamp is naive.
        nullable: Whether it is a nullable type.

    Examples:
        >>> t = timestamp("ms")
        >>> t
        timestamp(
          unit='ms',
        )
        >>>
        >>> t = timestamp("us", tz="UTC")
        >>> t
        timestamp(
          unit='ms',
          tz='UTC',
        )

    """

    _T = TypeVar("_T", bound="timestamp")

    unit: str = param(ptype=PTYPE.String, options=["s", "ms", "us", "ns"])
    tz: Optional[str] = param(None, ptype=PTYPE.String)
    nullable: bool = param(False, ptype=PTYPE.Boolean)

    def __init__(self, unit: str, tz: Optional[str] = None, nullable: bool = False) -> None:
        self.unit = self.params["unit"].check(unit)
        _tz = PTYPE.String.check(tz) if tz is not None else None
        if _tz is not None:
            tz_checker(_tz)

        self.tz = _tz

        super().__init__(nullable=nullable)

    @classmethod
    def _from_pyarrow(cls: Type[_T], paarray: pa.TimestampArray) -> _T:
        patype = paarray.type
        return cls(patype.unit, patype.tz)

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return pa.timestamp(self.unit, self.tz)


@PyArrowConversionRegister(pa.lib.Type_DURATION)
class timedelta(PortexBuiltinType):  # pylint: disable=invalid-name
    """Portex temporal type ``timedelta``.

    Arguments:
        unit: The unit of the timedelta, supports 's', 'ms', 'us' and 'ns'.
        nullable: Whether it is a nullable type.

    Examples:
        >>> t = timedelta("ms")
        >>> t
        timedelta(
          unit='ms',
        )

    """

    _T = TypeVar("_T", bound="timedelta")

    unit: str = param(ptype=PTYPE.String, options=["s", "ms", "us", "ns"])
    nullable: bool = param(False, ptype=PTYPE.Boolean)

    def __init__(self, unit: str, nullable: bool = False) -> None:
        self.unit = self.params["unit"].check(unit)
        super().__init__(nullable=nullable)

    @classmethod
    def _from_pyarrow(cls: Type[_T], paarray: pa.DurationArray) -> _T:
        return cls(paarray.type.unit)

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Returns:
            The corresponding builtin PyArrow DataType.

        """
        return pa.duration(self.unit)
