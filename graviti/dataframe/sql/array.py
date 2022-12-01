#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the search related array."""

from typing import Any, Callable, ClassVar, Dict, Type, TypeVar

import pyarrow as pa

import graviti.portex as pt
from graviti.dataframe.sql.container import _E, ArrayContainer, SearchContainerRegister
from graviti.dataframe.sql.scalar import (
    NUMERICAL_PRIORITIES,
    BooleanScalar,
    DateScalar,
    EnumScalar,
    NumberScalar,
    RowSeries,
    StringScalar,
    TimedeltaScalar,
    TimeScalar,
    TimestampScalar,
)

_LOM = TypeVar("_LOM", bound="LogicalOperatorsMixin")
_EOM = TypeVar("_EOM", bound="EqualOperatorsMixin")
_COM = TypeVar("_COM", bound="ComparisonOperatorsMixin")
_AOM = TypeVar("_AOM", bound="ArithmeticOperatorsMixin")
_NA = TypeVar("_NA", bound="NumberArray")
_EA = TypeVar("_EA", bound="EnumArray")
_TAB = TypeVar("_TAB", bound="TemporalArrayBase")  # pylint: disable=invalid-name


class LogicalOperatorsMixin(ArrayContainer):
    """A mixin for dynamically implementing logical operators."""

    _LOCICAL_OPERATORS: ClassVar[Dict[str, str]] = {
        "__and__": "and",
        "__or__": "or",
    }

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        for meth, opt in cls._LOCICAL_OPERATORS.items():
            setattr(cls, meth, cls._get_logical_operator(opt))

    @classmethod
    def _get_logical_operator(cls: Type[_LOM], opt: str) -> Callable[[_LOM, Any], "BooleanArray"]:
        def func(self: _LOM, other: Any) -> "BooleanArray":
            other_expr = other.expr if isinstance(other, ArrayContainer) else other
            expr = {f"${opt}": [self.expr, other_expr]}

            return BooleanArray(expr, pt.boolean(), self.upper_expr)

        return func


class EqualOperatorsMixin(ArrayContainer):
    """A mixin for dynamically implementing euqal operators."""

    _EQUAL_OPERATORS: ClassVar[Dict[str, str]] = {
        "__eq__": "eq",
        "__ne__": "ne",
    }

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        for meth, opt in cls._EQUAL_OPERATORS.items():
            setattr(cls, meth, cls._get_equal_operator(opt))

    @classmethod
    def _get_equal_operator(cls: Type[_EOM], opt: str) -> Callable[[_EOM, Any], "BooleanArray"]:
        def func(self: _EOM, other: Any) -> "BooleanArray":
            other_expr = other.expr if isinstance(other, ArrayContainer) else other
            expr = {f"${opt}": [self.expr, other_expr]}

            return BooleanArray(expr, pt.boolean(), self.upper_expr)

        return func


class ComparisonOperatorsMixin(ArrayContainer):
    """A mixin for dynamically implementing comparison operators."""

    _COMPARISON_OPERATORS: ClassVar[Dict[str, str]] = {
        "__gt__": "gt",
        "__ge__": "gte",
        "__lt__": "lt",
        "__le__": "lte",
    }

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        for meth, opt in cls._COMPARISON_OPERATORS.items():
            setattr(cls, meth, cls._get_comparison_operator(opt))

    @classmethod
    def _get_comparison_operator(
        cls: Type[_COM], opt: str
    ) -> Callable[[_COM, Any], "BooleanArray"]:
        def func(self: _COM, other: Any) -> "BooleanArray":
            if isinstance(other, ArrayContainer):
                if not isinstance(other, type(self)):
                    raise TypeError(
                        f"Invalid '{opt}' operation between {self.schema} and {other.schema}"
                    )
                other_expr = other.expr
            else:
                other_expr = other

            expr = {f"${opt}": [self.expr, other_expr]}
            return BooleanArray(expr, pt.boolean(), self.upper_expr)

        return func


class ArithmeticOperatorsMixin(ArrayContainer):
    """A mixin for dynamically implementing arithmetic operators."""

    _ARITHMETIC_OPERATORS: ClassVar[Dict[str, str]] = {
        "__div__": "div",
        "__mod__": "mod",
        "__pow__": "pow",
        "__sub__": "sub",
        "__mul__": "mult",
        "__add__": "add",
    }

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        for meth, opt in cls._ARITHMETIC_OPERATORS.items():
            setattr(cls, meth, cls._get_arithmetic_operator(opt))

    @classmethod
    def _get_arithmetic_operator(cls: Type[_AOM], opt: str) -> Callable[[_AOM, Any], Any]:
        raise NotImplementedError


class Array(ArrayContainer):
    """One-dimensional array for portex builtin type array."""

    prefix = "$."

    def query(self, func: Callable[[Any], Any]) -> "Array":
        """Query the data of an ArraySeries with a lambda function.

        Arguments:
            func: The query function.

        Returns:
            The ArraySeries with the query expression.

        """
        item_container = self.schema.search_container.item_container
        expr = {
            "$filter": [
                self.upper_expr,
                func(item_container.from_upper(self.expr, self.schema)).expr,
            ]
        }

        return Array(expr, self.schema, self.upper_expr)

    def any(self) -> BooleanScalar:
        """Whether any element is True.

        Returns:
            The BooleanSeries with the any expression.

        """
        expr = {"$any_match": [self.upper_expr, self.expr]}
        return BooleanScalar(expr)

    def all(self) -> BooleanScalar:
        """Whether all elements are True.

        Returns:
            The BooleanSeries with the all expression.

        """
        expr = {"$all_match": [self.upper_expr, self.expr]}
        return BooleanScalar(expr)


@SearchContainerRegister(pt.boolean)
class BooleanArray(Array, LogicalOperatorsMixin, EqualOperatorsMixin):
    """One-dimensional array for portex builtin type array with the boolean items."""

    item_container = BooleanScalar


@SearchContainerRegister(pt.string)
class StringArray(Array, LogicalOperatorsMixin, EqualOperatorsMixin):
    """One-dimensional array for portex builtin type array with the string and enum items."""

    item_container = StringScalar


@SearchContainerRegister(pt.enum)
class EnumArray(Array, EqualOperatorsMixin):
    """One-dimensional array for portex builtin type array with the string and enum items."""

    item_container = EnumScalar

    @classmethod
    def _get_equal_operator(cls: Type[_EA], opt: str) -> Callable[[_EA, Any], "BooleanArray"]:
        def func(self: _EA, other: Any) -> "BooleanArray":
            if isinstance(other, ArrayContainer):
                other_expr: Any = other.expr
            else:
                enum: pt.enum = self.schema.to_builtin()  # type: ignore[assignment]
                other_expr = enum.values.value_to_index[other]

            expr = {f"${opt}": [self.expr, other_expr]}
            return BooleanArray(expr, pt.boolean(), self.upper_expr)

        return func


class TemporalArrayBase(Array, EqualOperatorsMixin, ComparisonOperatorsMixin):
    """One-dimensional array for portex builtin temporal types."""

    def _time_to_int(self, value: Any) -> int:
        return pa.scalar(value, self.schema.to_pyarrow()).value  # type: ignore[no-any-return]

    @classmethod
    def _get_equal_operator(cls: Type[_TAB], opt: str) -> Callable[[_TAB, Any], "BooleanArray"]:
        def func(self: _TAB, other: Any) -> "BooleanArray":
            if isinstance(other, ArrayContainer):
                other_expr: Any = other.expr
            else:
                other_expr = self._time_to_int(other)  # pylint: disable=protected-access

            expr = {f"${opt}": [self.expr, other_expr]}
            return BooleanArray(expr, pt.boolean(), self.upper_expr)

        return func

    @classmethod
    def _get_comparison_operator(
        cls: Type[_TAB], opt: str
    ) -> Callable[[_TAB, Any], "BooleanArray"]:
        def func(self: _TAB, other: Any) -> "BooleanArray":
            if isinstance(other, ArrayContainer):
                if not isinstance(other, type(self)):
                    raise TypeError(
                        f"Invalid '{opt}' operation between {self.schema} and {other.schema}"
                    )
                other_expr: Any = other.expr
            else:
                other_expr = self._time_to_int(other)  # pylint: disable=protected-access

            expr = {f"${opt}": [self.expr, other_expr]}
            return BooleanArray(expr, pt.boolean(), self.upper_expr)

        return func


@SearchContainerRegister(pt.date)
class DateArray(TemporalArrayBase):
    """One-dimensional array for portex builtin date type."""

    item_container = DateScalar

    def _time_to_int(self, value: Any) -> int:
        scalar = pa.scalar(value, self.schema.to_pyarrow())
        return scalar.cast(pa.int32()).as_py()  # type: ignore[no-any-return]


@SearchContainerRegister(pt.time)
class TimeArray(TemporalArrayBase):
    """One-dimensional array for portex builtin time type."""

    item_container = TimeScalar


@SearchContainerRegister(pt.timestamp)
class TimestampArray(TemporalArrayBase):
    """One-dimensional array for portex builtin timestamp type."""

    item_container = TimestampScalar


@SearchContainerRegister(pt.timedelta)
class TimedeltaArray(TemporalArrayBase):
    """One-dimensional array for portex builtin timedelta type."""

    item_container = TimedeltaScalar


@SearchContainerRegister(
    pt.float32,
    pt.float64,
    pt.int32,
    pt.int64,
)
class NumberArray(Array, ComparisonOperatorsMixin, ArithmeticOperatorsMixin):
    """One-dimensional array for portex builtin type array with the numerical items."""

    item_container = NumberScalar

    @classmethod
    def _get_arithmetic_operator(cls: Type[_NA], opt: str) -> Callable[[_NA, Any], _NA]:
        def func(self: _NA, other: Any) -> _NA:
            if isinstance(other, ArrayContainer):
                if not isinstance(other, type(self)):
                    raise TypeError(
                        f"Invalid '{opt}' operation between {self.schema} and {other.schema}"
                    )
                other_expr = other.expr
                schema = (
                    self.schema
                    if NUMERICAL_PRIORITIES[self.schema.__class__]
                    > NUMERICAL_PRIORITIES[other.schema.__class__]
                    else other.schema
                )
            else:
                other_expr = other
                schema = self.schema

            expr = {f"${opt}": [self.expr, other_expr]}
            return cls(expr, schema, self.upper_expr)

        return func

    def _check_upper_expr(self, opt: str) -> None:
        if self.expr != self.prefix:
            raise TypeError(f"{opt} operation only support for numerical array.")

    def size(self) -> NumberScalar:
        """Get the length of array series.

        Returns:
            The NumberScalar with the size expression.

        """
        self._check_upper_expr("size")
        return NumberScalar({"$size": [self.upper_expr]}, self.schema)

    def max(self) -> "NumberScalar":
        """Get the max value of array series.

        Returns:
            The NumberScalar with the max expression.

        """
        self._check_upper_expr("max")
        return NumberScalar({"$max": [self.upper_expr]}, self.schema)

    def min(self) -> NumberScalar:
        """Get the min value of array series.

        Returns:
            The NumberScalar with the min expression.

        """
        self._check_upper_expr("min")
        return NumberScalar({"$min": [self.upper_expr]}, self.schema)

    def sum(self) -> NumberScalar:
        """Get the sum of array series.

        Returns:
            The NumberScalar with the sum expression.

        """
        self._check_upper_expr("sum")
        return NumberScalar({"$sum": [self.upper_expr]}, self.schema)


@SearchContainerRegister(pt.record)
class DataFrame(ArrayContainer):
    """The Two-dimensional array for the search."""

    prefix = "$"
    item_container = RowSeries
    schema: pt.PortexRecordBase

    def __getitem__(self, key: str) -> ArrayContainer:
        field = self.schema[key]
        return field.search_container(f"{self.expr}.{key}", field, self.upper_expr)

    def query(self, func: Callable[[Any], Any]) -> "DataFrame":
        """Query the data of an ArraySeries with a lambda function.

        Arguments:
            func: The query function.

        Returns:
            The DataFrame with the query expression.

        """
        expr = {"$filter": [self.upper_expr, func(RowSeries(self.schema)).expr]}
        return DataFrame(expr, self.schema, self.upper_expr)


class ArrayDistributor(ArrayContainer):
    """A distributor to instance DataFrame, ArrayScalar by different array items."""

    @classmethod
    def from_upper(cls, expr: _E, schema: pt.PortexType) -> "ArrayContainer":
        """Instantiate a Search object from the upper level.

        Arguments:
            expr: The expression of the search.
            schema: The schema of the series.

        Returns:
            The loaded object.

        """
        items: pt.PortexType = schema.to_builtin().items  # type: ignore[attr-defined]
        return items.search_container.from_upper(expr, items)


@SearchContainerRegister(pt.array)
class ArraySeries(ArrayContainer):
    """The One-dimensional array for the search."""

    item_container = ArrayDistributor
