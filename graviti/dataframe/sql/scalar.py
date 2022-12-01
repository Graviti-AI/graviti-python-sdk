#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the search related Scalar."""

from typing import Any, Callable, ClassVar, Dict, Type, TypeVar, Union

import pyarrow as pa

import graviti.portex as pt
from graviti.dataframe.sql.container import _E, ArrayContainer, ScalarContainer

NUMERICAL_PRIORITIES: Dict[Type[pt.PortexType], int] = {
    pt.int32: 0,
    pt.int64: 1,
    pt.float32: 2,
    pt.float64: 3,
}

_LOM = TypeVar("_LOM", bound="LogicalOperatorsMixin")
_EOM = TypeVar("_EOM", bound="EqualOperatorsMixin")
_COM = TypeVar("_COM", bound="ComparisonOperatorsMixin")
_AOM = TypeVar("_AOM", bound="ArithmeticOperatorsMixin")
_NS = TypeVar("_NS", bound="NumberScalar")
_ES = TypeVar("_ES", bound="EnumScalar")
_TSB = TypeVar("_TSB", bound="TemporalScalarBase")  # pylint: disable=invalid-name


class LogicalOperatorsMixin(ScalarContainer):
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
    def _get_logical_operator(cls: Type[_LOM], opt: str) -> Callable[[_LOM, Any], "BooleanScalar"]:
        def func(self: _LOM, other: Any) -> "BooleanScalar":
            other_expr = other.expr if isinstance(other, ScalarContainer) else other
            expr = {f"${opt}": [self.expr, other_expr]}

            return BooleanScalar(expr)

        return func


class EqualOperatorsMixin(ScalarContainer):
    """A mixin for dynamically implementing equal operators."""

    _LOCICAL_OPERATORS: ClassVar[Dict[str, str]] = {
        "__eq__": "eq",
        "__ne__": "ne",
    }

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        for meth, opt in cls._LOCICAL_OPERATORS.items():
            setattr(cls, meth, cls._get_equal_operator(opt))

    @classmethod
    def _get_equal_operator(cls: Type[_EOM], opt: str) -> Callable[[_EOM, Any], "BooleanScalar"]:
        def func(self: _EOM, other: Any) -> "BooleanScalar":
            other_expr = other.expr if isinstance(other, ScalarContainer) else other
            expr = {f"${opt}": [self.expr, other_expr]}

            return BooleanScalar(expr)

        return func


class ComparisonOperatorsMixin(ScalarContainer):
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
    ) -> Callable[[_COM, Any], "BooleanScalar"]:
        def func(self: _COM, other: Any) -> "BooleanScalar":
            if isinstance(other, ScalarContainer):
                if not isinstance(other, type(self)):
                    raise TypeError(
                        f"Invalid '{opt}' operation between {self.schema} and {other.schema}"
                    )
                other_expr = other.expr
            else:
                other_expr = other

            expr = {f"${opt}": [self.expr, other_expr]}
            return BooleanScalar(expr)

        return func


class ArithmeticOperatorsMixin(ScalarContainer):
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
            setattr(cls, meth, cls._get_arithemtic_operator(opt))

    @classmethod
    def _get_arithemtic_operator(cls: Type[_AOM], opt: str) -> Callable[[_AOM, Any], Any]:
        raise NotImplementedError


class NumberScalar(
    LogicalOperatorsMixin, EqualOperatorsMixin, ComparisonOperatorsMixin, ArithmeticOperatorsMixin
):
    """One-dimensional array for numerical portex builtin type."""

    @classmethod
    def _get_arithemtic_operator(cls: Type[_NS], opt: str) -> Callable[[_NS, Any], _NS]:
        def func(self: _NS, other: Any) -> _NS:
            if isinstance(other, ScalarContainer):
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
            return cls(expr, schema)

        return func


class BooleanScalar(LogicalOperatorsMixin, EqualOperatorsMixin):
    """One-dimensional array for portex builtin type boolean."""

    def __init__(self, expr: _E) -> None:
        super().__init__(expr, pt.boolean())


class StringScalar(LogicalOperatorsMixin, EqualOperatorsMixin):
    """One-dimensional array for portex builtin type string."""


class EnumScalar(EqualOperatorsMixin):
    """One-dimensional array for portex builtin type enum."""

    @classmethod
    def _get_equal_operator(cls: Type[_ES], opt: str) -> Callable[[_ES, Any], "BooleanScalar"]:
        def func(self: _ES, other: Any) -> "BooleanScalar":
            if isinstance(other, ScalarContainer):
                other_expr: Any = other.expr
            else:
                enum: pt.enum = self.schema.to_builtin()  # type: ignore[assignment]
                other_expr = enum.values.value_to_index[other]

            expr = {f"${opt}": [self.expr, other_expr]}
            return BooleanScalar(expr)

        return func


class TemporalScalarBase(EqualOperatorsMixin, ComparisonOperatorsMixin):
    """One-dimensional array for portex builtin temporal type."""

    def _time_to_int(self, value: Any) -> int:
        return pa.scalar(value, self.schema.to_pyarrow()).value  # type: ignore[no-any-return]

    @classmethod
    def _get_equal_operator(cls: Type[_TSB], opt: str) -> Callable[[_TSB, Any], "BooleanScalar"]:
        def func(self: _TSB, other: Any) -> "BooleanScalar":
            if isinstance(other, ScalarContainer):
                other_expr: Any = other.expr
            else:
                other_expr = self._time_to_int(other)  # pylint: disable=protected-access

            expr = {f"${opt}": [self.expr, other_expr]}
            return BooleanScalar(expr)

        return func

    @classmethod
    def _get_comparison_operator(
        cls: Type[_TSB], opt: str
    ) -> Callable[[_TSB, Any], "BooleanScalar"]:
        def func(self: _TSB, other: Any) -> "BooleanScalar":
            if isinstance(other, ScalarContainer):
                if not isinstance(other, type(self)):
                    raise TypeError(
                        f"Invalid '{opt}' operation between {self.schema} and {other.schema}"
                    )
                other_expr: Any = other.expr
            else:
                other_expr = self._time_to_int(other)  # pylint: disable=protected-access

            expr = {f"${opt}": [self.expr, other_expr]}
            return BooleanScalar(expr)

        return func


class DateScalar(TemporalScalarBase):
    """One-dimensional array for portex builtin date type."""

    def _time_to_int(self, value: Any) -> int:
        scalar = pa.scalar(value, self.schema.to_pyarrow())
        return scalar.cast(pa.int32()).as_py()  # type: ignore[no-any-return]


class TimeScalar(TemporalScalarBase):
    """One-dimensional array for portex builtin time type."""


class TimestampScalar(TemporalScalarBase):
    """One-dimensional array for portex builtin timestamp type."""


class TimedeltaScalar(TemporalScalarBase):
    """One-dimensional array for portex builtin timedelta type."""


class RowSeries(ScalarContainer):
    """The One-dimensional array for the search."""

    schema: pt.PortexRecordBase

    def __init__(self, schema: pt.PortexRecordBase) -> None:
        super().__init__("$", schema)

    def __getitem__(self, key: str) -> Union[ArrayContainer, ScalarContainer]:
        field = self.schema[key]
        return field.search_container.item_container.from_upper(f"{self.expr}.{key}", field)
