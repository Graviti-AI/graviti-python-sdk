#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the search related Scalar."""

from typing import Any, Callable, ClassVar, Dict, Type, TypeVar, Union

import graviti.portex as pt
from graviti.dataframe.sql.container import _E, ArrayContainer, ScalarContainer

NUMERICAL_PRIORITY: Dict[Type[pt.PortexType], int] = {
    pt.int32: 0,
    pt.int64: 1,
    pt.float32: 2,
    pt.float64: 3,
}

_LOM = TypeVar("_LOM", bound="LogicalOperatorsMixin")
_COM = TypeVar("_COM", bound="ComparisonOperatorsMixin")
_AOM = TypeVar("_AOM", bound="ArithmeticOperatorsMixin")
_NS = TypeVar("_NS", bound="NumberScalar")


class LogicalOperatorsMixin(ScalarContainer):
    """A mixin for dynamically implementing logical operators."""

    _LOCICAL_OPERATORS: ClassVar[Dict[str, str]] = {
        "__eq__": "eq",
        "__ne__": "ne",
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
            if isinstance(other, ScalarContainer):
                expr = {f"${opt}": [self.expr, other.expr]}
            else:
                expr = {f"${opt}": [self.expr, other]}

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
                expr = {f"${opt}": [self.expr, other.expr]}
            else:
                expr = {f"${opt}": [self.expr, other]}

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


class NumberScalar(LogicalOperatorsMixin, ComparisonOperatorsMixin, ArithmeticOperatorsMixin):
    """One-dimensional array for numerical portex builtin type."""

    @classmethod
    def _get_arithemtic_operator(cls: Type[_NS], opt: str) -> Callable[[_NS, Any], _NS]:
        def func(self: _NS, other: Any) -> _NS:
            if isinstance(other, ScalarContainer):
                if not isinstance(other, type(self)):
                    raise TypeError(
                        f"Invalid '{opt}' operation between {self.schema} and {other.schema}"
                    )
                expr = {f"${opt}": [self.expr, other.expr]}
            else:
                expr = {f"${opt}": [self.expr, other]}

            schema = (
                self.schema
                if NUMERICAL_PRIORITY[self.schema.__class__]
                > NUMERICAL_PRIORITY[other.schema.__class__]
                else other.schema
            )

            return cls(expr, schema)

        return func


class BooleanScalar(LogicalOperatorsMixin):
    """One-dimensional array for portex builtin type boolean."""

    def __init__(self, expr: _E) -> None:
        super().__init__(expr, pt.boolean())


class StringScalar(LogicalOperatorsMixin):
    """One-dimensional array for portex builtin type string."""


class EnumScalar(ScalarContainer):
    """One-dimensional array for portex builtin type enum."""

    def __eq__(self, other: Any) -> BooleanScalar:  # type: ignore[override]
        if isinstance(other, ScalarContainer):
            expr = {"$eq": [self.expr, other.expr]}
        else:
            values = self.schema.to_builtin().values  # type: ignore[attr-defined]
            if other in values:
                other = values.index(other)
            expr = {"$eq": [self.expr, other]}

        return BooleanScalar(expr)

    def __ne__(self, other: Any) -> BooleanScalar:  # type: ignore[override]
        if isinstance(other, ScalarContainer):
            expr = {"$ne": [self.expr, other.expr]}
        else:
            values = self.schema.to_builtin().values  # type: ignore[attr-defined]
            if other in values:
                other = values.index(other)
            expr = {"$ne": [self.expr, other]}

        return BooleanScalar(expr)


class RowSeries(ScalarContainer):
    """The One-dimensional array for the search."""

    schema: pt.PortexRecordBase

    def __init__(self, schema: pt.PortexRecordBase) -> None:
        super().__init__("$", schema)

    def __getitem__(self, key: str) -> Union[ArrayContainer, ScalarContainer]:
        field = self.schema[key]
        return field.search_container.item_container.from_upper(f"{self.expr}.{key}", field)
