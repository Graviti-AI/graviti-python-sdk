#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the search related Scalar."""

from typing import Any, Callable, Dict, Type, TypeVar, Union

import graviti.portex as pt
from graviti.dataframe.sql.container import SearchContainer, SearchScalarContainer

NUMERICAL_PRIORITY = {pt.int32: 0, pt.int64: 1, pt.float32: 2, pt.float64: 3}

_LOM = TypeVar("_LOM", bound="LogicalOperatorsMixin")
_CAOM = TypeVar("_CAOM", bound="ComparisonArithmeticOperatorsMixin")
_NS = TypeVar("_NS", bound="NumberScalar")


class LogicalOperatorsMixin:
    """A mixin for dynamically implementing logical operators."""

    _logical_operators: Dict[str, str] = {
        "__eq__": "eq",
        "__ne__": "ne",
        "__and__": "and",
        "__or__": "or",
    }
    expr: Union[str, Dict[str, Any]]
    schema: pt.PortexType

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        for meth, opt in cls._logical_operators.items():
            setattr(cls, meth, cls._get_logical_operator(opt))

    @classmethod
    def _get_logical_operator(cls: Type[_LOM], opt: str) -> Callable[[_LOM, Any], "BooleanScalar"]:
        def func(self: _LOM, other: Any) -> "BooleanScalar":
            if isinstance(other, SearchScalarContainer):
                expr = {f"${opt}": [self.expr, other.expr]}
            else:
                expr = {f"${opt}": [self.expr, other]}

            return BooleanScalar(expr)

        return func


class ComparisonArithmeticOperatorsMixin:
    """A mixin for dynamically implementing comparison and arithmetic operators."""

    expr: Union[str, Dict[str, Any]]
    schema: pt.PortexType
    _comparison_operators: Dict[str, str] = {
        "__gt__": "gt",
        "__ge__": "gte",
        "__lt__": "lt",
        "__le__": "lte",
    }
    _numerical_opeartors: Dict[str, str] = {
        "__div__": "div",
        "__mod__": "mod",
        "__pow__": "pow",
        "__sub__": "sub",
        "__mul__": "mult",
        "__add__": "add",
    }

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        for meth, opt in cls._comparison_operators.items():
            setattr(cls, meth, cls._get_comparison_operator(opt))
        for meth, opt in cls._numerical_opeartors.items():
            setattr(cls, meth, cls._get_numeric_operator(opt))

    @classmethod
    def _get_comparison_operator(cls: Type[_CAOM], opt: str) -> Callable[[_CAOM, Any], Any]:
        raise NotImplementedError

    @classmethod
    def _get_numeric_operator(cls: Type[_CAOM], opt: str) -> Callable[[_CAOM, Any], Any]:
        raise NotImplementedError

    def check_type_for_other(self, other: SearchScalarContainer, opt: str) -> None:
        """Check whether the other series is the same type as self series.

        Arguments:
            other: The needed check series.
            opt: Name of opt.

        Raises:
            TypeError: When the right series type is different from left.

        """
        if not isinstance(other, self.__class__):
            raise TypeError(f"Invalid {opt} operation between {self.schema} and {other.schema}")


class NumberScalar(
    SearchScalarContainer,
    LogicalOperatorsMixin,
    ComparisonArithmeticOperatorsMixin,
):
    """One-dimensional array for numerical portex builtin type."""

    schema: Union[pt.int32, pt.int64, pt.float32, pt.float64]

    @classmethod
    def _get_comparison_operator(cls: Type[_NS], opt: str) -> Callable[[_NS, Any], "BooleanScalar"]:
        def func(self: _NS, other: Any) -> "BooleanScalar":
            if isinstance(other, SearchScalarContainer):
                self.check_type_for_other(other, opt)
                expr = {f"${opt}": [self.expr, other.expr]}
            else:
                expr = {f"${opt}": [self.expr, other]}

            return BooleanScalar(expr)

        return func

    @classmethod
    def _get_numeric_operator(cls: Type[_NS], opt: str) -> Callable[[_NS, Any], "NumberScalar"]:
        def func(self: _NS, other: Any) -> "NumberScalar":
            if isinstance(other, SearchScalarContainer):
                self.check_type_for_other(other, opt)
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


class BooleanScalar(SearchScalarContainer, LogicalOperatorsMixin):
    """One-dimensional array for portex builtin type boolean."""

    def __init__(self, expr: Union[str, Dict[str, Any]]) -> None:
        super().__init__(expr, pt.boolean())


class StringScalar(SearchScalarContainer, LogicalOperatorsMixin):
    """One-dimensional array for portex builtin type string."""


class EnumScalar(SearchScalarContainer):
    """One-dimensional array for portex builtin type enum."""

    def __eq__(self, other: Any) -> BooleanScalar:  # type: ignore[override]
        if isinstance(other, SearchScalarContainer):
            expr = {"$eq": [self.expr, other.expr]}
        else:
            values = self.schema.to_builtin().values  # type: ignore[attr-defined]
            if other in values:
                other = values.index(other)
            expr = {"$eq": [self.expr, other]}

        return BooleanScalar(expr)

    def __ne__(self, other: Any) -> BooleanScalar:  # type: ignore[override]
        if isinstance(other, SearchScalarContainer):
            expr = {"$ne": [self.expr, other.expr]}
        else:
            values = self.schema.to_builtin().values  # type: ignore[attr-defined]
            if other in values:
                other = values.index(other)
            expr = {"$ne": [self.expr, other]}

        return BooleanScalar(expr)


class RowSeries(SearchScalarContainer):
    """The One-dimensional array for the search."""

    schema: pt.PortexRecordBase

    def __init__(self, schema: pt.PortexRecordBase) -> None:
        super().__init__("$", schema)

    def __getitem__(self, key: str) -> Union[SearchContainer, SearchScalarContainer]:
        field = self.schema[key]
        return field.search_container.item_container.from_upper(f"{self.expr}.{key}", field)
