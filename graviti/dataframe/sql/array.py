#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the search related array."""

from typing import Any, Callable, ClassVar, Dict, Type, TypeVar

import graviti.portex as pt
from graviti.dataframe.sql.container import (
    _E,
    ArrayContainer,
    ScalarContainer,
    SearchContainerRegister,
)
from graviti.dataframe.sql.scalar import (
    BooleanScalar,
    ComparisonArithmeticOperatorsMixin,
    EnumScalar,
    NumberScalar,
    RowSeries,
    StringScalar,
)

_LOM = TypeVar("_LOM", bound="LogicalOperatorsMixin")
_NSA = TypeVar("_NSA", bound="NumberArray")


class LogicalOperatorsMixin(ArrayContainer):
    """A mixin for dynamically implementing logical operators."""

    _LOCICAL_OPERATORS: ClassVar[Dict[str, str]] = {
        "__eq__": "eq",
        "__ne__": "ne",
        "__and__": "and",
        "__or__": "or",
    }
    expr: _E
    schema: pt.PortexType

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        for meth, opt in cls._LOCICAL_OPERATORS.items():
            setattr(cls, meth, cls._get_logical_operator(opt))

    @classmethod
    def _get_logical_operator(cls: Type[_LOM], opt: str) -> Callable[[_LOM, Any], "BooleanArray"]:
        def func(self: _LOM, other: Any) -> "BooleanArray":
            if isinstance(other, ScalarContainer):
                expr = {f"${opt}": [self.expr, other.expr]}
            else:
                expr = {f"${opt}": [self.expr, other]}

            return BooleanArray(expr, pt.boolean(), self.upper_expr)

        return func


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
class BooleanArray(Array, LogicalOperatorsMixin):
    """One-dimensional array for portex builtin type array with the boolean items."""

    item_container = BooleanScalar


@SearchContainerRegister(pt.string)
class StringArray(Array, LogicalOperatorsMixin):
    """One-dimensional array for portex builtin type array with the string and enum items."""

    item_container = StringScalar


@SearchContainerRegister(pt.enum)
class EnumArray(Array):
    """One-dimensional array for portex builtin type array with the string and enum items."""

    item_container = EnumScalar

    def __eq__(self, other: Any) -> BooleanArray:  # type: ignore[override]
        if isinstance(other, ScalarContainer):
            expr = {"$eq": [self.expr, other.expr]}
        else:
            values = self.schema.to_builtin().values  # type: ignore[attr-defined]
            if other in values:
                other = values.index(other)
            expr = {"$eq": [self.expr, other]}

        return BooleanArray(expr, pt.boolean(), self.upper_expr)

    def __ne__(self, other: Any) -> BooleanArray:  # type: ignore[override]
        if isinstance(other, ScalarContainer):
            expr = {"$ne": [self.expr, other.expr]}
        else:
            values = self.schema.to_builtin().values  # type: ignore[attr-defined]
            if other in values:
                other = values.index(other)
            expr = {"$ne": [self.expr, other]}

        return BooleanArray(expr, pt.boolean(), self.upper_expr)


@SearchContainerRegister(
    pt.float32,
    pt.float64,
    pt.int32,
    pt.int64,
)
class NumberArray(Array, ComparisonArithmeticOperatorsMixin):
    """One-dimensional array for portex builtin type array with the numerical items."""

    item_container = NumberScalar

    @classmethod
    def _get_comparison_operator(cls: Type[_NSA], opt: str) -> Callable[[_NSA, Any], Array]:
        def func(self: _NSA, other: Any) -> Array:
            if isinstance(other, ScalarContainer):
                self.check_type_for_other(other, opt)
                expr = {f"${opt}": [self.expr, other.expr]}
            else:
                expr = {f"${opt}": [self.expr, other]}

            return Array(expr, pt.array(pt.boolean()), self.upper_expr)

        return func

    @classmethod
    def _get_numeric_operator(cls: Type[_NSA], opt: str) -> Callable[[_NSA, Any], "NumberArray"]:
        def func(self: _NSA, other: Any) -> "NumberArray":
            if isinstance(other, ScalarContainer):
                self.check_type_for_other(other, opt)
                expr = {f"${opt}": [self.expr, other.expr]}
            else:
                expr = {f"${opt}": [self.expr, other]}

            return NumberArray(expr, self.schema, self.upper_expr)

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
