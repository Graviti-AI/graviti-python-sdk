#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the search related array."""

from typing import Any, Callable, Dict, Union

import graviti.portex as pt
from graviti.dataframe.sql.container import (
    SearchContainer,
    SearchContainerRegister,
    SearchScalarContainer,
)
from graviti.dataframe.sql.scalar import (
    BooleanScalar,
    ComparisonArithmeticOperatorsMixin,
    EnumScalar,
    LogicalOperatorsMixin,
    NumberScalar,
    RowSeries,
    StringScalar,
)


class ScalarArray(SearchContainer, LogicalOperatorsMixin):
    """One-dimensional array for portex builtin type array."""

    prefix = "$."

    @classmethod
    def _get_logical_operator(  # type: ignore[override]
        cls, opt: str
    ) -> Callable[["ScalarArray", Any], "ScalarArray"]:
        def func(self: "ScalarArray", other: Any) -> "ScalarArray":
            if isinstance(other, SearchScalarContainer):
                expr = {f"${opt}": [self.expr, other.expr]}
            else:
                expr = {f"${opt}": [self.expr, other]}
            return cls(expr, pt.boolean(), self.upper_expr)

        return func

    def query(self, func: Callable[[Any], Any]) -> "ScalarArray":
        """Query the data of an ArraySeries with a lambda function.

        Arguments:
            func: The query function.

        Returns:
            The ArraySeries with the query expression.

        """
        expr = {
            "$filter": [
                self.upper_expr,
                func(
                    self.schema.search_container.item_container.from_upper(self.expr, self.schema)
                ).expr,
            ]
        }
        return ScalarArray(expr, self.schema, self.upper_expr)

    def any(self) -> "BooleanScalar":
        """Whether any element is True.

        Returns:
            The BooleanSeries with the any expression.

        """
        expr = {"$any_match": [self.upper_expr, self.expr]}
        return BooleanScalar(expr)


@SearchContainerRegister(pt.boolean)
class BooleanScalarArray(ScalarArray, LogicalOperatorsMixin):
    """One-dimensional array for portex builtin type array with the boolean items."""

    item_container = BooleanScalar


@SearchContainerRegister(pt.string)
class StringScalarArray(ScalarArray, LogicalOperatorsMixin):
    """One-dimensional array for portex builtin type array with the string and enum items."""

    item_container = StringScalar


@SearchContainerRegister(pt.enum)
class EnumScalarArray(ScalarArray, LogicalOperatorsMixin):
    """One-dimensional array for portex builtin type array with the string and enum items."""

    item_container = EnumScalar

    def __eq__(self, other: Any) -> BooleanScalarArray:  # type: ignore[override]
        if isinstance(other, SearchScalarContainer):
            expr = {"$eq": [self.expr, other.expr]}
        else:
            values = self.schema.to_builtin().values  # type: ignore[attr-defined]
            if other in values:
                other = values.index(other)
            expr = {"$eq": [self.expr, other]}
        return BooleanScalarArray(expr, pt.boolean(), self.upper_expr)

    def __ne__(self, other: Any) -> BooleanScalarArray:  # type: ignore[override]
        if isinstance(other, SearchScalarContainer):
            expr = {"$ne": [self.expr, other.expr]}
        else:
            values = self.schema.to_builtin().values  # type: ignore[attr-defined]
            if other in values:
                other = values.index(other)
            expr = {"$ne": [self.expr, other]}
        return BooleanScalarArray(expr, pt.boolean(), self.upper_expr)


@SearchContainerRegister(
    pt.float32,
    pt.float64,
    pt.int32,
    pt.int64,
)
class NumberScalarArray(ScalarArray, ComparisonArithmeticOperatorsMixin):
    """One-dimensional array for portex builtin type array with the numerical items."""

    item_container = NumberScalar

    @classmethod
    def _get_comparison_operator(cls, opt: str) -> Callable[..., ScalarArray]:
        def func(self: "NumberScalarArray", other: Any) -> ScalarArray:
            if isinstance(other, SearchScalarContainer):
                self.check_type_for_other(other, opt)
                expr = {f"${opt}": [self.expr, other.expr]}
            else:
                expr = {f"${opt}": [self.expr, other]}

            return ScalarArray(expr, pt.array(pt.boolean()), self.upper_expr)

        return func

    @classmethod
    def _get_numeric_operator(cls, opt: str) -> Callable[..., "NumberScalarArray"]:
        def func(self: "NumberScalarArray", other: Any) -> "NumberScalarArray":
            if isinstance(other, SearchScalarContainer):
                self.check_type_for_other(other, opt)
                expr = {f"${opt}": [self.expr, other.expr]}
            else:
                expr = {f"${opt}": [self.expr, other]}

            return NumberScalarArray(expr, self.schema, self.upper_expr)

        return func

    def _check_upper_expr(self, opt: str) -> None:
        if self.expr != self.prefix:
            raise TypeError(f"{opt} operation only support for numerical array.")

    def size(self) -> "NumberScalar":
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

    def min(self) -> "NumberScalar":
        """Get the min value of array series.

        Returns:
            The NumberScalar with the min expression.

        """
        self._check_upper_expr("min")
        return NumberScalar({"$min": [self.upper_expr]}, self.schema)

    def sum(self) -> "NumberScalar":
        """Get the sum of array series.

        Returns:
            The NumberScalar with the sum expression.

        """
        self._check_upper_expr("sum")
        return NumberScalar({"$sum": [self.upper_expr]}, self.schema)


@SearchContainerRegister(pt.record)
class DataFrame(SearchContainer):
    """The Two-dimensional array for the search."""

    prefix = "$"
    item_container = RowSeries
    schema: pt.PortexRecordBase

    def __getitem__(self, key: str) -> SearchContainer:
        field = self.schema[key]
        expr = f"{self.expr}.{key}"
        return field.search_container(expr, field, self.upper_expr)

    def query(self, func: Callable[[Any], Any]) -> "DataFrame":
        """Query the data of an ArraySeries with a lambda function.

        Arguments:
            func: The query function.

        Returns:
            The DataFrame with the query expression.

        """
        expr = {"$filter": [self.upper_expr, func(RowSeries(self.schema)).expr]}
        return DataFrame(expr, self.schema, self.upper_expr)


class ArrayDistributor(SearchContainer):
    """A distributor to instance DataFrame, ArrayScalar by different array items."""

    @classmethod
    def from_upper(
        cls, expr: Union[str, Dict[str, Any]], schema: pt.PortexType
    ) -> "SearchContainer":
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
class ArraySeries(SearchContainer):
    """The One-dimensional array for the search."""

    item_container = ArrayDistributor
