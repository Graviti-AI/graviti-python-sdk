#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The search operators for type inference."""

from typing import Any, Callable, Dict, List, TypeVar, Union

import graviti.portex as pt
from graviti.dataframe.sql.scalar import NUMERICAL_PRIORITIES
from graviti.exception import CriteriaError

_Expr = Union[str, int, float, bool, Dict[str, List[Any]]]
_Args = List[_Expr]

_Operator = Callable[[pt.PortexRecordBase, _Args], pt.PortexType]
_T = TypeVar("_T", bound=_Operator)

PYTHON_TYPE_TO_PORTEX_TYPE = {
    str: pt.string,
    bool: pt.boolean,
    float: pt.float64,
    int: pt.int64,
}

OPERATORS: Dict[str, _Operator] = {}


class OperatorRegister:
    """The class decorator to connect operator name and operator function.

    Arguments:
        name: The name of the operator.

    """

    def __init__(self, *names: str) -> None:
        self._names = names

    def __call__(self, operator: _T) -> _T:
        """Connect data container with the portex types.

        Arguments:
            operator: The operator function needs to be regisitered.

        Returns:
            The input operator function unchanged.

        """
        for name in self._names:
            OPERATORS[name] = operator

        return operator


def infer_type(schema: pt.PortexRecordBase, expr: _Expr) -> pt.PortexType:
    """Infer portex type from the Graviti criteria expr.

    Arguments:
        schema: The schema of the sheet.
        expr: The Graviti criteria expr.

    Returns:
        The result portex type inferred from the expr.

    Raises:
        CriteriaError: When the operator is not supported.

    """
    if isinstance(expr, str) and expr.startswith("$."):
        return get_type(schema, expr)

    if isinstance(expr, dict):
        name, args = expr.copy().popitem()
        try:
            operator = OPERATORS[name]
        except KeyError as error:
            raise CriteriaError(f"Operator '{name}' is not supported") from error

        return operator(schema, args)

    return PYTHON_TYPE_TO_PORTEX_TYPE[type(expr)]()


def get_type(schema: pt.PortexRecordBase, expr: str) -> pt.PortexType:
    """Get portex type from the Graviti criteria expr.

    Arguments:
        schema: The schema of the sheet.
        expr: The Graviti criteria expr.

    Returns:
        The result portex type inferred from the expr.

    Raises:
        CriteriaError: When the portex type cannot be get from the expr.

    """
    names = expr[2:].split(".")

    sub_schema = schema
    for name in names[:-1]:
        sub_schema = sub_schema[name]  # type: ignore[assignment]
        if not isinstance(sub_schema, pt.PortexRecordBase):
            raise CriteriaError(f"Failed to get portex type from expression '{expr}'")

    return sub_schema[names[-1]]


@OperatorRegister("$filter")
def _filter(schema: pt.PortexRecordBase, args: _Args) -> pt.PortexType:
    arg = args[0]
    if isinstance(arg, str) and arg.startswith("$."):
        return get_type(schema, arg)

    raise CriteriaError("The first argument of $filter should be an array")


@OperatorRegister("$add", "$sub", "$mult", "$mod", "$pow")
def _arithmetic(schema: pt.PortexRecordBase, args: _Args) -> pt.PortexType:
    pttypes = (infer_type(schema, arg) for arg in args)
    return max((NUMERICAL_PRIORITIES[type(pttype)], pttype) for pttype in pttypes)[1]


# pylint: disable=unused-argument
@OperatorRegister("$div")
def _div(schema: pt.PortexRecordBase, args: _Args) -> pt.PortexType:
    return pt.float64()


@OperatorRegister(
    "$or", "$and", "$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$any_match", "$all_match"
)
def _boolean(schema: pt.PortexRecordBase, expr: _Args) -> pt.PortexType:
    return pt.boolean()
