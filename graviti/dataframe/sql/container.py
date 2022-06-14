#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The search container and register."""

from typing import TYPE_CHECKING, Any, Dict, Type, TypeVar, Union

import graviti.portex as pt

if TYPE_CHECKING:
    from graviti.portex.base import PortexType


_S = TypeVar("_S", bound="SearchScalarContainer")
_A = TypeVar("_A", bound="SearchContainer")
_E = Union[str, Dict[str, Any]]


class SearchScalarContainer:
    """The base class for the search container.

    Arguments:
        expr: The expression of the search.
        schema: The schema of the series.

    """

    expr: _E
    schema: pt.PortexType

    def __init__(self, expr: _E, schema: pt.PortexType) -> None:
        self.expr = expr
        self.schema = schema

    @classmethod
    def from_upper(cls: Type[_S], expr: _E, schema: pt.PortexType) -> _S:
        """Instantiate a Search object from the upper level.

        Arguments:
            expr: The expression of the search.
            schema: The schema of the series.

        Returns:
            The loaded object.

        """
        obj: _S = object.__new__(cls)
        obj.expr = expr
        obj.schema = schema
        return obj


class SearchContainer:
    """The base class for the search array container.

    Arguments:
        expr: The expression of the search.
        schema: The schema of the series.
        upper_expr: The expression of the search.

    """

    prefix: str
    item_container: Type[Union[SearchScalarContainer, "SearchContainer"]]

    def __init__(self, expr: _E, schema: pt.PortexType, upper_expr: _E) -> None:
        self.expr = expr
        self.schema = schema
        self.upper_expr = upper_expr

    @classmethod
    def from_upper(cls, expr: _E, schema: pt.PortexType) -> "SearchContainer":
        """Instantiate a Search object from the upper level.

        Arguments:
            expr: The upper expression of the search.
            schema: The schema of the series.

        Returns:
            The loaded object.

        """
        obj: "SearchContainer" = object.__new__(cls)
        obj.expr = cls.prefix
        obj.schema = schema
        obj.upper_expr = expr
        return obj


class SearchContainerRegister:
    """The class decorator to connect portex type and the search array container.

    Arguments:
        portex_types: The portex types needs to be connected.

    """

    def __init__(self, *portex_types: Type["PortexType"]) -> None:
        self._portex_types = portex_types

    def __call__(self, container: Type[_A]) -> Type[_A]:
        """Connect search array container with the portex types.

        Arguments:
            container: The search array container needs to be connected.

        Returns:
            The input container class unchanged.

        """
        for portex_type in self._portex_types:
            portex_type.search_container = container

        return container
