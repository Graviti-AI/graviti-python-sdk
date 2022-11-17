#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The search container and register."""

from typing import Any, ClassVar, Dict, Type, TypeVar, Union

import graviti.portex as pt

_S = TypeVar("_S", bound="ScalarContainer")
_A = TypeVar("_A", bound="ArrayContainer")
_E = Union[str, Dict[str, Any]]


class ScalarContainer:
    """The base class for the search scalar container.

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


class ArrayContainer:
    """The base class for the search array container.

    Arguments:
        expr: The expression of the search.
        schema: The schema of the series.
        upper_expr: The expression of the search.

    """

    prefix: str
    item_container: ClassVar[Type[Union[ScalarContainer, "ArrayContainer"]]]

    def __init__(self, expr: _E, schema: pt.PortexType, upper_expr: _E) -> None:
        self.expr = expr
        self.schema = schema
        self.upper_expr = upper_expr

    @classmethod
    def from_upper(cls, expr: _E, schema: pt.PortexType) -> "ArrayContainer":
        """Instantiate a Search object from the upper level.

        Arguments:
            expr: The upper expression of the search.
            schema: The schema of the series.

        Returns:
            The loaded object.

        """
        return cls(cls.prefix, schema, expr)


class SearchContainerRegister:
    """The class decorator to connect portex type and the search array container.

    Arguments:
        portex_types: The portex types needs to be connected.

    """

    def __init__(self, *portex_types: Type[pt.PortexType]) -> None:
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
