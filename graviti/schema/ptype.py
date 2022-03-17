#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Parameter type releated classes."""


from typing import TYPE_CHECKING
from typing import Any as TypingAny
from typing import Sequence, Type, Union

from graviti.schema.base import PortexType as ClassPortexType
from graviti.schema.field import Field as ClassField
from graviti.schema.field import Fields as ClassFields

if TYPE_CHECKING:
    from graviti.schema.factory import Dynamic


class ParameterType:
    """The base class of parameter type."""

    @staticmethod
    def check(_: TypingAny) -> TypingAny:
        """Check the parameter type.

        Arguments:
            _: The argument which needs to be checked.

        Raises:
            NotImplementedError: The check method in base class should never be called.

        """
        raise NotImplementedError


PType = Union[Type[ParameterType], "Dynamic"]


class Any(ParameterType):
    """Unconstrained parameter type."""

    @staticmethod
    def check(arg: TypingAny) -> TypingAny:
        """Check the parameter type.

        Arguments:
            arg: The argument which needs to be checked.

        Returns:
            The input argument unchanged.

        """
        return arg


class Array(ParameterType):
    """Parameter type for JSON Array."""

    @staticmethod
    def check(arg: TypingAny) -> Sequence[TypingAny]:
        """Check the parameter type.

        Arguments:
            arg: The argument which needs to be checked.

        Returns:
            The input argument unchanged.

        Raises:
            TypeError: When the input argument is not a JSON array (Sequence in python).

        """
        if not isinstance(arg, Sequence):
            raise TypeError("Argument should be a Sequence")

        return arg


class Field(ParameterType):
    """Parameter type for Portex record field."""

    @staticmethod
    def check(arg: TypingAny) -> ClassField:
        """Check and transfer the parameter type.

        Arguments:
            arg: The argument which needs to be checked.

        Returns:
            A :class:`Field` instance created by the input argument.

        """
        return ClassField(*arg)


class Fields(ParameterType):
    """Parameter type for Portex record fields."""

    @staticmethod
    def check(arg: TypingAny) -> ClassFields:
        """Check and transfer the parameter type.

        Arguments:
            arg: The argument which needs to be checked.

        Returns:
            A :class:`Fields` instance created by the input argument.

        """
        return ClassFields(arg)


class Number(ParameterType):
    """Parameter type for JSON number."""

    @staticmethod
    def check(arg: TypingAny) -> float:
        """Check the parameter type.

        Arguments:
            arg: The argument which needs to be checked.

        Returns:
            The input argument unchanged.

        Raises:
            TypeError: When the input argument is not a JSON number (float and int in python).

        """
        if not isinstance(arg, (float, int)):
            raise TypeError("Argument should be a float or int")

        return arg


class Integer(ParameterType):
    """Parameter type for JSON integer."""

    @staticmethod
    def check(arg: TypingAny) -> float:
        """Check the parameter type.

        Arguments:
            arg: The argument which needs to be checked.

        Returns:
            The input argument unchanged.

        Raises:
            TypeError: When the input argument is not a JSON integer (int in python).

        """
        if not isinstance(arg, int):
            raise TypeError("Argument should be a int")

        return arg


class PortexType(ParameterType):
    """Parameter type for Portex type."""

    @staticmethod
    def check(arg: TypingAny) -> ClassPortexType:
        """Check the parameter type.

        Arguments:
            arg: The argument which needs to be checked.

        Returns:
            The input argument unchanged.

        Raises:
            TypeError: When the input argument is not a Portex type.

        """
        if not isinstance(arg, ClassPortexType):
            raise TypeError("Argument should be a Portex type")

        return arg


class String(ParameterType):
    """Parameter type for JSON string."""

    @staticmethod
    def check(arg: TypingAny) -> str:
        """Check the parameter type.

        Arguments:
            arg: The argument which needs to be checked.

        Returns:
            The input argument unchanged.

        Raises:
            TypeError: When the input argument is not a JSON string (str in python).

        """
        if not isinstance(arg, str):
            raise TypeError("Argument should be a string")

        return arg


class TypeName(String):
    """Parameter type for Portex type name."""
