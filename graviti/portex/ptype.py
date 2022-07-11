#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Parameter type releated classes."""


from typing import Any as TypingAny
from typing import ClassVar, Dict, Iterable, List
from typing import Mapping as TypingMapping
from typing import Optional, Sequence, Tuple, Type, Union

from graviti.portex.base import PortexType as ClassPortexType
from graviti.portex.field import Fields as ClassFields
from graviti.portex.package import Imports


class ParameterType:
    """The base class of parameter type."""

    @staticmethod
    def check(arg: TypingAny) -> TypingAny:
        """Check the parameter type.

        Arguments:
            arg: The argument which needs to be checked.

        Returns:
            The input argument unchanged.

        """
        return arg

    @staticmethod
    def load(content: TypingAny, _: Optional[Imports] = None) -> TypingAny:
        """Create an instance of the parameter type from the python content.

        Arguments:
            content: A python presentation of the parameter type.
            _: The imports of the parameter type.

        Returns:
            An instance of the parameter type.

        """
        return content

    @staticmethod
    def dump(arg: TypingAny) -> TypingAny:
        """Dump the parameter type instance into the python presentation.

        Arguments:
            arg: The parameter type instance.

        Returns:
            The python presentation of the input instance.

        """
        return arg


PType = Type[ParameterType]


class _JsonType(ParameterType):
    _type: ClassVar[Union[Type[TypingAny], Tuple[Type[TypingAny], ...]]]

    @classmethod
    def check(cls, arg: TypingAny) -> TypingAny:
        """Check the type of the input argument.

        Arguments:
            arg: The argument which needs to be checked.

        Returns:
            The input argument unchanged.

        Raises:
            TypeError: When the input argument does not match the parameter type.

        """
        if not isinstance(arg, cls._type):
            raise TypeError(f"Argument should be a {cls._type}")

        return arg


class Any(ParameterType):
    """Unconstrained parameter type."""


class Boolean(_JsonType):
    """Parameter type for JSON Boolean."""

    _type = bool


class Array(_JsonType):
    """Parameter type for JSON Array."""

    _type = Sequence


class Mapping(_JsonType):
    """Parameter type for JSON object."""

    _type = TypingMapping


class Number(_JsonType):
    """Parameter type for JSON number."""

    _type = (int, float)


class Integer(_JsonType):
    """Parameter type for JSON integer."""

    _type = int


class String(_JsonType):
    """Parameter type for JSON string."""

    _type = str


class Enum(ParameterType):
    """Parameter type for Portex enum values."""

    @staticmethod
    def check(arg: TypingAny) -> List[Union[int, float, str, bool, None]]:
        """Check and transfer the parameter type.

        Arguments:
            arg: The argument which needs to be checked.

        Raises:
            TypeError: When the argument is not iterable or
                the value of the input argument is not string type.

        Returns:
            A list of enum values created by the input argument.

        """
        if not isinstance(arg, Iterable):
            raise TypeError("Argument should be iterable")

        values = list(arg)
        for value in values:
            if not isinstance(value, (str, int, float, bool, type(None))):
                raise TypeError("The value of enum must be string, int, float, bool or None type")

        return values


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

    @staticmethod
    def load(content: Optional[List[TypingAny]], imports: Optional[Imports] = None) -> ClassFields:
        """Create Portex field list instance from python list.

        Arguments:
            content: A python list representing a Portex field list.
            imports: The imports of the Portex field.

        Returns:
            A Portex field list instance created from the input python list.

        """
        if content is not None:
            return ClassFields.from_pyobj(content, imports)

        return ClassFields()

    @staticmethod
    def dump(arg: ClassFields) -> List[TypingAny]:
        """Dump the input Portex field list instance to a python list.

        Arguments:
            arg: A Portex field list instance.

        Returns:
            A Python list representation of the Portex field list.

        """
        return arg.to_pyobj()


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

    @staticmethod
    def load(
        content: Optional[Dict[str, TypingAny]], imports: Optional[Imports] = None
    ) -> Optional[ClassPortexType]:
        """Create Portex type instance from python dict.

        Arguments:
            content: A python dict representing a Portex type.
            imports: The imports of the Portex type.

        Returns:
            A Portex type instance created from the input python dict.

        """
        if content is not None:
            return ClassPortexType.from_pyobj(content, imports)
        return None

    @staticmethod
    def dump(arg: ClassPortexType) -> Dict[str, TypingAny]:
        """Dump the instance to a python dict.

        Arguments:
            arg: A Portex type instance.

        Returns:
            A python dict representation of the Portex type.

        """
        return arg.to_pyobj(False)
