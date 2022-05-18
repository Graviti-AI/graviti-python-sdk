#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Parameter type releated classes."""


from typing import Any as TypingAny
from typing import Dict, List, Optional, Sequence, Tuple, Type

from graviti.portex.base import PortexType as ClassPortexType
from graviti.portex.field import Fields as ClassFields
from graviti.portex.package import Imports


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


class Boolean(ParameterType):
    """Parameter type for JSON Boolean."""

    @staticmethod
    def check(arg: TypingAny) -> TypingAny:
        """Check the parameter type.

        Arguments:
            arg: The argument which needs to be checked.

        Returns:
            The input argument unchanged.

        Raises:
            TypeError: When the input argument is not a JSON boolean (bool in python).

        """
        if not isinstance(arg, bool):
            raise TypeError("Argument should be a bool")

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


class Mapping(ParameterType):
    """Parameter type for JSON object."""

    @staticmethod
    def check(arg: TypingAny) -> Dict[TypingAny, TypingAny]:
        """Check the parameter type.

        Arguments:
            arg: The argument which needs to be checked.

        Returns:
            The input argument unchanged.

        Raises:
            TypeError: When the input argument is not a JSON object (dict in python).

        """
        if not isinstance(arg, dict):
            raise TypeError("Argument should be a dict")

        return arg


class Field(ParameterType):
    """Parameter type for Portex record field."""

    @staticmethod
    def check(arg: TypingAny) -> Tuple[str, ClassPortexType]:
        """Check and transfer the parameter type.

        Arguments:
            arg: The argument which needs to be checked.

        Raises:
            TypeError: When the input argument is not a tuple of a str and a PortexType.

        Returns:
            A tuple of str and PortexType created by the input argument.

        """
        name, portex_type = arg
        if isinstance(name, str) and isinstance(portex_type, ClassPortexType):
            return name, portex_type

        raise TypeError("Argument should be a tuple of a str and a PortexType")

    @staticmethod
    def load(
        content: Dict[str, TypingAny], imports: Optional[Imports] = None
    ) -> Tuple[str, ClassPortexType]:
        """Create Portex field instance from python dict.

        Arguments:
            content: A python dict representing a Portex field.
            imports: The imports of the Portex field.

        Returns:
            A tuple of name and PortexType created from the input python dict.

        """
        return content["name"], ClassPortexType.from_pyobj(content, imports)

    @staticmethod
    def dump(arg: Tuple[str, ClassPortexType]) -> Dict[str, TypingAny]:
        """Dump the input Portex field instance to a python dict.

        Arguments:
            arg: A tuple of name and PortexType.

        Returns:
            A Python dict representation of the Portex field.

        """
        name, portex_type = arg
        return {"name": name, **portex_type.to_pyobj(False)}


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
    def load(content: List[TypingAny], imports: Optional[Imports] = None) -> ClassFields:
        """Create Portex field list instance from python list.

        Arguments:
            content: A python list representing a Portex field list.
            imports: The imports of the Portex field.

        Returns:
            A Portex field list instance created from the input python list.

        """
        return ClassFields.from_pyobj(content, imports)

    @staticmethod
    def dump(arg: ClassFields) -> List[TypingAny]:
        """Dump the input Portex field list instance to a python list.

        Arguments:
            arg: A Portex field list instance.

        Returns:
            A Python list representation of the Portex field list.

        """
        return arg.to_pyobj()


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

    @staticmethod
    def load(content: Dict[str, TypingAny], imports: Optional[Imports] = None) -> ClassPortexType:
        """Create Portex type instance from python dict.

        Arguments:
            content: A python dict representing a Portex type.
            imports: The imports of the Portex type.

        Returns:
            A Portex type instance created from the input python dict.

        """
        return ClassPortexType.from_pyobj(content, imports)

    @staticmethod
    def dump(arg: ClassPortexType) -> Dict[str, TypingAny]:
        """Dump the instance to a python dict.

        Arguments:
            arg: A Portex type instance.

        Returns:
            A python dict representation of the Portex type.

        """
        return arg.to_pyobj(False)


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
