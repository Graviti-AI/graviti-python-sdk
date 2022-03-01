#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""The base elements of Portex type."""


import json
from typing import Any, Dict, Iterable, Optional, Type, TypeVar

import yaml

_INDENT = " " * 2


class TypeRegister:
    """A class decorator to register the Portex types.

    Arguments:
        name: The name of the Portex type.

    """

    _S = TypeVar("_S", bound=Type["PortexType"])

    NAME_TO_CLASS: Dict[str, Type["PortexType"]] = {}
    CLASS_TO_NAME: Dict[Type["PortexType"], str] = {}

    def __init__(self, name: str) -> None:
        self._name = name

    def __call__(self, class_: _S) -> _S:
        """Register the input Portex type.

        Arguments:
            class_: The Portex type needs be registered.

        Returns:
            The input class unchanged.

        """
        self.NAME_TO_CLASS[self._name] = class_
        self.CLASS_TO_NAME[class_] = self._name
        return class_


class Param:
    """Represents a parameter of a portex type.

    Arguments:
        key: The name of the parameter.
        required: Whether the parameter is required.
        default: The default value of the parameter.
        options: All possible values of the parameter.

    Raises:
        TypeError: A required parameter should not have default value.

    """

    def __init__(
        self, key: str, required: bool, default: Any = ..., options: Optional[Iterable[Any]] = None
    ) -> None:
        if required and default is not ...:
            raise TypeError("required parameter should not have default value")

        self.key = key
        self.required = required
        self.default = default
        self.options = options


def param(
    key: str, required: bool, default: Any = ..., options: Optional[Iterable[Any]] = None
) -> Any:
    """The factory function of Param.

    Arguments:
        key: The name of the parameter.
        required: Whether the parameter is required.
        default: The default value of the parameter.
        options: All possible values of the parameter.

    Returns:
        A :class:`Param` instance created by the given input.

    """
    return Param(key, required, default, options)


class PortexType:
    """The base class of portex type."""

    params: Dict[str, Param] = {}

    def __init_subclass__(cls) -> None:
        params = cls.params.copy()
        for name in getattr(cls, "__annotations__", {}):
            parameter = getattr(cls, name, None)
            if isinstance(parameter, Param):
                params[name] = parameter
                delattr(cls, name)

        cls.params = params

    def __repr__(self) -> str:
        return self._repr1(0)

    def _repr1(self, level: int) -> str:
        with_params = False
        indent = level * _INDENT
        lines = [f"{self.__class__.__name__}("]
        for name, parameter in self.params.items():
            attr = getattr(self, name)
            if attr != parameter.default:
                with_params = True
                lines.append(
                    f"{_INDENT}{name}="  # pylint: disable=protected-access
                    f"{attr._repr1(level + 1) if hasattr(attr, '_repr1') else repr(attr)},"
                )

        if with_params:
            lines.append(")")
            return f"\n{indent}".join(lines)

        return f"{lines[0]})"

    @classmethod
    def from_pyobj(cls, content: Dict[str, Any]) -> "PortexType":
        """Create Portex type instance from python dict.

        Arguments:
            content: A python dict representing a Portex type.

        Returns:
            A Portex type instance created from the input python dict.

        """
        class_ = TypeRegister.NAME_TO_CLASS[content["type"]]
        assert issubclass(class_, cls)
        kwargs = {}
        for parameter in class_.params:
            kwarg = content.get(parameter, ...)
            if kwarg is not ...:
                kwargs[parameter] = kwarg

        return class_(**kwargs)  # type: ignore[call-arg]

    @classmethod
    def from_json(cls, content: str) -> "PortexType":
        """Create Portex type instance from JSON string.

        Arguments:
            content: A JSON string representing a Portex type.

        Returns:
            A Portex type instance created from the input JSON string.

        """
        return cls.from_pyobj(json.loads(content))

    @classmethod
    def from_yaml(cls, content: str) -> "PortexType":
        """Create Portex type instance from YAML string.

        Arguments:
            content: A YAML string representing a Portex type.

        Returns:
            A Portex type instance created from the input YAML string.

        """
        return cls.from_pyobj(yaml.load(content, yaml.Loader))

    def to_pyobj(self) -> Dict[str, Any]:
        """Dump the instance to a python dict.

        Returns:
            A python dict representation of the Portex type.

        """
        pydict = {"type": TypeRegister.CLASS_TO_NAME[self.__class__]}
        for name, parameter in self.params.items():
            attr = getattr(self, name)
            if attr != parameter.default:
                pydict[name] = attr.to_pyobj() if hasattr(attr, "to_pyobj") else attr

        return pydict

    def to_json(self) -> str:
        """Dump the instance to a JSON string.

        Returns:
            A JSON representation of the Portex type.

        """
        return json.dumps(self.to_pyobj())

    def to_yaml(self) -> str:
        """Dump the instance to a YAML string.

        Returns:
            A YAML representation of the Portex type.

        """
        return yaml.dump(self.to_pyobj(), sort_keys=False)  # type: ignore[no-any-return]
