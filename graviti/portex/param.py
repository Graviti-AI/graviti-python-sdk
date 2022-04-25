#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Parameter releated classes."""


from collections import OrderedDict
from inspect import Parameter, Signature
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Optional

import graviti.portex.ptype as PTYPE
from graviti.portex.factory import Dynamic
from graviti.portex.package import Imports
from graviti.utility import UserMapping


def param(
    default: Any = Parameter.empty,
    options: Optional[Iterable[Any]] = None,
    ptype: PTYPE.PType = PTYPE.Any,
) -> Any:
    """The factory function of Param.

    Arguments:
        default: The default value of the parameter.
        options: All possible values of the parameter.
        ptype: The parameter type.

    Returns:
        A tuple which contains "default", "options" and "ptype".

    """
    return default, options, ptype


class Param(Parameter):
    """Represents a parameter of a portex type.

    Arguments:
        name: The name of the parameter.
        default: The default value of the parameter.
        options: All possible values of the parameter.
        ptype: The parameter type.

    """

    _empty = Parameter.empty

    def __init__(
        self,
        name: str,
        default: Any = _empty,
        options: Optional[Iterable[Any]] = None,
        ptype: PTYPE.PType = PTYPE.Any,
    ) -> None:
        super().__init__(name, Parameter.POSITIONAL_OR_KEYWORD, default=default)
        self.options = set(options) if options else None
        self.ptype = ptype

    @property
    def required(self) -> bool:
        """Whether this parameter is a required parameter.

        Returns:
            ``True`` for required and ``False`` for non-required parameter.

        """
        return self.default == self._empty  # type: ignore[no-any-return]

    @classmethod
    def from_pyobj(cls, name: str, pyobj: Dict[str, Any]) -> "Param":
        """Create Param instance from python dict.

        Arguments:
            name: The name of the parameter.
            pyobj: A python dict representing a parameter.

        Returns:
            A Param instance created from the input python dict.

        """
        return cls(
            name,
            pyobj.get("default", cls._empty),
            pyobj.get("options"),
        )

    def to_pyobj(self) -> Dict[str, Any]:
        """Dump the instance to a python dict.

        Returns:
            A python dict representation of the Param.

        """
        required = self.required
        pyobj: Dict[str, Any] = {"required": required}
        if not required:
            pyobj["default"] = self.default
        if self.options is not None:
            pyobj["options"] = list(self.options)

        return pyobj

    def check(self, arg: Any) -> Any:
        """Check the validity of the parameter.

        Arguments:
            arg: The argument which needs to be checked.

        Returns:
            The argument after checking.

        Raises:
            TypeError: Raise when ptype is dynamic.
            ValueError: Raise when the argument is not in options.

        """
        if arg == self.default:
            return arg

        # TODO: Support check for dynamic parameter types.
        if isinstance(self.ptype, Dynamic):
            raise TypeError("Check for dynamic type is not supported yet")

        arg = self.ptype.check(arg)
        # https://github.com/PyCQA/pylint/issues/3045
        # pylint: disable=unsupported-membership-test
        if self.options and arg not in self.options:
            raise ValueError(f"Arguments '{arg}' is not in {self.options}")

        return arg

    def load(self, content: Any, imports: Imports) -> Any:
        """Create an instance of the parameter type from the python content.

        Arguments:
            content: A python presentation of the parameter type.
            imports: The imports of the parameter type.

        Returns:
            An instance of the parameter type.

        Raises:
            TypeError: When ptype is dynamic.

        """
        # TODO: Support load for dynamic parameter types.
        if isinstance(self.ptype, Dynamic):
            raise TypeError("Load for dynamic type is not supported yet")

        return self.ptype.load(content, imports)

    def dump(self, arg: Any) -> Any:
        """Dump the parameter type instance into the python presentation.

        Arguments:
            arg: The parameter type instance.

        Returns:
            The python presentation of the input instance.

        Raises:
            TypeError: When ptype is dynamic.

        """
        # TODO: Support dump for dynamic parameter types.
        if isinstance(self.ptype, Dynamic):
            raise TypeError("Dump for dynamic type is not supported yet")

        return self.ptype.dump(arg)


class Params(UserMapping[str, Param]):
    """Represents all parameters of a portex type.

    Arguments:
        values: The parameters mapping.

    """

    def __init__(self, values: Optional[Mapping[str, Param]] = None) -> None:
        # Why not use typing.OrderedDict here?
        # typing.OrderedDict is supported after python 3.7.2
        # typing_extensions.OrderedDict will trigger https://github.com/python/mypy/issues/11528
        self._data: MutableMapping[str, Param] = OrderedDict(values if values else {})

    @classmethod
    def from_pyobj(cls, pyobj: Dict[str, Any]) -> "Params":
        """Create Params instance from python dict.

        Arguments:
            pyobj: A python dict representing parameters.

        Returns:
            A Params instance created from the input python dict.

        """
        params = cls()
        for key, value in pyobj.items():
            params.add(Param.from_pyobj(key, value))

        return params

    def to_pyobj(self) -> Dict[str, Any]:
        """Dump the instance to a python dict.

        Returns:
            A python dict representation of the Params.

        """
        return {key: value.to_pyobj() for key, value in self._data.items()}

    def add(self, value: Param) -> None:
        """Add a parameter.

        Arguments:
            value: The parameter which needs to be added to this instance.

        Raises:
            KeyError: When the parameter name is duplicated.

        """
        name = value.name
        if name in self._data:
            raise KeyError(f"Duplicate parameter name: '{name}'")

        self._data[name] = value

    def update(self, values: Mapping[str, Param]) -> None:
        """Update the parameters.

        Arguments:
            values: The parameters which need to be updated to this instance.

        """
        for value in values.values():
            self.add(value)

    def get_signature(self) -> Signature:
        """Get the python inspect Signature from parameters.

        Returns:
            The :class:`Signature` instance created by all parameters in this instance.

        """
        return Signature(list(self._data.values()))
