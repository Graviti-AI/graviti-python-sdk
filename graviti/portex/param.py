#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Parameter releated classes."""


from collections import OrderedDict
from copy import deepcopy
from inspect import Parameter, Signature
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional

from graviti.portex import ptype as PTYPE
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
        if default is not self._empty:
            default = ptype.load(default)
        super().__init__(name, Parameter.POSITIONAL_OR_KEYWORD, default=default)
        self.options = set(options) if options else None
        self.ptype = ptype

    @classmethod
    def from_pyobj(cls, pyobj: Dict[str, Any], ptype: PTYPE.PType = PTYPE.Any) -> "Param":
        """Create Param instance from python dict.

        Arguments:
            pyobj: A python dict representing a parameter.
            ptype: The parameter type.

        Returns:
            A Param instance created from the input python dict.

        """
        return cls(
            pyobj["name"],
            pyobj.get("default", cls._empty),
            pyobj.get("options"),
            ptype,
        )

    @property
    def required(self) -> bool:
        """Whether this parameter is a required parameter.

        Returns:
            ``True`` for required and ``False`` for non-required parameter.

        """
        return self.default == self._empty  # type: ignore[no-any-return]

    def to_pyobj(self) -> Dict[str, Any]:
        """Dump the instance to a python dict.

        Returns:
            A python dict representation of the Param.

        """
        pyobj: Dict[str, Any] = {"name": self.name}
        if self.default != self._empty:
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
            ValueError: Raise when the argument is not in options.

        """
        if arg is self.default:
            return deepcopy(arg)

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

        """
        return self.ptype.load(content, imports)

    def dump(self, arg: Any) -> Any:
        """Dump the parameter type instance into the python presentation.

        Arguments:
            arg: The parameter type instance.

        Returns:
            The python presentation of the input instance.

        """
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
    def from_pyobj(cls, pyobj: List[Dict[str, Any]], keys: Dict[str, Any]) -> "Params":
        """Create Params instance from python list.

        Arguments:
            pyobj: A python dict representing parameters.
            keys: A python dict containing parameter types.

        Returns:
            A Params instance created from the input python list.

        """
        params = cls()
        for item in pyobj:
            params.add(Param.from_pyobj(item, keys.get(item["name"], PTYPE.Any)))

        return params

    def to_pyobj(self) -> List[Dict[str, Any]]:
        """Dump the instance to a python list.

        Returns:
            A python list representation of the Params.

        """
        return [item.to_pyobj() for item in self._data.values()]

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
