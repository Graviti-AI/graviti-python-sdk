#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""The base elements of Portex type."""


import json
from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, Optional, Tuple, Type, TypeVar

import pyarrow as pa
import yaml

from graviti.portex.package import Imports, Package

if TYPE_CHECKING:
    from graviti.portex.param import Params

_INDENT = " " * 2

_T = TypeVar("_T", bound="PortexType")

PYARROW_TYPE_ID_TO_PORTEX_TYPE = {}


class PyArrowConversionRegister:
    """Register the Portex type to set the conversion from PyArrow to Portex.

    Arguments:
        pyarrow_type_ids: The id of the corresponding PyArrow types.

    """

    def __init__(self, *pyarrow_type_ids: int) -> None:
        self._pyarrow_type_ids = pyarrow_type_ids

    def __call__(self, portex_type: Type[_T]) -> Type[_T]:
        """Register the Portex type and return it back.

        Arguments:
            portex_type: The Portex type to register.

        Returns:
            The original Portex type.

        """
        for pyarrow_type_id in self._pyarrow_type_ids:
            PYARROW_TYPE_ID_TO_PORTEX_TYPE[pyarrow_type_id] = portex_type
        return portex_type


class PortexType:
    """The base class of portex type."""

    nullable: bool
    package: ClassVar[Package[Any]]
    params: ClassVar["Params"]

    def __init__(self, **kwargs: Any) -> None:
        self.__dict__.update(kwargs)

    def __repr__(self) -> str:
        return self._repr1(0)

    @classmethod
    def _from_pyarrow(cls: Type[_T], pyarrow_type: pa.DataType) -> _T:
        raise NotImplementedError

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

    def _bind_arguments(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        signature = self.params.get_signature()
        bound_arguments = signature.bind(*args, **kwargs)
        bound_arguments.apply_defaults()
        return bound_arguments.arguments

    def _dump_arguments(self) -> Dict[str, Any]:
        arguments = {}
        for key, value in self.params.items():
            argument = getattr(self, key)
            if argument == value.default:
                continue

            arguments[key] = value.dump(argument)

        return arguments

    def _get_keys(self) -> List[Tuple[str, ...]]:  # pylint: disable=no-self-use
        return []

    @property
    def imports(self) -> Imports:
        """Get the PortexType imports.

        Returns:
            The :class:`Imports` instance of this PortexType.

        """
        imports = Imports()

        for name in self.params:
            argument = getattr(self, name)
            if hasattr(argument, "imports"):
                imports.update(argument.imports)

            if isinstance(argument, PortexType):
                class_ = argument.__class__
                imports[class_.__name__] = class_

        return imports

    @classmethod
    def from_pyobj(
        cls: Type[_T], content: Dict[str, Any], _imports: Optional["Imports"] = None
    ) -> _T:
        """Create Portex type instance from python dict.

        Arguments:
            content: A python dict representing a Portex type.

        Returns:
            A Portex type instance created from the input python dict.

        """
        if _imports is None:
            _imports = Imports.from_pyobj(content.get("imports", []))

        class_: Type[_T] = _imports[content["type"]]  # type: ignore[assignment]

        assert issubclass(class_, cls)
        kwargs = {}
        for name, param in class_.params.items():
            kwarg = content.get(name, ...)
            if kwarg is not ...:
                kwargs[name] = param.load(kwarg, _imports)

        type_ = class_(**kwargs)
        return type_

    @classmethod
    def from_pyarrow(cls: Type[_T], pyarrow_type: pa.DataType) -> _T:
        """Create Portex type instance from PyArrow type.

        Arguments:
            pyarrow_type: The PyArrow type.

        Raises:
            TypeError: When the PyArrow type is not supported.

        Returns:
            The created Portex type instance.

        """
        pyarrow_type_id = (
            pyarrow_type.value_type.id
            if pyarrow_type.id == pa.lib.Type_DICTIONARY  # pylint: disable=c-extension-no-member
            else pyarrow_type.id
        )

        try:
            portex_type = PYARROW_TYPE_ID_TO_PORTEX_TYPE[pyarrow_type_id]
        except KeyError:
            raise TypeError(f'Not supported PyArrow type "{pyarrow_type}"') from None

        return portex_type._from_pyarrow(pyarrow_type)  # pylint: disable=protected-access

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

    def to_pyobj(self, _with_imports: bool = True) -> Dict[str, Any]:
        """Dump the instance to a python dict.

        Returns:
            A python dict representation of the Portex type.

        """
        pydict: Dict[str, Any] = {}
        if _with_imports:
            imports_pyobj = self.imports.to_pyobj()
            if imports_pyobj:
                pydict["imports"] = imports_pyobj

        pydict["type"] = self.__class__.__name__
        for name, parameter in self.params.items():
            attr = getattr(self, name)
            if attr != parameter.default:
                pydict[name] = parameter.dump(attr)

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

    def to_pyarrow(self) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        Return:
            The corresponding builtin PyArrow DataType.

        """
        raise NotImplementedError

    def copy(self: _T) -> _T:
        """Get a copy of the portex type.

        Returns:
            A copy of the portex type.

        """
        obj: _T = object.__new__(self.__class__)
        for name, param in self.params.items():
            setattr(obj, name, param.copy(getattr(self, name)))

        return obj
