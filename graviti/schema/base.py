#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""The base elements of Portex type."""


import json
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional

import yaml

from graviti.schema.package import Imports, Package

if TYPE_CHECKING:
    from graviti.schema.param import Params

_INDENT = " " * 2


class PortexType:
    """The base class of portex type."""

    name: str
    imports: Imports = Imports()
    package: ClassVar[Package[Any]]
    params: ClassVar["Params"]

    def __init__(self, **kwargs: Any) -> None:
        self.__dict__.update(kwargs)
        imports = Imports()
        for kwarg in kwargs.values():
            if hasattr(kwarg, "imports"):
                imports.update(kwarg.imports)

            if isinstance(kwarg, PortexType):
                class_ = kwarg.__class__
                imports[class_.name] = class_

        self.imports = imports

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

    def _bind_arguments(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        signature = self.params.get_signature()
        bound_arguments = signature.bind(*args, **kwargs)
        bound_arguments.apply_defaults()
        return bound_arguments.arguments

    @classmethod
    def from_pyobj(
        cls, content: Dict[str, Any], _imports: Optional["Imports"] = None
    ) -> "PortexType":
        """Create Portex type instance from python dict.

        Arguments:
            content: A python dict representing a Portex type.

        Returns:
            A Portex type instance created from the input python dict.

        """
        if _imports is None:
            _imports = Imports.from_pyobj(content.get("imports", []))

        class_ = _imports[content["type"]]

        assert issubclass(class_, cls)
        kwargs = {}
        for name, param in class_.params.items():
            kwarg = content.get(name, ...)
            if kwarg is not ...:
                kwargs[name] = param.load(kwarg, _imports)

        type_ = class_(**kwargs)
        return type_

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

        pydict["type"] = self.__class__.name
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
