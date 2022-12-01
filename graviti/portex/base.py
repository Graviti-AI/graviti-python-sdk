#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""The base elements of Portex type."""


import json
from copy import deepcopy
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Type, TypeVar

import pyarrow as pa
import yaml

from graviti.portex.package import Imports, Package
from graviti.portex.register import PyArrowConversionRegister
from graviti.utility import INDENT, PathLike, UserMutableMapping

if TYPE_CHECKING:
    from graviti.dataframe import Container
    from graviti.dataframe.sql.container import ArrayContainer
    from graviti.portex.builtin import PortexBuiltinType
    from graviti.portex.factory import ConnectedFieldsFactory
    from graviti.portex.field import ConnectedFields
    from graviti.portex.param import Params

PYARROW_TYPE_ID_TO_PORTEX_TYPE = PyArrowConversionRegister.PYARROW_TYPE_ID_TO_PORTEX_TYPE

_T = TypeVar("_T", bound="PortexType")


class PortexType:
    """The base class of portex type."""

    nullable: bool
    package: ClassVar[Package[Any]]
    params: ClassVar["Params"]
    container: ClassVar[Type["Container"]]
    element: ClassVar[Type[Any]]
    search_container: ClassVar[Type["ArrayContainer"]]

    def __repr__(self) -> str:
        return self._repr1(0)

    @classmethod
    def _from_pyarrow(cls: Type[_T], paarray: pa.Array) -> _T:
        raise NotImplementedError

    def _repr1(self, level: int) -> str:
        with_params = False
        indent = level * INDENT
        lines = [f"{self.__class__.__name__}("]
        for name, parameter in self.params.items():
            attr = getattr(self, name)
            if attr != parameter.default:
                with_params = True
                lines.append(
                    f"{INDENT}{name}="  # pylint: disable=protected-access
                    f"{attr._repr1(level + 1) if hasattr(attr, '_repr1') else repr(attr)},"
                )

        if with_params:
            lines.append(")")
            return f"\n{indent}".join(lines)

        return f"{lines[0]})"

    def _get_column_count(self) -> int:  # pylint: disable=no-self-use
        """Get the total column count of the portex type.

        Returns:
            The total column count.

        """
        return 1

    @property
    def imports(self) -> Imports:
        """Get the PortexType imports.

        Returns:
            The :class:`Imports` instance of this PortexType.

        """
        imports = Imports()

        cls = self.__class__
        imports[cls.__name__] = cls

        for name in self.params:
            argument = getattr(self, name)
            argument_imports = getattr(argument, "imports", None)
            if argument_imports:
                imports.update(argument_imports)

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
    def from_pyarrow(cls: Type[_T], paarray: pa.Array) -> _T:
        """Create Portex type instance from PyArrow type.

        Arguments:
            paarray: The PyArrow array.

        Raises:
            TypeError: When the PyArrow type is not supported.

        Returns:
            The created Portex type instance.

        """
        patype = paarray.type
        try:
            portex_type = PYARROW_TYPE_ID_TO_PORTEX_TYPE[patype.id]
        except KeyError:
            raise TypeError(f'Not supported PyArrow type "{patype}"') from None

        # pylint: disable=protected-access
        return portex_type._from_pyarrow(paarray)  # type: ignore[return-value]

    @classmethod
    def from_json(cls: Type[_T], content: str) -> _T:
        """Create Portex type instance from JSON string.

        Arguments:
            content: A JSON string representing a Portex type.

        Returns:
            A Portex type instance created from the input JSON string.

        """
        return cls.from_pyobj(json.loads(content))

    @classmethod
    def from_yaml(cls: Type[_T], content: str) -> _T:
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
        return json.dumps(self.to_pyobj(), ensure_ascii=False)

    def to_yaml(self) -> str:
        """Dump the instance to a YAML string.

        Returns:
            A YAML representation of the Portex type.

        """
        return yaml.dump(  # type: ignore[no-any-return]
            self.to_pyobj(), sort_keys=False, allow_unicode=True
        )

    def to_pyarrow(self, *, _to_backend: bool = False) -> pa.DataType:
        """Convert the Portex type to the corresponding builtin PyArrow DataType.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        Return:
            The corresponding builtin PyArrow DataType.

        """
        raise NotImplementedError

    def to_builtin(self) -> "PortexBuiltinType":
        """Expand the top level type to Portex builtin type.

        Raises:
            NotImplementedError: The method of the base class should not be called.

        """
        raise NotImplementedError

    def copy(self: _T) -> _T:
        """Get a copy of the portex type.

        Returns:
            A copy of the portex type.

        """
        return deepcopy(self)


class PortexRecordBase(
    PortexType, UserMutableMapping[str, PortexType]
):  # pylint: disable=abstract-method
    """The base class of record like Portex types."""

    _fields_factory: "ConnectedFieldsFactory"

    @property
    def _data(self) -> "ConnectedFields":  # type: ignore[override]
        return self._fields_factory({name: getattr(self, name) for name in self.params})

    def _get_column_count(self) -> int:
        """Get the total column count of the record base type.

        Returns:
            The total column count.

        """
        return sum(
            portex_type._get_column_count()  # pylint: disable=protected-access
            for portex_type in self._data.values()
        )

    def insert(self, index: int, name: str, portex_type: PortexType) -> None:
        """Insert the name and portex_type at the index.

        Arguments:
            index: The index to insert the field.
            name: The name of the field to be inserted.
            portex_type: The portex_type of the field to be inserted.

        """
        self._data.insert(index, name, portex_type)

    def astype(self, name: str, portex_type: PortexType) -> None:
        """Convert the type of the field with the given name to the new PortexType.

        Arguments:
            name: The name of the field to convert.
            portex_type: The new PortexType of the field to convert to.

        """
        self._data.astype(name, portex_type)

    def rename(self, old_name: str, new_name: str) -> None:
        """Rename the name of a field.

        Arguments:
            old_name: The current name of the field to be renamed.
            new_name: The new name of the field to assign.

        """
        self._data.rename(old_name, new_name)

    def to_pyarrow(self, *, _to_backend: bool = False) -> pa.StructType:
        """Convert the Portex type to the corresponding builtin PyArrow StructType.

        Returns:
            The corresponding builtin PyArrow StructType.

        """
        return pa.struct(
            [
                pa.field(key, value.to_pyarrow(_to_backend=_to_backend))
                for key, value in self._data.items()
            ]
        )


def read_yaml(path: PathLike) -> PortexType:
    """Read a yaml file into Portex type.

    Arguments:
        path: The path of the yaml file.

    Returns:
        A Portex type instance created from the input yaml file.

    """
    with open(path, encoding="utf-8") as fp:
        return PortexType.from_pyobj(yaml.load(fp, yaml.Loader))


def read_json(path: PathLike) -> PortexType:
    """Read a json file into Portex type.

    Arguments:
        path: The path of the json file.

    Returns:
        A Portex type instance created from the input json file.

    """
    with open(path, encoding="utf-8") as fp:
        return PortexType.from_pyobj(json.load(fp))
