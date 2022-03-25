#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Package related class."""


from itertools import chain
from pathlib import Path
from subprocess import run
from tempfile import gettempdir
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

import yaml
from tensorbay.utility import ReprMixin, ReprType, UserMapping

from graviti.utility import AttrDict

if TYPE_CHECKING:
    from graviti.schema.base import PortexType
    from graviti.schema.builtin import PortexBuiltinType
    from graviti.schema.template import PortexExternalType

_T = TypeVar("_T", bound=Type["PortexType"])
_S = TypeVar("_S", bound=Type["PortexBuiltinType"])


class Package(AttrDict[_T]):
    """The base class of Portex package."""

    def __setitem__(self, key: str, value: _T) -> None:
        if key in self:
            raise KeyError(f"{key} already exists in package")

        super().__setitem__(key, value)
        value.name = key
        value.package = self


class BuiltinPackage(Package[Type["PortexBuiltinType"]]):
    """The builtin Portex package used to manage builtin types."""

    def __call__(self, name: str) -> Callable[[_S], _S]:
        """Parameterized decorator for registering Portex builtin type to builtin package.

        Arguments:
            name: The builtin type name.

        Returns:
            The decorator to registering Portex builtin type to builtin package.

        """

        def register(class_: _S) -> _S:
            self[name] = class_
            return class_

        return register


class LocalPackage(Package[Type["PortexType"]]):
    """The local Portex package used to manage local types."""


class ExternalPackage(Package[Type["PortexExternalType"]]):
    """The external Portex package used to manage external types.

    Arguments:
        url: The git repo url of the external package.
        revision: The git repo revision (tag/commit) of the external package.

    """

    def __init__(self, url: str, revision: str) -> None:
        super().__init__()
        self.url = url
        self.revision = revision
        self._builders: Dict[str, TypeBuilder] = {}
        self._build()

    def __missing__(self, key: str) -> Type["PortexExternalType"]:
        return self._builders[key]()

    def _build(self) -> None:
        temp_path = Path(gettempdir()) / "portex"
        temp_path.mkdir(exist_ok=True)
        repo_name = self.url.rsplit("/", 1)[-1]
        repo_path = temp_path / repo_name
        if not repo_path.exists():
            run(["git", "clone", self.url, "-b", self.revision, repo_path], check=True)

        roots = list(repo_path.glob("**/ROOT.yaml"))

        if len(roots) == 0:
            raise TypeError("No 'ROOT.yaml' file found")
        if len(roots) >= 2:
            raise TypeError("More than one 'ROOT.yaml' file found")

        root_dir = roots[0].parent
        for yaml_file in root_dir.glob("**/*.yaml"):
            if yaml_file.name == "ROOT.yaml":
                continue

            parts = (*yaml_file.relative_to(root_dir).parent.parts, yaml_file.stem)
            name = ".".join(parts)
            self._builders[name] = TypeBuilder(name, yaml_file, self)

        for builder in self._builders.values():
            if builder.is_building:
                continue
            builder()

    @property
    def repo(self) -> str:
        """The repo string of the package.

        Returns:
            The "<url>@<rev>" format repo string.

        """
        return f"{self.url}@{self.revision}"


class Subpackage(UserMapping[str, Type["PortexExternalType"]]):
    """The subset of Portex package, used in :class:`Imports`.

    Arguments:
        package: The source package of this subpackage.

    """

    def __init__(self, package: ExternalPackage) -> None:
        self.package = package
        self._data: Dict[str, Type["PortexExternalType"]] = {}

    def __setitem__(self, key: str, value: Type["PortexExternalType"]) -> None:
        type_ = self._data.get(key)
        if type_:
            if type_ != value:
                raise KeyError("Duplicate names")
            return

        for name, type_ in self._data.items():
            if type_ == value:
                if name != key:
                    raise ValueError(
                        "Same type with a different name already exists in the subpackage"
                    )
                return

        self._data[key] = value

    @classmethod
    def from_pyobj(cls, content: Dict[str, Any]) -> "Subpackage":
        """Create :class:`Subpackage` instance from python dict.

        Arguments:
            content: A python dict representing a subpackage.

        Returns:
            A :class:`Subpackage` instance created from the input python dict.

        """
        url, revision = content["repo"].split("@", 1)
        try:
            package = packages.externals[url, revision]
        except KeyError:
            package = packages.build_package(url, revision)

        subpackage = cls(package)
        for type_ in content["types"]:
            name = type_["name"]
            subpackage[type_.get("alias", name)] = package[name]

        return subpackage

    def to_pyobj(self) -> Dict[str, Any]:
        """Dump the instance to a python dict.

        Returns:
            A python dict representation of the :class:`Subpackage` instance.

        """
        pyobj: Dict[str, Any] = {"repo": self.package.repo}
        types = []
        for key, value in self._data.items():
            name = value.name
            type_ = {"name": name}
            if key != name:
                type_["alias"] = key
            types.append(type_)

        pyobj["types"] = types
        return pyobj


class TypeBuilder:
    """The builder of the external Portex template type.

    Arguments:
        name: The name of the Portex template type.
        path: The source file path of the Portex template type.
        package: The package the Portex template type belongs to.

    """

    def __init__(self, name: str, path: Path, package: "ExternalPackage") -> None:
        self._name = name
        self._path = path
        self._package = package
        self.is_building = False

    def __call__(self) -> Type["PortexExternalType"]:
        """Build the Portex external type.

        Returns:
            The builded Portex external type.

        Raises:
            TypeError: Raise when circular reference detected.

        """
        # TODO: Import "template" on toplevel will cause circular imports, wait to be solved.
        # pylint: disable=import-outside-toplevel
        from graviti.schema.template import template

        if self.is_building:
            raise TypeError("Circular reference")

        self.is_building = True

        with self._path.open() as fp:
            content = yaml.load(fp, yaml.Loader)

        return template(self._name, content, self._package)


class Packages:
    """The package manager to manage different Portex packages."""

    def __init__(self) -> None:
        self.builtins = BuiltinPackage()
        self.externals: Dict[Tuple[str, str], "ExternalPackage"] = {}
        self.locals = LocalPackage()

    def build_package(self, url: str, revision: str) -> "ExternalPackage":
        """Build an external package.

        Arguments:
            url: The git repo url of the external package.
            revision: The git repo revision (tag/commit) of the external package.

        Returns:
            The :class:`ExternalPackage` instance.

        """
        package = ExternalPackage(url, revision)
        self.externals[url, revision] = package
        return package


packages = Packages()


class Imports(Mapping[str, Type["PortexType"]], ReprMixin):
    """The imports of the Portex template type.

    Arguments:
        package: The package the portex belongs to.

    """

    _repr_type = ReprType.MAPPING

    def __init__(self, package: Optional[Package[Type["PortexType"]]] = None) -> None:
        self._package = package
        self._subpackages: Dict[str, Subpackage] = {}

    def __len__(self) -> int:
        return sum(map(len, self._subpackages.values()))

    def __getitem__(self, key: str) -> Type["PortexType"]:
        try:
            return packages.builtins[key]
        except KeyError:
            pass

        for subpackage in self._subpackages.values():
            try:
                return subpackage[key]
            except KeyError:
                continue

        if self._package:
            return self._package[key]

        raise KeyError(key)

    def __setitem__(self, key: str, value: Type["PortexType"]) -> None:
        type_ = self.get(key)
        if type_:
            if type_ != value:
                raise KeyError("Duplicate names")
            return

        # TODO: Import "PortexExternalType" on toplevel will cause circular imports, to be solved.
        # pylint: disable=import-outside-toplevel
        from graviti.schema.template import PortexExternalType

        if not issubclass(value, PortexExternalType):
            raise TypeError("Local portex type is not supported yet")

        package = value.package
        subpackage = self._subpackages.get(package.repo)
        if not subpackage:
            subpackage = Subpackage(package)
            self._subpackages[package.repo] = subpackage

        subpackage[key] = value

    def __iter__(self) -> Iterator[str]:
        return chain(*self._subpackages.values())

    def update(self, other: "Imports") -> None:
        """Update the imports with another imports.

        Arguments:
            other: An :class:`Imports` instance whose types need to be updated to this imports.

        """
        assert not self._package and not other._package  # pylint: disable=protected-access
        for key, value in other.items():
            self.__setitem__(key, value)

    def update_from_type(self, type_: "PortexType") -> None:
        """Update the imports from a Portex type instance.

        Arguments:
            type_: The Portex type instance needs to be updated into this imports.

        """
        class_ = type_.__class__
        self.__setitem__(class_.name, class_)
        self.update(type_.imports)

    @classmethod
    def from_pyobj(cls, content: List[Dict[str, Any]]) -> "Imports":
        """Create :class:`Imports` instance from python list.

        Arguments:
            content: A python list representing imported types.

        Returns:
            A :class:`Imports` instance created from the input python list.

        """
        imports = cls()
        for pyobj in content:
            subpackage = Subpackage.from_pyobj(pyobj)
            imports.add_subpackage(subpackage)

        return imports

    def to_pyobj(self) -> List[Dict[str, Any]]:
        """Dump the instance to a python list.

        Returns:
            A python list representation of the Portex imported types.

        """
        return [subpackage.to_pyobj() for subpackage in self._subpackages.values()]

    def add_subpackage(self, subpackage: Subpackage) -> None:
        """Add subpackage to this :class:`Imports` instance.

        Arguments:
            subpackage: The subpackage which needs to be added.

        Raises:
            KeyError: When there are duplicate names in the :class:`imports` instance.

        """
        for key in subpackage:
            if self.__contains__(key):
                raise KeyError("Duplicate names")

        self._subpackages[subpackage.package.repo] = subpackage

    def update_base_package(self, package: Package[Type["PortexType"]]) -> None:
        """Update base package to the :class:`Imports` instance.

        Arguments:
            package: The base package which needs to be updated.

        """
        self._package = package
