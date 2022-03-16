#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Package related class."""


from pathlib import Path
from subprocess import run
from tempfile import gettempdir
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Type, TypeVar

import yaml
from tensorbay.utility import UserMapping

if TYPE_CHECKING:
    from graviti.schema.base import PortexType
    from graviti.schema.template import PortexExternalType

_T = TypeVar("_T", bound=Type["PortexType"])
_S = TypeVar("_S", bound=Type["PortexType"])


class Package(UserMapping[str, _T]):
    """The base class of Portex package."""

    def __init__(self) -> None:
        self._data: Dict[str, _T] = {}

    def __getitem__(self, key: str) -> _T:
        try:
            return self._data.__getitem__(key)
        except KeyError:
            return self.__missing__(key)

    def __setitem__(self, key: str, value: _T) -> None:
        if key in self._data:
            raise KeyError(f"{key} already exists in package")
        value.name = key
        value.package = self
        self._data[key] = value

    def __missing__(self, key: str) -> _T:
        raise KeyError(key)


class BuiltinPackage(Package[Type["PortexType"]]):
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

    def add_type(self, name: str, alias: Optional[str] = None) -> None:
        """Add type from the source package to the subpackage.

        Arguments:
            name: The name of the Portex type.
            alias: The alias of the Portex type.

        """
        self._data[name if alias is None else alias] = self.package[name]

    @classmethod
    def from_pyobj(cls, content: Dict[str, Any]) -> "Subpackage":
        """Create :class:`Subpackage` instance from python dict.

        Arguments:
            content: A python dict representing a subpackage.

        Returns:
            A :class:`Subpackage` instance created from the input python dict.

        """
        url, revision = content["repo"].split("@", 1)
        package = packages.externals[url, revision]
        partial_package = cls(package)
        for type_ in content["types"]:
            partial_package.add_type(type_["name"], type_.get("alias"))

        return partial_package

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
        # Import "template" on toplevel will cause circular imports, wait to be solved.
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

    def build_package(self, repo: str, version: str) -> "ExternalPackage":
        """Build an external package.

        Arguments:
            repo: The git repo url of the external package.
            version: The git repo version (tag/commit) of the external package.

        Returns:
            The :class:`ExternalPackage` instance.

        """
        package = ExternalPackage(repo, version)
        self.externals[repo, version] = package
        return package


packages = Packages()


class Imports:
    """The imports of the Portex template type.

    Arguments:
        package: The package the portex belongs to.

    """

    def __init__(self, package: Optional[Package[Type["PortexType"]]] = None) -> None:
        self._package = package
        self._subpackages: List[Subpackage] = []

    def __getitem__(self, key: str) -> Type["PortexType"]:
        try:
            return packages.builtins[key]
        except KeyError:
            pass

        for subpackage in self._subpackages:
            try:
                return subpackage[key]
            except KeyError:
                continue

        if self._package:
            return self._package[key]

        raise KeyError(key)

    def __contain__(self, key: str) -> bool:
        try:
            self.__getitem__(key)
            return True
        except KeyError:
            return False

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
            partial_package = Subpackage.from_pyobj(pyobj)
            imports.add_subpackage(partial_package)

        return imports

    def to_pyobj(self) -> List[Dict[str, Any]]:
        """Dump the instance to a python list.

        Returns:
            A python list representation of the Portex imported types.

        """
        return [partial_package.to_pyobj() for partial_package in self._subpackages]

    def add_subpackage(self, subpackage: Subpackage) -> None:
        """Add subpackage to this :class:`Imports` instance.

        Arguments:
            subpackage: The subpackage which needs to be added.

        Raises:
            KeyError: When there are duplicate names in the :class:`imports` instance.

        """
        for key in subpackage:
            if self.__contain__(key):
                raise KeyError("Duplicate names")

        self._subpackages.append(subpackage)

    def update_base_package(self, package: Package[Type["PortexType"]]) -> None:
        """Update base package to the :class:`Imports` instance.

        Arguments:
            package: The base package which needs to be updated.

        """
        self._package = package
