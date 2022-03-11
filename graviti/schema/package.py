#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Package related class."""


from pathlib import Path
from subprocess import run
from tempfile import gettempdir
from typing import TYPE_CHECKING, Callable, Dict, Optional, Tuple, Type, TypeVar

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
        repo: The git repo url of the external package.
        version: The git repo version (tag/commit) of the external package.

    """

    def __init__(self, repo: str, version: str) -> None:
        super().__init__()
        self.repo = repo
        self.version = version
        self._builders: Dict[str, TypeBuilder] = {}
        self._build()

    def __setitem__(self, key: str, value: Type["PortexExternalType"]) -> None:
        super().__setitem__(key, value)
        value.repo = self.repo
        value.version = self.version

    def __missing__(self, key: str) -> Type["PortexExternalType"]:
        return self._builders[key]()

    def _build(self) -> None:
        temp_path = Path(gettempdir()) / "portex"
        temp_path.mkdir(exist_ok=True)
        repo_name = self.repo.rsplit("/", 1)[-1]
        repo_path = temp_path / repo_name
        if not repo_path.exists():
            run(["git", "clone", self.repo, "-b", self.version, repo_path], check=True)

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

    def search_type(
        self, name: str, repo: Optional[str] = None, version: Optional[str] = None
    ) -> Type["PortexType"]:
        """Search the Portex type from all packages.

        Arguments:
            name: The name of the Portex type.
            repo: The git repo url of the external package.
            version: The git repo version (tag/commit) of the external package.

        Returns:
            The Portex Type which matches the inputs.

        Raises:
            KeyError: The Portex type which meets the inputs does not exist.

        """
        if name in self.builtins:
            return self.builtins[name]

        try:
            external_package = self.externals[repo, version]  # type: ignore[index]
            return external_package[name]
        except KeyError:
            pass

        try:
            return self.locals[name]
        except KeyError as error:
            raise KeyError("Type not found") from error


packages = Packages()


class Imports:
    """The imports of the Portex template type.

    Arguments:
        package: The package the portex belongs to.

    """

    def __init__(self, package: Package[Type["PortexType"]]) -> None:
        self._package = package

    def __getitem__(self, key: str) -> Type["PortexType"]:
        try:
            return packages.builtins[key]
        except KeyError:
            return self._package[key]
