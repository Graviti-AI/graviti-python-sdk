#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Portex type builder related classes."""


from hashlib import md5
from pathlib import Path
from shutil import rmtree
from subprocess import PIPE, CalledProcessError, run
from tempfile import gettempdir
from typing import TYPE_CHECKING, Any, Dict, List, Tuple, Type, TypeVar

import yaml

import graviti.portex.ptype as PTYPE
from graviti.exception import GitCommandError, GitNotFoundError
from graviti.portex.base import PortexRecordBase
from graviti.portex.external import PortexExternalType
from graviti.portex.factory import ConnectedFieldsFactory, TypeFactory
from graviti.portex.package import ExternalPackage, Imports, packages
from graviti.portex.param import Param, Params
from graviti.portex.register import ExternalContainerRegister

if TYPE_CHECKING:
    from subprocess import CompletedProcess

    from graviti.portex.base import PortexType


EXTERNAL_TYPE_TO_CONTAINER = ExternalContainerRegister.EXTERNAL_TYPE_TO_CONTAINER

_I = TypeVar("_I", bound="BuilderImports")


class PackageRepo:
    """The local git repo of the external Portex package.

    Arguments:
        url: The git repo url of the external package.
        revision: The git repo revision (tag/commit) of the external package.

    """

    _env: Dict[str, Any] = {}

    def __init__(self, url: str, revision: str) -> None:
        tempdir = Path(gettempdir()) / "portex"
        tempdir.mkdir(exist_ok=True)

        md5_instance = md5()
        md5_instance.update(url.encode("utf-8"))
        md5_instance.update(revision.encode("utf-8"))

        self._path = tempdir / md5_instance.hexdigest()
        self._url = url
        self._revision = revision

        try:
            self._prepare_repo()
        except FileNotFoundError:
            raise GitNotFoundError() from None

    def _prepare_repo(self) -> None:
        if not self._path.exists():
            self._clone_repo()
        elif not self._check_repo_integrity():
            rmtree(self._path)
            self._clone_repo()

    def _run(self, args: List[str]) -> "CompletedProcess[bytes]":
        return run(args, cwd=self._path, env=self._env, stdout=PIPE, stderr=PIPE, check=True)

    def _init_repo(self) -> None:
        self._run(["git", "init"])
        self._run(["git", "remote", "add", "origin", self._url])

    def _shallow_fetch(self) -> None:
        self._run(["git", "fetch", "origin", self._revision, "--depth=1"])
        self._run(["git", "checkout", "FETCH_HEAD"])

    def _deep_fetch(self) -> None:
        try:
            self._run(["git", "fetch", "origin"])
        except CalledProcessError as error:
            raise GitCommandError(
                "'git fetch' failed, most likely due to the repo url is invalid.",
                error,
            ) from None

        try:
            self._run(["git", "checkout", self._revision])
        except CalledProcessError as error:
            raise GitCommandError(
                "'git checkout' failed, most likely due to the repo revision is invalid.",
                error,
            ) from None

    def _check_repo_integrity(self) -> bool:
        try:
            result = self._run(["git", "status", "--porcelain"])
        except CalledProcessError:
            # The git command failed means the git repo has been cleaned or broken
            return False

        return not bool(result.stdout)

    def _clone_repo(self) -> None:
        print(f"Cloning repo '{self._url}@{self._revision}'")

        path = self._path
        path.mkdir()
        try:
            self._init_repo()
            try:
                self._shallow_fetch()
            except CalledProcessError:
                self._deep_fetch()
        except (CalledProcessError, GitCommandError, FileNotFoundError):
            rmtree(path)
            raise

        print(f"Cloned to '{path}'")

    def get_root(self) -> Path:
        """Get the root directory path of the package repo.

        Returns:
            The root directory path of the package repo.

        Raises:
            TypeError: when the "ROOT.yaml" not found or more than one "ROOT.yaml" found.

        """
        roots = list(self._path.glob("**/ROOT.yaml"))

        if len(roots) == 0:
            raise TypeError("No 'ROOT.yaml' file found")
        if len(roots) >= 2:
            raise TypeError("More than one 'ROOT.yaml' file found")

        return roots[0].parent


class PackageBuilder:
    """The builder of the external Portex package.

    Arguments:
        url: The git repo url of the external package.
        revision: The git repo revision (tag/commit) of the external package.

    """

    def __init__(self, url: str, revision: str) -> None:
        self.package = ExternalPackage(url, revision)

        self._builders = self._create_type_builders()

    def __getitem__(self, key: str) -> Type["PortexExternalType"]:
        try:
            return self.package[key]
        except KeyError:
            return self._builders.__getitem__(key).build()

    def _create_type_builders(self) -> Dict[str, "TypeBuilder"]:
        repo = PackageRepo(self.package.url, self.package.revision)
        root = repo.get_root()

        builders = {}
        for yaml_file in root.glob("**/*.yaml"):
            if yaml_file.name == "ROOT.yaml":
                continue

            parts = (*yaml_file.relative_to(root).parent.parts, yaml_file.stem)
            name = ".".join(parts)
            builders[name] = TypeBuilder(name, yaml_file, self)

        return builders

    def build(self) -> ExternalPackage:
        """Build the Portex external package.

        Returns:
            The builded Portex external package.

        """
        for builder in self._builders.values():
            if builder.is_building:
                continue
            builder.build()

        return self.package


class TypeBuilder:
    """The builder of the external Portex template type.

    Arguments:
        name: The name of the Portex template type.
        path: The source file path of the Portex template type.
        package: The package the Portex template type belongs to.

    """

    def __init__(self, name: str, path: Path, builder: PackageBuilder) -> None:
        self._name = name
        self._path = path
        self._builder = builder
        self.is_building = False

    def build(self) -> Type["PortexExternalType"]:
        """Build the Portex external type.

        Returns:
            The builded Portex external type.

        Raises:
            TypeError: Raise when circular reference detected.

        """
        if self.is_building:
            raise TypeError("Circular reference")

        self.is_building = True

        with self._path.open() as fp:
            content = yaml.load(fp, yaml.Loader)

        params_pyobj = content.get("parameters", [])
        decl = content["declaration"]

        imports = BuilderImports.from_pyobj(content.get("imports", []), self._builder)

        factory = TypeFactory(decl, imports)

        keys = factory.keys
        params = Params.from_pyobj(params_pyobj)

        for key, value in params.items():
            value.ptype = keys.get(key, PTYPE.Any)

        params.add(Param("nullable", False, ptype=PTYPE.Boolean))

        class_attrs: Dict[str, Any] = {
            "__module__": __name__,
            "_factory": factory,
            "params": params,
            "package": self._builder.package,
        }
        if issubclass(factory.class_, PortexRecordBase):
            bases: Tuple[Type["PortexType"], ...] = (PortexRecordBase, PortexExternalType)
            class_attrs["_fields_factory"] = ConnectedFieldsFactory(
                decl, factory.class_, imports, factory.transform_kwargs
            )
        else:
            bases = (PortexExternalType,)
        type_ = type(self._name, bases, class_attrs)

        self._builder.package[self._name] = type_

        return type_


class BuilderImports(Imports):
    """The imports of the Portex template type.

    Arguments:
        package: The package the portex belongs to.

    """

    _builder: PackageBuilder

    def __getitem__(self, key: str) -> Type["PortexType"]:
        try:
            return super().__getitem__(key)
        except KeyError:
            return self._builder.__getitem__(key)

    @classmethod
    def from_pyobj(  # type: ignore[override] # pylint: disable=arguments-differ
        cls: Type[_I], content: List[Dict[str, Any]], builder: PackageBuilder
    ) -> _I:
        """Create :class:`Imports` instance from python list.

        Arguments:
            content: A python list representing imported types.
            builder: The package builder.

        Returns:
            A :class:`Imports` instance created from the input python list.

        """
        imports = super().from_pyobj(content)
        imports._builder = builder  # pylint: disable=protected-access
        return imports


def build_package(url: str, revision: str) -> ExternalPackage:
    """Build an external package.

    Arguments:
        url: The git repo url of the external package.
        revision: The git repo revision (tag/commit) of the external package.

    Returns:
        The :class:`ExternalPackage` instance.

    """
    builder = PackageBuilder(url, revision)
    package = builder.build()
    packages.externals[url, revision] = package
    return package
