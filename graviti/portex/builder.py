#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Portex type builder related classes."""


from hashlib import md5
from pathlib import Path
from subprocess import PIPE, run
from tempfile import gettempdir
from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar

import yaml

import graviti.portex.ptype as PTYPE
from graviti.portex.external import PortexExternalType
from graviti.portex.factory import type_factory_creator
from graviti.portex.package import ExternalPackage, Imports, packages
from graviti.portex.param import Param, Params
from graviti.portex.register import ExternalContainerRegister

if TYPE_CHECKING:
    from graviti.portex.base import PortexType


EXTERNAL_TYPE_TO_CONTAINER = ExternalContainerRegister.EXTERNAL_TYPE_TO_CONTAINER

_I = TypeVar("_I", bound="BuilderImports")


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
        package = self.package
        repo = package.repo

        temp_path = Path(gettempdir()) / "portex"
        temp_path.mkdir(exist_ok=True)

        md5_instance = md5()
        md5_instance.update(repo.encode("utf-8"))
        checksum = md5_instance.hexdigest()

        repo_path = temp_path / checksum

        if not repo_path.exists():
            print(f"Cloning repo '{repo}'")
            run(
                ["git", "clone", package.url, "--depth=1", "-b", package.revision, repo_path],
                stdout=PIPE,
                stderr=PIPE,
                check=True,
            )
            print(f"Cloned to '{repo_path}'")

        roots = list(repo_path.glob("**/ROOT.yaml"))

        if len(roots) == 0:
            raise TypeError("No 'ROOT.yaml' file found")
        if len(roots) >= 2:
            raise TypeError("More than one 'ROOT.yaml' file found")

        builders = {}
        root_dir = roots[0].parent
        for yaml_file in root_dir.glob("**/*.yaml"):
            if yaml_file.name == "ROOT.yaml":
                continue

            parts = (*yaml_file.relative_to(root_dir).parent.parts, yaml_file.stem)
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

        factory = type_factory_creator(decl, imports)

        keys = factory.keys
        params = Params.from_pyobj(params_pyobj)

        for key, value in params.items():
            value.ptype = keys.get(key, PTYPE.Any)

        params.add(Param("nullable", False, ptype=PTYPE.Boolean))

        package = self._builder.package

        type_: Type[PortexExternalType] = type(
            self._name,
            (PortexExternalType,),
            {"params": params, "factory": factory, "package": package},
        )

        package[self._name] = type_

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


def build(url: str, revision: str) -> ExternalPackage:
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


def build_openbytes(revision: str) -> ExternalPackage:
    """Build the OpenBytes standard external package.

    The repo url is: https://github.com/Project-OpenBytes/standard.

    Arguments:
        revision: The git repo revision (tag/commit) of the external package.

    Returns:
        The :class:`ExternalPackage` instance.

    """
    return build("https://github.com/Project-OpenBytes/standard", revision)
