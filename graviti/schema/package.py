#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Package related class."""


from typing import TYPE_CHECKING, Callable, Dict, Iterator, Mapping, Optional, Tuple, Type, TypeVar

if TYPE_CHECKING:
    from graviti.schema.base import PortexType
    from graviti.schema.template import PortexExternalType

_T = TypeVar("_T", bound=Type["PortexType"])
_S = TypeVar("_S", bound=Type["PortexType"])


class Package(Mapping[str, _T]):
    """The base class of Portex package."""

    def __init__(self) -> None:
        self._data: Dict[str, _T] = {}

    def __getitem__(self, key: str) -> _T:
        return self._data.__getitem__(key)

    def __len__(self) -> int:
        return self._data.__len__()

    def __iter__(self) -> Iterator[str]:
        return self._data.__iter__()

    def __setitem__(self, key: str, value: _T) -> None:
        if key in self._data:
            raise KeyError(f"{key} already exists in package")
        value.name = key
        self._data[key] = value


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
            self._data[name] = class_
            return class_

        return register


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

    def __setitem__(self, key: str, value: Type["PortexExternalType"]) -> None:
        super().__setitem__(key, value)
        value.repo = self.repo
        value.version = self.version


class LocalPackage(Package[Type["PortexType"]]):
    """The local Portex package used to manage local types."""


class PackageManager:
    """The package manager to manage different Portex packages."""

    def __init__(self) -> None:
        self.builtin_package = BuiltinPackage()
        self.external_packages: Dict[Tuple[str, str], ExternalPackage] = {}
        self.local_package = LocalPackage()

    def create_package(self, repo: str, version: str) -> ExternalPackage:
        """Create an empty external package.

        Arguments:
            repo: The git repo url of the external package.
            version: The git repo version (tag/commit) of the external package.

        Returns:
            The :class:`ExternalPackage` instance.

        """
        package = ExternalPackage(repo, version)
        self.external_packages[repo, version] = package
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
        if name in self.builtin_package:
            return self.builtin_package[name]

        try:
            external_package = self.external_packages[repo, version]  # type: ignore[index]
            return external_package[name]
        except KeyError:
            pass

        try:
            return self.local_package[name]
        except KeyError as error:
            raise KeyError("Type not found") from error


package_manager = PackageManager()
