#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Package related class."""


from itertools import chain
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Mapping, Tuple, Type, TypeVar

from graviti.utility import AttrDict, ReprMixin, ReprType, UserMapping, urlnorm

if TYPE_CHECKING:
    from graviti.portex.base import PortexType
    from graviti.portex.builtin import PortexBuiltinType
    from graviti.portex.external import PortexExternalType

_T = TypeVar("_T", bound=Type["PortexType"])
_I = TypeVar("_I", bound="Imports")


class Package(AttrDict[_T]):
    """The base class of Portex package."""

    def __setitem__(self, key: str, value: _T) -> None:
        if key in self:
            raise KeyError(f"{key} already exists in package")

        super().__setitem__(key, value)


class BuiltinPackage(Package[Type["PortexBuiltinType"]]):
    """The builtin Portex package used to manage builtin types."""


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
        url = urlnorm(url)
        try:
            package = packages.externals[url, revision]
        except KeyError:
            # TODO: Import "build" on toplevel will cause circular imports, to be solved.
            # pylint: disable=import-outside-toplevel
            from graviti.portex.builder import build_package

            package = build_package(url, revision)

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
            name = value.__name__
            type_ = {"name": name}
            if key != name:
                type_["alias"] = key
            types.append(type_)

        pyobj["types"] = types
        return pyobj


class Packages:
    """The package manager to manage different Portex packages."""

    def __init__(self) -> None:
        self.builtins = BuiltinPackage()
        self.externals: Dict[Tuple[str, str], ExternalPackage] = {}
        self.locals = LocalPackage()


packages = Packages()


class Imports(Mapping[str, Type["PortexType"]], ReprMixin):
    """The imports of the Portex template type.

    Arguments:
        package: The package the portex belongs to.

    """

    _repr_type = ReprType.MAPPING

    def __init__(self) -> None:
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

        raise KeyError(key)

    def __setitem__(self, key: str, value: Type["PortexType"]) -> None:
        type_ = self.get(key)
        if type_:
            if type_ != value:
                raise KeyError("Duplicate names")
            return

        # TODO: Import "PortexExternalType" on toplevel will cause circular imports, to be solved.
        # pylint: disable=import-outside-toplevel
        from graviti.portex.external import PortexExternalType

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
        for key, value in other.items():
            self.__setitem__(key, value)

    @classmethod
    def from_pyobj(cls: Type[_I], content: List[Dict[str, Any]]) -> _I:
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
