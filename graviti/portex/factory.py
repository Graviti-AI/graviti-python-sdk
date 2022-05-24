#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Template factory releated classes."""


from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import graviti.portex.ptype as PTYPE
from graviti.portex.base import PortexType
from graviti.portex.field import Fields
from graviti.portex.package import Imports

_C = TypeVar("_C", str, float, bool, None)


class Factory:
    """The base class of the template factory."""

    is_unpack: bool = False
    keys: Dict[str, Any]

    def __call__(self, **_: Any) -> Any:
        """Apply the input arguments to the template.

        Arguments:
            _: The input arguments.

        """
        ...


class TypeFactory(Factory):
    """The template factory for portex type.

    Arguments:
        decl: A dict which indicates a portex type.

    """

    def __init__(self, decl: Dict[str, Any], imports: Imports) -> None:
        class_ = imports[decl["type"]]

        factories = {}
        keys = {}

        for name, parameter in class_.params.items():
            try:
                value = decl[name]
            except KeyError as error:
                if parameter.required:
                    raise KeyError(f"Parameter '{name}' is required") from error
                continue

            factory = factory_creator(value, imports, parameter.ptype)
            if factory.is_unpack:
                raise ValueError("Use array unpack in object value is not allowed")
            factories[name] = factory
            keys.update(factory.keys)

        if "+" in decl:
            unpack_factory = mapping_unpack_factory_creator(decl["+"], PTYPE.Mapping)
            keys.update(unpack_factory.keys)
            self._unpack_factory = unpack_factory

        self._factories = factories
        self.keys = keys
        self._class = class_

    def __call__(self, **kwargs: Any) -> PortexType:
        """Apply the input arguments to the type template.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The applied Portex type.

        """
        type_kwargs: Dict[str, Any] = (
            self._unpack_factory(**kwargs) if hasattr(self, "_unpack_factory") else {}
        )
        type_kwargs.update({key: factory(**kwargs) for key, factory in self._factories.items()})
        return self._class(**type_kwargs)


class ConstantFactory(Factory, Generic[_C]):
    """The template factory for a constant.

    Arguments:
        decl: The constant to be created by the factory.

    """

    def __init__(self, decl: _C) -> None:
        self._constant: _C = decl
        self.keys: Dict[str, Any] = {}

    def __call__(self, **_: Any) -> _C:
        """Get the constant stored in the factory.

        Arguments:
            _: The input arguments.

        Returns:
            The constant stored in the factory.

        """
        return self._constant


class VariableFactory(Factory):
    """The template factory for a variable.

    Arguments:
        decl: The parameter name of the variable.
        ptype: The parameter type.

    """

    def __init__(self, decl: str, ptype: PTYPE.PType = PTYPE.Any, is_unpack: bool = False) -> None:
        self._key = decl
        self.keys = {decl: ptype}
        self.is_unpack = is_unpack

    def __call__(self, **kwargs: Any) -> Any:
        """Apply the input arguments to the variable template.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The applied variable.

        """
        return kwargs[self._key]


class ListFactory(Factory):
    """The template factory for a list.

    Arguments:
        decl: A list template.
        ptype: The parameter type of the list.

    """

    def __init__(self, decl: List[Any], ptype: PTYPE.PType = PTYPE.Any) -> None:
        factories = []
        keys = {}
        for value in decl:
            factory = factory_creator(value, None, ptype)
            factories.append(factory)
            keys.update(factory.keys)

        self._factories = factories
        self.keys = keys

    def __call__(self, **kwargs: Any) -> List[Any]:
        """Apply the input arguments to the list template.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The applied list.

        """
        return list(_generate_factory_items(self._factories, kwargs))


class DictFactory(Factory):
    """The template factory for a dict.

    Arguments:
        decl: A dict template.
        ptype: The parameter type of the dict.

    """

    def __init__(self, decl: Dict[str, Any], ptype: PTYPE.PType = PTYPE.Any) -> None:
        factories = {}
        keys = {}

        for key, value in decl.items():
            if key == "+":
                unpack_factory = mapping_unpack_factory_creator(value, PTYPE.Mapping)
                keys.update(unpack_factory.keys)
                self._unpack_factory = unpack_factory
                continue

            factory = factory_creator(value, None, ptype)
            if factory.is_unpack:
                raise ValueError("Use array unpack in object value is not allowed")
            factories[key] = factory
            keys.update(factory.keys)

        self._factories = factories
        self.keys = keys

    def __call__(self, **kwargs: Any) -> Dict[str, Any]:
        """Apply the input arguments to the dict template.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The applied dict.

        """
        items: Dict[str, Any] = (
            self._unpack_factory(**kwargs) if hasattr(self, "_unpack_factory") else {}
        )
        items.update({key: factory(**kwargs) for key, factory in self._factories.items()})
        return items


class FieldFactory(Factory):
    """The template factory for a tuple of name and PortexType.

    Arguments:
        decl: A dict which indicates a tuple of name and PortexType.

    """

    def __init__(self, decl: Dict[str, Any], imports: Imports) -> None:
        self.creator: Callable[..., Tuple[str, PortexType]]

        item = decl.copy()
        keys = {}

        condition = string_factory_creator(item.pop("exist_if", True))
        keys.update(condition.keys)

        name_factory = string_factory_creator(item.pop("name"), PTYPE.String)
        type_factory = type_factory_creator(item, imports)

        keys.update(name_factory.keys)
        keys.update(type_factory.keys)

        self._condition = condition
        self._name_factory = name_factory
        self._type_factory = type_factory
        self.keys = keys

    def __call__(self, **kwargs: Any) -> Optional[Tuple[str, PortexType]]:
        """Apply the input arguments to the template.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The applied tuple of name and PortexType.

        """
        if self._condition(**kwargs) is None:
            return None

        return self._name_factory(**kwargs), self._type_factory(**kwargs)


class FieldsFactory(Factory):
    """The template factory for a ``Fields``.

    Arguments:
        decl: A list which indicates a ``Fields``.

    """

    def __init__(self, decl: List[Union[Dict[str, Any], str]], imports: Imports) -> None:
        self._factories = []
        keys = {}

        for item in decl:
            if isinstance(item, dict):
                factory: Factory = FieldFactory(item, imports)
            elif isinstance(item, str) and item.startswith("+$"):
                factory = VariableFactory(item[2:], PTYPE.Fields, True)
            else:
                raise ValueError("The items of fields can only be object or list unpack parameter")

            keys.update(factory.keys)
            self._factories.append(factory)

        self.keys = keys

    def __call__(self, **kwargs: Any) -> Fields:
        """Apply the input arguments to the ``Fields`` template.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The applied ``Fields``.

        """
        return Fields(
            filter(
                bool,
                _generate_factory_items(self._factories, kwargs, lambda x: x.items()),
            )
        )


def _generate_factory_items(
    factories: Iterable[Factory],
    kwargs: Dict[str, Any],
    unpack_item_processer: Callable[[Any], Any] = lambda x: x,
) -> Iterator[Any]:
    for factory in factories:
        item = factory(**kwargs)
        if factory.is_unpack:
            yield from unpack_item_processer(item)
        else:
            yield item


def mapping_unpack_factory_creator(decl: str, ptype: PTYPE.PType) -> VariableFactory:
    """Check the object unpack grammar and returns the corresponding factory.

    Arguments:
        decl: The parameter decalaration.
        ptype: The parameter type of the input.

    Raises:
        ValueError: When the object unpack grammar is incorrect.

    Returns:
        A ``VariableFactory`` instance according to the input.

    """
    if not decl.startswith("$"):
        raise ValueError("The decl does not have the correct object unpack grammar")

    return VariableFactory(decl[1:], ptype)


def type_factory_creator(
    decl: Dict[str, Any], imports: Imports
) -> Union[TypeFactory, VariableFactory]:
    """Check the input and returns the corresponding type factory.

    Arguments:
        decl: A dict which indicates a portex type or has object unpack grammar.
        imports: The :class:`Imports` instance to specify the import scope of the template.

    Raises:
        ValueError: When setting the type name as a parameter.

    Returns:
        A ``TypeFactory`` or a ``VariableFactory`` instance.

    """
    if "type" not in decl:
        return mapping_unpack_factory_creator(decl["+"], PTYPE.PortexType)

    if decl["type"].startswith("$"):
        raise ValueError(
            "Setting the type name as a parameter is not allowed. Please use object unpack grammar"
        )

    return TypeFactory(decl, imports)


def string_factory_creator(
    decl: str, ptype: PTYPE.PType = PTYPE.Any
) -> Union[VariableFactory, ConstantFactory[str]]:
    """Check whether the input string is variable and returns the corresponding factory.

    Arguments:
        decl: A string which indicates a constant or a variable.
        ptype: The parameter type of the string.

    Returns:
        A ``VariableFactory`` or a ``ConstantFactory`` instance according to the input.

    """
    if isinstance(decl, str) and decl.startswith("$"):
        return VariableFactory(decl[1:], ptype)

    return ConstantFactory(decl)


def factory_creator(  # pylint: disable=too-many-return-statements
    decl: Any, imports: Optional[Imports], ptype: PTYPE.PType = PTYPE.Any
) -> Factory:
    """Check input type and returns the corresponding factory.

    Arguments:
        decl: A template which indicates any Portex object.
        imports: The :class:`Imports` instance to specify the import scope of the template.
        ptype: The parameter type of the input.

    Returns:
        A ``Factory`` instance according the input.

    """
    if isinstance(decl, str):
        if decl.startswith("$"):
            return VariableFactory(decl[1:], ptype)

        if decl.startswith("+$"):
            return VariableFactory(decl[2:], PTYPE.Array, True)

    if ptype == PTYPE.PortexType:
        assert isinstance(decl, dict)
        assert imports is not None
        return type_factory_creator(decl, imports)

    if ptype == PTYPE.Fields:
        assert isinstance(decl, list)
        assert imports is not None
        return FieldsFactory(decl, imports)

    if isinstance(decl, list):
        return ListFactory(decl, ptype)

    if isinstance(decl, dict):
        return DictFactory(decl, ptype)

    return ConstantFactory(decl)
