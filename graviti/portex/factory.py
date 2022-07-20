#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Template factory releated classes."""


from copy import deepcopy
from itertools import groupby
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
    Type,
    TypeVar,
    Union,
)

from graviti.portex import ptype as PTYPE
from graviti.portex.base import PortexRecordBase, PortexType
from graviti.portex.field import ConnectedFields, Fields, FrozenFields
from graviti.portex.package import Imports

_C = TypeVar("_C", str, float, bool, None)
_CFF = TypeVar("_CFF", bound="ConnectedFieldsFactory")


class Factory:
    """The base class of the template factory."""

    is_unpack: bool = False
    keys: Dict[str, Any]

    def __call__(self, _: Dict[str, Any]) -> Any:
        """Apply the input arguments to the template.

        Arguments:
            _: The input arguments.

        """
        ...


class FrozenFieldsFactory(Factory):
    """The factory for FrozenFields.

    Arguments:
        decl: The decalaration of frozen fields.
        imports: The :class:`Imports` instance to specify the import scope of the fields.

    """

    def __init__(self, decl: Iterable[Dict[str, Any]], imports: Imports) -> None:
        self._factories = [FieldFactory(field, imports) for field in decl]

    def __call__(self, kwargs: Dict[str, Any]) -> FrozenFields:
        """Apply the input arguments to the FrozenFields factory.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The applied FrozenFields.

        """
        return FrozenFields(
            filter(
                bool,
                (factory(kwargs) for factory in self._factories),  # type: ignore[misc]
            )
        )


class FrozenFieldsFactoryWrapper(Factory):
    """The factory for FrozenFields which needs kwargs transformed.

    Arguments:
        factory: The factory of frozen fields.
        kwargs_transformer: The method to transform the kwargs to the kwargs of base type.

    """

    def __init__(
        self,
        factory: Union[FrozenFieldsFactory, "FrozenFieldsFactoryWrapper"],
        kwargs_transformer: Callable[..., Dict[str, Any]],
    ) -> None:
        self._factory = factory
        self._kwargs_transformer = kwargs_transformer

    def __call__(self, kwargs: Dict[str, Any]) -> FrozenFields:
        """Apply the input arguments to the base FrozenFields factory.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The applied FrozenFields.

        """
        return self._factory(self._kwargs_transformer(kwargs))


UnionFieldsFactory = Union[
    "VariableFactory", "ConstantFactory", FrozenFieldsFactory, FrozenFieldsFactoryWrapper
]


class ConnectedFieldsFactory:
    """The factory for ConnectedFields.

    Arguments:
        decl: A dict which indicates a portex type.
        class_: The base type.
        imports: The :class:`Imports` instance to specify the import scope of the template.
        kwargs_transformer: The method to transform the kwargs to the kwargs of base type.

    """

    _factories: List[UnionFieldsFactory]

    def __init__(
        self,
        decl: Dict[str, Any],
        class_: Type[PortexRecordBase],
        imports: Imports,
        kwargs_transformer: Callable[..., Dict[str, Any]],
    ) -> None:
        factories: List[UnionFieldsFactory] = []
        for base_factory in class_._fields_factory._factories:
            if not isinstance(base_factory, VariableFactory):
                if isinstance(base_factory, ConstantFactory):
                    factories.append(base_factory)
                else:
                    factories.append(FrozenFieldsFactoryWrapper(base_factory, kwargs_transformer))
                continue

            key = base_factory.key
            if key not in decl:
                factories.append(
                    ConstantFactory(FrozenFields(deepcopy(class_.params[key].default)))
                )
                continue

            fields_decl = decl[key]
            if isinstance(fields_decl, str) and fields_decl.startswith("$"):
                factories.append(VariableFactory(fields_decl[1:]))
                continue

            groups = groupby(fields_decl, lambda x: isinstance(x, str) and x.startswith("+$"))
            for is_mutable, fields in groups:
                if is_mutable:
                    for unpack_fields in fields:
                        factories.append(VariableFactory(unpack_fields[2:]))
                else:
                    factories.append(FrozenFieldsFactory(fields, imports))

        self._factories = factories

    def __call__(self, kwargs: Dict[str, Any]) -> ConnectedFields:
        """Apply the input arguments to the ConnectedFields factory.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The applied ConnectedFields.

        """
        return ConnectedFields(factory(kwargs) for factory in self._factories)

    @classmethod
    def from_parameter_name(cls: Type[_CFF], name: str) -> _CFF:
        """Create ConnectedFieldsFactory for Fields with the given parameter name.

        Arguments:
            name: The parameter name of the input fields.

        Returns:
            The created ConnectedFieldsFactory.

        """
        obj: _CFF = object.__new__(cls)
        obj._factories = [VariableFactory(name)]
        return obj


class TypeFactory(Factory):
    """The template factory for portex type.

    Arguments:
        decl: A dict which indicates a portex type.

    """

    # After removing utility/pyarrow.py in pull request #420,
    # mypy raises error: Cannot determine type of "class_"  [has-type].
    # This is a workaround for the above mentioned error.
    class_: Type[PortexType]

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
        self.class_ = class_

    def __call__(self, kwargs: Dict[str, Any]) -> PortexType:
        """Apply the input arguments to the type template.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The applied Portex type.

        """
        return self.class_(**self.transform_kwargs(kwargs))  # type: ignore[call-arg]

    def transform_kwargs(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Transform the keyword arguments to what the base type needs.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The transformed keyword arguments.

        """
        type_kwargs: Dict[str, Any] = (
            self._unpack_factory(kwargs) if hasattr(self, "_unpack_factory") else {}
        )
        type_kwargs.update({key: factory(kwargs) for key, factory in self._factories.items()})
        return type_kwargs


class ConstantFactory(Factory, Generic[_C]):
    """The template factory for a constant.

    Arguments:
        decl: The constant to be created by the factory.

    """

    def __init__(self, decl: _C) -> None:
        self._constant: _C = decl
        self.keys: Dict[str, Any] = {}

    def __call__(self, _: Dict[str, Any]) -> _C:
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
        self.key = decl
        self.keys = {decl: ptype}
        self.is_unpack = is_unpack

    def __call__(self, kwargs: Dict[str, Any]) -> Any:
        """Apply the input arguments to the variable template.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The applied variable.

        """
        return kwargs[self.key]


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

    def __call__(self, kwargs: Dict[str, Any]) -> List[Any]:
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

    def __call__(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Apply the input arguments to the dict template.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The applied dict.

        """
        items: Dict[str, Any] = (
            self._unpack_factory(kwargs) if hasattr(self, "_unpack_factory") else {}
        )
        items.update({key: factory(kwargs) for key, factory in self._factories.items()})
        return items


class FieldFactory(Factory):
    """The template factory for a tuple of name and PortexType.

    Arguments:
        decl: A dict which indicates a tuple of name and PortexType.

    """

    def __init__(self, decl: Dict[str, Any], imports: Imports) -> None:
        self.creator: Callable[..., Tuple[str, PortexType]]
        if "+" in decl and len(decl) == 1:
            raise ValueError(
                "Use object unpack for entire record field is not allowed. "
                "Please use list unpack for fields"
            )

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

    def __call__(self, kwargs: Dict[str, Any]) -> Optional[Tuple[str, PortexType]]:
        """Apply the input arguments to the template.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The applied tuple of name and PortexType.

        """
        if not self._condition(kwargs):
            return None

        return self._name_factory(kwargs), self._type_factory(kwargs)


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

    def __call__(self, kwargs: Dict[str, Any]) -> Fields:
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
        item = factory(kwargs)
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
