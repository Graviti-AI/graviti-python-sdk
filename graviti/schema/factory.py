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
    List,
    Optional,
    OrderedDict,
    Set,
    Type,
    TypeVar,
    Union,
)

import yaml

from graviti.schema.base import Param, PortexType
from graviti.schema.builtin import Field, Fields
from graviti.schema.package import Imports, packages

_C = TypeVar("_C", str, float, bool, None)


class Dynamic:
    """The base class of the runtime parameter type analyzer."""

    def __call__(self, **_: Any) -> Any:
        """Get the parameter type.

        Arguments:
            _: The input arguments.

        """
        ...


class DynamicPortexType(Dynamic):
    """The runtime parameter type analyzer for portex type."""

    def __call__(self, **_: Any) -> Type[PortexType]:
        """Get the parameter type.

        Arguments:
            _: The input arguments.

        Returns:
            The ``PortexType``.

        """
        return PortexType


class DynamicDictParameter(Dynamic):
    """The runtime parameter type analyzer for dict values.

    Arguments:
        annotation_getter: A callable object returns the type of the dict.
        key: The key of the dict value.
        decl: The full dict.

    """

    def __init__(self, annotation_getter: Dynamic, key: str, decl: Dict[str, Any]):
        self._annotation_getter = annotation_getter
        self._key = key
        self._decl = decl

    def __call__(self, **kwargs: Any) -> Any:
        """Get the parameter type.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The parameter type of the dict value.

        """
        annotation = self._annotation_getter(**kwargs)
        if annotation in {PortexType, Field}:
            if self._key in {"name", "type"} if annotation == Field else {"type"}:
                return str

            name_factory = string_factory_creator(self._decl["type"])
            class_ = packages.search_type(name_factory(**kwargs))
            for parameter in class_.params:
                if parameter.name == self._key:
                    return parameter.annotation

        return Any


class DynamicListParameter(Dynamic):
    """The runtime parameter type analyzer for list values.

    Arguments:
        annotation_getter: A callable object returns the type of the list.

    """

    def __init__(self, annotation_getter: Dynamic):
        self._annotation_getter = annotation_getter

    def __call__(self, **kwargs: Any) -> Any:
        """Get the parameter type.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The parameter type of the list value.

        """
        if self._annotation_getter(**kwargs) == Fields:
            return Field

        return Any


class Factory:
    """The base class of the template factory."""

    keys: Dict[str, Any]
    dependences: Set[Type[PortexType]]

    def __call__(self, **_: Any) -> Any:
        """Apply the input arguments to the template.

        Arguments:
            _: The input arguments.

        """
        ...


class BinaryExpression(Factory):
    """The Portex binary expression parser.

    Arguments:
        decl: A dict which indicates a portex expression.

    """

    _OPERATORS: OrderedDict[str, Callable[[Any, Any], bool]] = OrderedDict(
        {
            "==": lambda x, y: x == y,  # type: ignore[no-any-return]
            "!=": lambda x, y: x != y,  # type: ignore[no-any-return]
            ">=": lambda x, y: x >= y,  # type: ignore[no-any-return]
            "<=": lambda x, y: x <= y,  # type: ignore[no-any-return]
            ">": lambda x, y: x > y,  # type: ignore[no-any-return]
            "<": lambda x, y: x < y,  # type: ignore[no-any-return]
        }
    )

    def __init__(self, decl: str) -> None:
        keys = {}
        for operator, method in self._OPERATORS.items():
            if operator not in decl:
                continue

            operands = decl.split(operator)
            if len(operands) != 2:
                raise SyntaxError("Binary operator only accept two operands")

            factories = [string_factory_creator(operand.strip()) for operand in operands]
            for i, factory in enumerate(factories):
                if isinstance(factory, ConstantFactory):
                    factories[i] = ConstantFactory(yaml.load(factory(), yaml.Loader))
                else:
                    keys.update(factory.keys)

            self._factories = factories
            self._method = method
            self.keys = keys
            return

        raise SyntaxError("No operator found in expression")

    def __call__(self, **kwargs: Any) -> bool:
        """Apply the input arguments to the expression.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The bool result of the expression.

        """
        return self._method(*(factory(**kwargs) for factory in self._factories))


class TypeFactory(Factory):
    """The template factory for portex type.

    Arguments:
        decl: A dict which indicates a portex type.

    """

    def __init__(self, decl: Dict[str, Any], imports: Imports) -> None:
        class_ = imports[decl["type"]]

        factories = {}
        keys = {}
        dependences = {class_}

        for parameter in class_.params:
            key = parameter.name
            try:
                value = decl[key]
            except KeyError as error:
                if parameter.default == Param.empty:
                    raise KeyError(f"parameter '{key}' is required") from error
                continue

            factory = factory_creator(value, imports, parameter.annotation)
            factories[key] = factory
            keys.update(factory.keys)
            dependences.update(factory.dependences)

        self._factories = factories
        self.keys = keys
        self.dependences = dependences
        self._class = class_

    def __call__(self, **kwargs: Any) -> PortexType:
        """Apply the input arguments to the type template.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The applied Portex type.

        """
        type_kwargs = {key: factory(**kwargs) for key, factory in self._factories.items()}
        return self._class(**type_kwargs)  # type: ignore[call-arg]


class DynamicTypeFactory(Factory):
    """The template factory for dynamic Portex type.

    Arguments:
        decl: A dict which indicates a dynamic Portex type.

    """

    def __init__(self, decl: Dict[str, Any], imports: Imports) -> None:
        self._type_parameter = decl["type"][8:]
        self._decl = decl
        self._imports = imports

        self.keys = DictFactory(decl, DynamicPortexType()).keys.copy()
        self.dependences = set()
        self.keys[self._type_parameter] = str

    def __call__(self, **kwargs: Any) -> PortexType:
        """Apply the input arguments to the dynamic type template.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The applied Portex type.

        """
        decl = self._decl.copy()
        decl["type"] = kwargs[self._type_parameter]
        return TypeFactory(decl, self._imports)(**kwargs)


class ConstantFactory(Factory, Generic[_C]):
    """The template factory for a constant.

    Arguments:
        decl: The constant to be created by the factory.

    """

    def __init__(self, decl: _C) -> None:
        self._constant: _C = decl
        self.dependences = set()
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
        annotation: The parameter type.

    """

    def __init__(self, decl: str, annotation: Any = Any) -> None:
        self._key = decl
        self.dependences = set()
        self.keys = {decl: annotation}

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
        annotation: The parameter type of the list.

    """

    def __init__(self, decl: List[Any], annotation: Any = Any) -> None:
        factories = []
        dependences = set()
        keys = {}
        annotation = (
            DynamicListParameter(annotation) if isinstance(annotation, Dynamic) else annotation
        )
        for value in decl:
            factory = factory_creator(value, None, annotation)
            factories.append(factory)
            dependences.update(factory.dependences)
            keys.update(factory.keys)

        self._factories = factories
        self.dependences = dependences
        self.keys = keys

    def __call__(self, **kwargs: Any) -> List[Any]:
        """Apply the input arguments to the list template.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The applied list.

        """
        return list(factory(**kwargs) for factory in self._factories)


class DictFactory(Factory):
    """The template factory for a dict.

    Arguments:
        decl: A dict template.
        annotation: The parameter type of the dict.

    """

    def __init__(self, decl: Dict[str, Any], annotation: Any = Any) -> None:
        factories = {}
        dependences = set()
        keys = {}
        is_dynamic = isinstance(annotation, Dynamic)
        for key, value in decl.items():
            annotation = DynamicDictParameter(annotation, key, decl) if is_dynamic else annotation
            factory = factory_creator(value, None, annotation)
            factories[key] = factory
            dependences.update(factory.dependences)
            keys.update(factory.keys)

        self._factories = factories
        self.dependences = dependences
        self.keys = keys

    def __call__(self, **kwargs: Any) -> Dict[str, Any]:
        """Apply the input arguments to the dict template.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The applied dict.

        """
        return {key: factory(**kwargs) for key, factory in self._factories.items()}


class FieldFactory(Factory):
    """The template factory for a ``Field``.

    Arguments:
        decl: A dict which indicates a ``Field``.

    """

    def __init__(self, decl: Dict[str, Any], imports: Imports) -> None:
        self.creator: Callable[..., Field]

        item = decl.copy()
        dependences = set()
        keys = {}

        expression = expression_creator(item.pop("existIf", None))
        keys.update(expression.keys)

        name_factory = string_factory_creator(item.pop("name"))
        type_factory = type_factory_creator(item, imports)

        dependences.update(type_factory.dependences)
        keys.update(name_factory.keys)
        keys.update(type_factory.keys)

        self._expression = expression
        self._name_factory = name_factory
        self._type_factory = type_factory
        self.dependences = dependences
        self.keys = keys

    def __call__(self, **kwargs: Any) -> Optional[Field]:
        """Apply the input arguments to the ``Field`` template.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The applied ``Field``.

        """
        if not self._expression(**kwargs):
            return None

        return Field(self._name_factory(**kwargs), self._type_factory(**kwargs))


class FieldsFactory(Factory):
    """The template factory for a ``Fields``.

    Arguments:
        decl: A list which indicates a ``Fields``.

    """

    def __init__(self, decl: List[Dict[str, Any]], imports: Imports) -> None:
        self._factories = [FieldFactory(item, imports) for item in decl]

        dependences = set()
        keys = {}
        for factory in self._factories:
            dependences.update(factory.dependences)
            keys.update(factory.keys)

        self.dependences = dependences
        self.keys = keys

    def __call__(self, **kwargs: Any) -> Fields:
        """Apply the input arguments to the ``Fields`` template.

        Arguments:
            kwargs: The input arguments.

        Returns:
            The applied ``Fields``.

        """
        return Fields(
            filter(bool, (factory(**kwargs) for factory in self._factories)),  # type: ignore[misc]
        )


def type_factory_creator(
    decl: Dict[str, Any], imports: Imports
) -> Union[TypeFactory, DynamicTypeFactory]:
    """Check whether the input is dynamic and returns the corresponding type factory.

    Arguments:
        decl: A dict which indicates a portex type or a dynamic portex type.
        imports: The :class:`Imports` instance to specify the import scope of the template.

    Returns:
        A ``TypeFactory`` or a ``DynamicTypeFactory`` instance according to the input.

    """
    if decl["type"].startswith("$params."):
        return DynamicTypeFactory(decl, imports)

    return TypeFactory(decl, imports)


def string_factory_creator(decl: str) -> Union[VariableFactory, ConstantFactory[str]]:
    """Check whether the input string is variable and returns the corresponding factory.

    Arguments:
        decl: A string which indicates a constant or a variable.

    Returns:
        A ``VariableFactory`` or a ``ConstantFactory`` instance according to the input.

    """
    if decl.startswith("$params."):
        return VariableFactory(decl[8:], str)

    return ConstantFactory(decl)


def expression_creator(decl: Optional[str]) -> Union[BinaryExpression, ConstantFactory[bool]]:
    """Check whether the input string is binary expression and returns the corresponding factory.

    Arguments:
        decl: A string which indicates a expression.

    Returns:
        A ``BinaryExpression`` or a ``ConstantFactory`` instance according to the input.

    """
    if decl is None:
        return ConstantFactory(True)

    return BinaryExpression(decl)


def factory_creator(decl: Any, imports: Optional[Imports], annotation: Any = Any) -> Factory:
    """Check input type and returns the corresponding factory.

    Arguments:
        decl: A template which indicates any Portex object.
        imports: The :class:`Imports` instance to specify the import scope of the template.
        annotation: The parameter type of the input.

    Returns:
        A ``Factory`` instance according the input.

    """
    if isinstance(decl, str) and decl.startswith("$params."):
        return VariableFactory(decl[8:], annotation)

    if annotation == PortexType:
        assert isinstance(decl, dict)
        assert imports is not None
        return type_factory_creator(decl, imports)

    if annotation == Fields:
        assert isinstance(decl, list)
        assert imports is not None
        return FieldsFactory(decl, imports)

    if isinstance(decl, list):
        return ListFactory(decl, annotation)

    if isinstance(decl, dict):
        return DictFactory(decl, annotation)

    return ConstantFactory(decl)
