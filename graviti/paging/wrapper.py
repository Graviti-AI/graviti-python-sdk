#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""PyArrow array wrapper related class."""

from typing import Any, ClassVar, Dict, Iterator, Type, TypeVar, Union, overload

import pyarrow as pa

_A = TypeVar("_A", bound="ArrayWrapper")
_S = TypeVar("_S", bound="StructScalarWrapper")
_LS = TypeVar("_LS", bound="ListScalarWrapper")
_LA = TypeVar("_LA", bound="ListArrayWrapper")


class WrapperRegister:
    """The class decorator to connect pyarrow type and the pyarrow array wrapper.

    Arguments:
        pyarrow_type_id: The PyArrow type id.

    """

    _WRAPPERS: Dict[int, Type["ArrayWrapper"]] = {}
    _default_wrapper: Any = lambda x: x
    _default_wrapper.scalar = _default_wrapper

    def __init__(self, pyarrow_type_id: int) -> None:
        self._pyarrow_type_id = pyarrow_type_id

    def __call__(self, wrapper: Type[_A]) -> Type[_A]:
        """Connect pyarrow array wrapper with the pyarrow type id.

        Arguments:
            wrapper: The pyarrow array wrapper needs to be connected.

        Returns:
            The input wrapper class unchanged.

        """
        self._WRAPPERS[self._pyarrow_type_id] = wrapper

        return wrapper

    @classmethod
    def get(cls, pyarrow_type_id: int) -> Type["ArrayWrapper"]:
        """Get the corresponding registered pyarrow array wrapper.

        Arguments:
            pyarrow_type_id: The PyArrow type id.

        Returns:
            The corresponding registered pyarrow array wrapper.

        """
        return cls._WRAPPERS.get(pyarrow_type_id, cls._default_wrapper)


class ScalarWrapper:
    """The wrapper of pyarrow scalar.

    Arguments:
        scalar: The PyArrow scalar needs to be wrapped.

    """

    __slots__ = ("_scalar",)

    def __init__(self, scalar: pa.scalar) -> None:
        self._scalar = scalar

    @property
    def is_valid(self) -> bool:
        """The wrapper of pyarrow Scalar.is_valid method.

        Returns:
            Bool value indicating whether this scalar is None.

        """
        return self._scalar.is_valid  # type: ignore[no-any-return]

    def as_py(self) -> Any:
        """The wrapper of pyarrow Scalar.as_py method.

        Returns:
            Return this value as a Python builtin object.

        """
        return self._scalar.as_py()


class ArrayWrapper:
    """The wrapper of pyarrow array.

    Arguments:
        array: The PyArrow array needs to be wrapped.

    """

    __slots__ = ("_array",)

    scalar: ClassVar[Type[ScalarWrapper]]

    def __init__(self, array: pa.Array) -> None:
        self._array = array

    def __len__(self) -> int:
        return len(self._array)


class StructScalarWrapper(ScalarWrapper):
    """The wrapper of pyarrow StructScalar to make it case insensitive.

    Arguments:
        scalar: The PyArrow StructScalar needs to be wrapped.

    """

    __slots__ = ("_scalar", "_wrappers")

    def __init__(self, scalar: pa.StructScalar) -> None:  # pylint: disable=super-init-not-called
        self._scalar = scalar
        self._wrappers = {field.name: WrapperRegister.get(field.type.id) for field in scalar.type}

    @classmethod
    def from_wrapper(
        cls: Type[_S], scalar: pa.ListScalar, wrappers: Dict[str, Type[ArrayWrapper]]
    ) -> _S:
        """Create StructScalarWrapper instance by inputing scalar and wrappers.

        Arguments:
            scalar: The PyArrow StructScalar needs to be wrapped.
            wrappers: The wrappers of the input scalar.

        Returns:
            The StructScalarWrapper instance created by the input scalar and wrapper.

        """
        obj: _S = object.__new__(cls)
        obj._scalar = scalar
        obj._wrappers = wrappers

        return obj

    def __getitem__(self, key: str) -> ScalarWrapper:
        lower_key = key.lower()
        result = self._scalar[lower_key]
        wrapper = self._wrappers[key]

        return wrapper.scalar(result)


@WrapperRegister(pa.lib.Type_STRUCT)  # pylint: disable=c-extension-no-member
class StructArrayWrapper(ArrayWrapper):
    """The wrapper of pyarrow StructArray to make it case insensitive.

    Arguments:
        array: The PyArrow StructArray instance needs to be wrapped.

    """

    __slots__ = ("_array", "_wrappers")

    scalar = StructScalarWrapper

    def __init__(self, array: pa.StringArray) -> None:  # pylint: disable=super-init-not-called
        self._array = array
        self._wrappers = {field.name: WrapperRegister.get(field.type.id) for field in array.type}

    def __getitem__(self, index: int) -> "StructScalarWrapper":
        return StructScalarWrapper.from_wrapper(self._array[index], self._wrappers)

    def __iter__(self) -> Iterator["StructScalarWrapper"]:
        return (StructScalarWrapper.from_wrapper(item, self._wrappers) for item in self._array)

    def field(self, key: str) -> pa.Array:
        """The wrapper of pyarrow StructArray.field method.

        Arguments:
            key: The name of the field.

        Returns:
            The child array belonging to the field.

        """
        lower_key = key.lower()
        result = self._array.field(lower_key)
        return self._wrappers[lower_key](result)


class ListScalarWrapper(ScalarWrapper):
    """The wrapper of pyarrow ListScalar to make it case insensitive.

    Arguments:
        scalar: The PyArrow ListScalar instance needs to be wrapped.

    """

    __slots__ = ("_scalar", "_wrapper")

    def __init__(self, scalar: pa.ListScalar) -> None:  # pylint: disable=super-init-not-called
        self._scalar = scalar
        self._wrapper = WrapperRegister.get(scalar.type.value_type.id)

    @classmethod
    def from_wrapper(cls: Type[_LS], scalar: pa.ListScalar, wrapper: Type[ArrayWrapper]) -> _LS:
        """Create ListScalarWrapper instance by inputing scalar and wrapper.

        Arguments:
            scalar: The PyArrow ListScalar instance needs to be wrapped.
            wrapper: The wrapper of the input scalar.

        Returns:
            The ListScalarWrapper instance created by the input scalar and wrapper.

        """
        obj: _LS = object.__new__(cls)
        obj._scalar = scalar
        obj._wrapper = wrapper

        return obj

    @property
    def values(self) -> ArrayWrapper:
        """The wrapper of pyarrow ListScalar.values attr.

        Returns:
            The internal values of the pyarrow scalar.

        """
        return self._wrapper(self._scalar.values)


@WrapperRegister(pa.lib.Type_LIST)  # pylint: disable=c-extension-no-member
class ListArrayWrapper(ArrayWrapper):
    """The wrapper of pyarrow ListArray to make it case insensitive.

    Arguments:
        array: The PyArrow ListScalar instance needs to be wrapped.

    """

    __slots__ = ("_array", "_wrapper")

    scalar = ListScalarWrapper

    def __init__(self, array: pa.ListArray) -> None:  # pylint: disable=super-init-not-called
        self._array = array
        self._wrapper = WrapperRegister.get(array.type.value_type.id)

    @overload
    def __getitem__(self, index: int) -> ListScalarWrapper:
        ...

    @overload
    def __getitem__(self: _LA, index: slice) -> _LA:
        ...

    def __getitem__(self: _LA, index: Union[int, slice]) -> Union[ListScalarWrapper, _LA]:
        if isinstance(index, int):
            return ListScalarWrapper.from_wrapper(self._array[index], self._wrapper)

        return self.from_wrapper(self._array[index], self._wrapper)

    def __iter__(self) -> Iterator[ListScalarWrapper]:
        return (ListScalarWrapper.from_wrapper(item, self._wrapper) for item in self._array)

    @classmethod
    def from_wrapper(cls: Type[_LA], array: pa.ListArray, wrapper: Type[ArrayWrapper]) -> _LA:
        """Create ListScalarWrapper instance by inputing scalar and wrapper.

        Arguments:
            array: The PyArrow ListArray instance needs to be wrapped.
            wrapper: The wrapper of the input array.

        Returns:
            The ListScalarWrapper instance created by the input scalar and wrapper.

        """
        obj: _LA = object.__new__(cls)
        obj._array = array
        obj._wrapper = wrapper

        return obj
