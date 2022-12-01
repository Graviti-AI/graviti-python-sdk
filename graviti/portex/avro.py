#!/usr/bin/env python3

# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# flake8: noqa

"""Code converting PyArrow schema to Avro Schema."""

from typing import Any, Dict, Type

from graviti.portex.base import PortexType
from graviti.portex.builtin import (
    array,
    binary,
    boolean,
    date,
    enum,
    float32,
    float64,
    int32,
    int64,
    record,
    string,
    time,
    timedelta,
    timestamp,
)


class AvroSchema:
    def __init__(self, name: str, namespace: str, portex_type: PortexType) -> None:
        raise NotImplementedError

    def to_json(self) -> Dict[str, Any]:
        raise NotImplementedError


class AvroField:
    def __init__(
        self,
        type_: AvroSchema,
        name: str,
        *,
        optional: bool = True,
        has_default: bool = False,
        default: Any = None,
    ) -> None:
        self._type = type_
        self._name = name
        self._optional = optional
        self._has_default = has_default
        self._default = default

    def to_json(self) -> Dict[str, Any]:
        if self._optional:
            result: Dict[str, Any] = {
                "name": self._name,
                "type": [
                    "null",
                    self._type.to_json(),
                ],
            }
        else:
            result = {
                "name": self._name,
                "type": self._type.to_json(),
            }

        if self._has_default:
            result["default"] = self._default

        return result


class AvroPrimitiveSchema(AvroSchema):
    _PRIMITIVE_TYPES: Dict[Type[PortexType], str] = {
        int32: "int",
        int64: "long",
        float32: "float",
        float64: "double",
        boolean: "boolean",
        string: "string",
        binary: "bytes",
    }

    def __init__(self, name: str, namespace: str, portex_type: PortexType):
        try:
            self._type = self._PRIMITIVE_TYPES[type(portex_type)]
        except KeyError:
            raise Exception(f"unsupported type {portex_type}") from None

    def to_json(self) -> str:  # type: ignore[override]
        return self._type


class AvroRecord(AvroSchema):
    def __init__(self, name: str, namespace: str, portex_type: record):
        self._name = name
        self._namespace = namespace
        self._fields = [
            AvroField(_avro_schema_creator(key, f"{namespace}.{name}", value), key)
            for key, value in portex_type.items()
        ]

    def to_json(self) -> Dict[str, Any]:
        return {
            "type": "record",
            "name": self._name,
            "namespace": self._namespace,
            "fields": [field.to_json() for field in self._fields],
        }


class AvroArray(AvroSchema):
    def __init__(self, name: str, namespace: str, portex_type: array):
        self._items = _avro_schema_creator("items", f"{namespace}.{name}.items", portex_type.items)

    def to_json(self) -> Dict[str, Any]:
        return {
            "type": "array",
            "items": self._items.to_json(),
            "default": [],
        }


class PortexEnum(AvroSchema):
    def __init__(self, name: str, namespace: str, portex_type: enum):
        self._values = portex_type.values

    def to_json(self) -> Dict[str, Any]:
        min_index, max_index = self._values.index_scope
        avro_type = "int" if min_index >= -2147483648 and max_index <= 2147483647 else "long"

        return {
            "type": avro_type,
            "logicalType": "portex.enum",
            "values": self._values.to_pyobj(),
        }


class PortexDate(AvroSchema):
    def __init__(self, name: str, namespace: str, portex_type: PortexType) -> None:
        pass

    def to_json(self) -> Dict[str, Any]:
        return {
            "type": "int",
            "logicalType": "portex.date",
        }


class PortexTime(AvroSchema):
    _AVRO_TYPES = {
        "s": "int",
        "ms": "int",
        "us": "long",
        "ns": "long",
    }

    def __init__(self, name: str, namespace: str, portex_type: time) -> None:
        self._unit = portex_type.unit

    def to_json(self) -> Dict[str, Any]:
        return {
            "type": self._AVRO_TYPES[self._unit],
            "logicalType": "portex.time",
            "unit": self._unit,
        }


class PortexTimestamp(AvroSchema):
    def __init__(self, name: str, namespace: str, portex_type: timestamp):
        self._unit = portex_type.unit
        self._tz = portex_type.tz

    def to_json(self) -> Dict[str, Any]:
        return {
            "type": "long",
            "logicalType": "portex.timestamp",
            "unit": self._unit,
            "tz": self._tz,
        }


class PortexTimedelta(AvroSchema):
    def __init__(self, name: str, namespace: str, portex_type: timedelta) -> None:
        self._unit = portex_type.unit

    def to_json(self) -> Dict[str, Any]:
        return {
            "type": "long",
            "logicalType": "portex.timedelta",
            "unit": self._unit,
        }


_COMPLEX_TYPE_PROCESSERS: Dict[Type[PortexType], Type[AvroSchema]] = {
    record: AvroRecord,
    array: AvroArray,
    enum: PortexEnum,
    date: PortexDate,
    time: PortexTime,
    timestamp: PortexTimestamp,
    timedelta: PortexTimedelta,
}


def _avro_schema_creator(name: str, namespace: str, portex_type: PortexType) -> AvroSchema:
    builtin_portex_type = portex_type.to_builtin()
    processer = _COMPLEX_TYPE_PROCESSERS.get(type(builtin_portex_type), AvroPrimitiveSchema)
    return processer(name, namespace, builtin_portex_type)


def convert_portex_schema_to_avro(portex_type: record) -> Dict[str, Any]:
    return AvroRecord("root", "cn.graviti.portex", portex_type).to_json()
