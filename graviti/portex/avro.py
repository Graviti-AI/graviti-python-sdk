#!/usr/bin/env python3

# pylint: disable-all
# type: ignore
# flake8: noqa

"""Code converting PyArrow schema to Avro Schema."""

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
    timestamp,
)

_REMOTE_FILE_FIELD_NAMES = {"checksum", "url"}

_PRIMITIVE_TYPES = {
    int32: "int",
    int64: "long",
    float32: "float",
    float64: "double",
    boolean: "boolean",
    string: "string",
    binary: "bytes",
}


class AvroSchema:
    def __init__(self):
        pass

    def to_json(self):
        return {}


class AvroField:
    def __init__(
        self,
        name_registry,
        typ: AvroSchema,
        name,
        optional=True,
        has_default=False,
        default=None,
    ):
        self._type = typ
        self._name = name
        self._optional = optional
        self._has_default = has_default
        self._default = default

    def to_json(self):
        if self._optional:
            result = {
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
    def __init__(self, typ, has_default=False, default=None):
        super().__init__()
        self._default = default
        self._has_default = has_default
        self._type = typ

    def to_json(self):
        if self._has_default:
            return {"type": self._type, "default": self._default}
        else:
            return self._type


class AvroRecordSchema(AvroSchema):
    def __init__(self, name_registry, name, namespace, fields: [], aliases=None):
        super().__init__()
        self._name = name
        self._namespace = namespace
        self._fields = fields

        self._aliases = aliases
        if self._aliases is None:
            self._aliases = []

    def to_json(self):
        return {
            "type": "record",
            "name": self._name,
            "namespace": self._namespace,
            "aliases": self._aliases,
            "fields": [field.to_json() for field in self._fields],
        }


class AvroArraySchema(AvroSchema):
    def __init__(self, items: AvroSchema):
        super().__init__()
        self._items = items

    def to_json(self):
        return {
            "type": "array",
            "items": self._items.to_json(),
            "default": [],
        }


class AvroDateSchema(AvroSchema):
    def to_json(self):
        return {
            "type": "int",
            "logicalType": "portex.date",
        }


class AvroTimestampSchema(AvroSchema):
    def __init__(self, unit: str, tz: str):
        super().__init__()
        self._unit = unit
        self._tz = tz

    def to_json(self):
        return {
            "type": "long",
            "logicalType": "portex.timestamp",
            "unit": self._unit,
            "tz": self._tz,
        }


class AvroEnumSchema(AvroSchema):
    def __init__(self, values):
        super().__init__()
        self._values = values

    def to_json(self):
        min_index, max_index = self._values.index_scope
        avro_type = "int" if min_index >= -2147483648 and max_index <= 2147483647 else "long"

        return {
            "type": avro_type,
            "logicalType": "portex.enum",
            "values": self._values.to_pyobj(),
        }


def _on_list(names, namespace, name, _pa_ist: array) -> AvroArraySchema:
    sub_namespace = f"{namespace}.{name}.items"
    sub_name = "items"
    items = _on_type(names, sub_namespace, sub_name, _pa_ist.items.to_builtin())
    return AvroArraySchema(items=items)


def _on_primitive(portex_type: PortexType) -> AvroSchema:
    try:
        return AvroPrimitiveSchema(typ=_PRIMITIVE_TYPES[type(portex_type)])
    except KeyError:
        raise Exception(f"unsupported type {portex_type}") from None


def _on_struct(names, namespace, name, _struct: record) -> AvroRecordSchema:
    avro_record_fields = list()
    skip_url = False

    # remove "url" field in avro schema
    if set(_struct.keys()) == _REMOTE_FILE_FIELD_NAMES:
        skip_url = True

    for sub_name, sub_type in _struct.items():
        if skip_url and sub_name == "url":
            continue

        sub_type = sub_type.to_builtin()
        sub_namespace = f"{namespace}.{name}"
        sub_schema = _on_type(names, sub_namespace, sub_name, sub_type)
        avro_record_field = AvroField(
            typ=sub_schema,
            name=sub_name,
            has_default=False,
            name_registry=names,
        )
        avro_record_fields.append(avro_record_field)

    return AvroRecordSchema(
        name=name, namespace=namespace, fields=avro_record_fields, name_registry=names
    )


def _on_enum(name_registry, namespace, name, _filed: enum) -> AvroPrimitiveSchema:
    return AvroEnumSchema(_filed.values)


def _on_timestamp(names, namespace, name, _timestamp_type: timestamp) -> AvroPrimitiveSchema:
    return AvroTimestampSchema(_timestamp_type)


def _on_date(names, namespace, name, _date_type: date) -> AvroDateSchema:
    return AvroDateSchema(_date_type)


def _on_type(names, namespace, name, _portex_type):
    typ = type(_portex_type)
    if typ in _COMPLEX_TYPES_PROCESSERS:
        return _COMPLEX_TYPES_PROCESSERS[typ](names, namespace, name, _portex_type)

    return _on_primitive(_portex_type)


def convert_portex_schema_to_avro(_schema: record):
    avro_schema = _on_struct([], "cn.graviti.portex", "root", _schema)
    return avro_schema.to_json()


_COMPLEX_TYPES_PROCESSERS = {
    record: _on_struct,
    array: _on_list,
    enum: _on_enum,
    date: _on_date,
    timestamp: _on_timestamp,
}
