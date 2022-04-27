#!/usr/bin/env python3

# pylint: disable-all
# type: ignore
# flake8: noqa

"""Code converting PyArrow schema to Avro Schema."""


import pyarrow as pa


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
        index,
        optional=False,
        has_default=False,
        default=None,
    ):
        self._type = typ
        self._name = name
        self._index = index
        self._optional = optional
        self._has_default = has_default
        self._default = default

    def to_json(self):
        if self._optional:
            result = {
                "name": self._name,
                "type": [
                    None,
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


class AvroFixedSchema(AvroSchema):
    def __init__(
        self,
        name_registry,
        name,
        namespace,
        size,
    ):
        super().__init__()
        self._name = name
        self._namespace = namespace
        self._size = size

    def to_json(self):
        return {
            "type": "fixed",
            "size": self._size,
            "name": self._name,
            "namespace": self._namespace,
        }


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


class AvroEnumSchema(AvroSchema):
    def __init__(self, namespace, name, symbols):
        super().__init__()
        self._namespace = namespace
        self._name = name
        self._symbols = symbols

    def to_json(self):
        return {
            "type": "enum",
            "namespace": self._namespace,
            "name": self._name,
            "symbols": self._symbols,
        }


def _on_list(names, namespace, name, _pa_ist: pa.ListType) -> AvroArraySchema:
    items = _on_type(names, namespace, name, 0, _pa_ist.value_type)
    return AvroArraySchema(items=items)


def _on_primitive(_filed: pa.DataType) -> AvroSchema:
    pa_type = str(_filed)
    if pa_type in ("int8", "int16", "int32", "uint8", "uint16"):
        return AvroPrimitiveSchema(typ="int")
    elif pa_type in ("uint32", "int64"):
        return AvroPrimitiveSchema(typ="long")
    elif pa_type == "float32":
        return AvroPrimitiveSchema(typ="float")
    elif pa_type == "float64":
        return AvroPrimitiveSchema(typ="double")
    elif pa_type == "date32":
        return AvroPrimitiveSchema(typ="int")
    elif pa_type == "date64":
        return AvroPrimitiveSchema(typ="long")
    elif pa_type == "bool":
        return AvroPrimitiveSchema(typ="boolean")
    elif pa_type == "string":
        return AvroPrimitiveSchema(typ="string")
    else:
        raise Exception(f"unsupported type {pa_type}")


def _on_struct(names, namespace, name, _struct: pa.StructType) -> AvroRecordSchema:
    avro_record_fields = list()
    for i in range(_struct.num_fields):
        ps_sub_filed = _struct[i]
        sub_name = ps_sub_filed.name
        sub_namespace = f"{namespace}.{sub_name}"
        sub_schema = _on_type(names, sub_namespace, sub_name, i, ps_sub_filed.type)
        avro_record_field = AvroField(
            typ=sub_schema,
            name=sub_name,
            index=i,
            has_default=False,
            name_registry=names,
            optional=ps_sub_filed.nullable,
        )
        avro_record_fields.append(avro_record_field)

    return AvroRecordSchema(
        name=name, namespace=namespace, fields=avro_record_fields, name_registry=names
    )


def _on_fixed_size_binary(
    name_registry, namespace, name, _filed: pa.FixedSizeBinaryType
) -> AvroSchema:
    return AvroFixedSchema(
        name=name, namespace=namespace, size=_filed.byte_width, name_registry=name_registry
    )


def _on_enum(name_registry, namespace, name, _filed: pa.DictionaryType) -> AvroSchema:
    pass


def _on_type(names, namespace, name, _type_index, _arrow_field):
    if isinstance(_arrow_field, pa.StructType):
        return _on_struct(names, namespace, name, _arrow_field)
    elif isinstance(_arrow_field, pa.ListType):
        return _on_list(names, namespace, name, _arrow_field)
    elif isinstance(_arrow_field, pa.FixedSizeBinaryType):
        return _on_fixed_size_binary(names, namespace, name, _arrow_field)
    elif isinstance(_arrow_field, pa.DictionaryType):
        # return _on_enum(names, namespace, name, _arrow_field)
        return _on_primitive(_arrow_field.value_type)
    elif isinstance(_arrow_field, pa.DataType):
        return _on_primitive(_arrow_field)
    else:
        raise f"unsupported type {_arrow_field}"


def convert_arrow_schema_to_avro(_schema: pa.Schema):
    avro_record_fields = list()
    namespace = "cn.graviti.portex"
    name = "root"
    names = []

    for i in range(len(_schema.types)):
        typ = _schema.types[i]
        sub_name = _schema.names[i]
        sub_namespace = f"{namespace}.{name}"
        sub_schema = _on_type(names, sub_namespace, sub_name, i, typ)
        avro_record_field = AvroField(
            typ=sub_schema,
            name=sub_name,
            index=i,
            has_default=False,
            name_registry=names,
            optional=_schema[i].nullable,
        )

        avro_record_fields.append(avro_record_field)

    avro_schema = AvroRecordSchema(
        name=name, namespace=namespace, fields=avro_record_fields, name_registry=names
    )
    return avro_schema.to_json()
