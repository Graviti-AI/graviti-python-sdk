#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#
"""Schema module."""

# https://github.com/python/mypy/issues/9318
from graviti.portex.avro import convert_portex_schema_to_avro  # type: ignore[attr-defined]
from graviti.portex.base import PortexRecordBase, PortexType, read_json, read_yaml
from graviti.portex.builder import build_package
from graviti.portex.builtin import (
    array,
    binary,
    boolean,
    enum,
    float32,
    float64,
    int32,
    int64,
    record,
    string,
)
from graviti.portex.register import (
    STANDARD_URL,
    ContainerRegister,
    ExternalContainerRegister,
    RemoteFileTypeResgister,
)

__all__ = [
    "ContainerRegister",
    "ExternalContainerRegister",
    "STANDARD_URL",
    "PortexRecordBase",
    "PortexType",
    "RemoteFileTypeResgister",
    "array",
    "binary",
    "boolean",
    "build_package",
    "convert_portex_schema_to_avro",
    "enum",
    "float32",
    "float64",
    "int32",
    "int64",
    "read_json",
    "read_yaml",
    "record",
    "string",
]
