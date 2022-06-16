#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Common tools."""

import base64
import json
from typing import Tuple

import pyarrow as pa

from graviti.portex import PortexType, convert_portex_schema_to_avro


def get_schema(schema: PortexType) -> Tuple[str, str, str]:
    """Get portex schema, avro schema and arrow schema.

    Arguments:
        schema: The portex schema.

    Returns:
        The tuple of portex schema, avro schema and arrow schema.
    """
    portex_schema = schema.to_yaml()
    avro_schema = json.dumps(convert_portex_schema_to_avro(schema))
    pyarrow_schema = pa.schema(schema.fields.to_pyarrow())  # type: ignore[attr-defined]
    arrow_schema = base64.encodebytes(pyarrow_schema.serialize().to_pybytes()).decode("ascii")

    return portex_schema, avro_schema, arrow_schema
