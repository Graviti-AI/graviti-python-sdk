#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Functions converting TensorBay Catalog to Graviti Schema."""

from functools import reduce
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple, Union

from tensorbay.dataset import Notes, RemoteData
from tensorbay.label import (
    Catalog,
    ClassificationSubcatalog,
    InstanceMaskSubcatalog,
    Keypoints2DSubcatalog,
    Label,
    LabeledKeypoints2D,
    PanopticMaskSubcatalog,
)
from tensorbay.label.basic import SubcatalogBase, _LabelBase
from tensorbay.label.supports import AttributesMixin, CategoriesMixin, MaskCategoriesMixin

_JSON_TYPE_TO_PORTEX_TYPE = {
    "array": "array",
    "boolean": "boolean",
    "integer": "int32",
    "number": "float32",
    "string": "string",
}


_PYTHON_TYPE_TO_PORTEX_TYPE = {
    int: "int32",
    float: "float32",
}

_LABEL_PROCESSORS: Dict[str, "_ConverterBase"] = {}

_SUFFIX_TO_FILE_FIELDS: Dict[str, Dict[str, Any]] = {}


def _fill_file_fields() -> None:
    _SUFFIX_TO_FILE_FIELDS[".bin"] = {"name": "point_cloud", "type": "file.PointCloudBin"}
    for suffix in (".pcd", ".ply", ".las", ".laz"):
        _SUFFIX_TO_FILE_FIELDS[suffix] = {"name": "point_cloud", "type": "file.PointCloud"}
    for suffix in (".bmp", ".jpg", ".jpeg", ".png", ".gif"):
        _SUFFIX_TO_FILE_FIELDS[suffix] = {"name": "image", "type": "file.Image"}
    for suffix in (".mp3", ".wav", ".wma", ".aac"):
        _SUFFIX_TO_FILE_FIELDS[suffix] = {"name": "audio", "type": "file.Audio"}


_fill_file_fields()


def _get_category_params(subcatalog: Union[CategoriesMixin, MaskCategoriesMixin]) -> Dict[str, Any]:
    if not hasattr(subcatalog, "categories"):
        return {}

    category_params: Dict[str, Any] = {}
    if isinstance(subcatalog, MaskCategoriesMixin):
        category_params["categoryIds"] = subcatalog.get_index_to_category()
    else:
        category_params["categories"] = list(subcatalog.categories.keys())

    category_delimiter = getattr(subcatalog, "category_delimiter", None)
    if category_delimiter:
        category_params["categoryDelimiter"] = category_delimiter

    return category_params


def _get_attribute_params(subcatalog: AttributesMixin) -> Dict[str, Any]:
    if not hasattr(subcatalog, "attributes"):
        return {}

    attribute_params: List[Dict[str, Any]] = []
    for attribute in subcatalog.attributes:
        field: Dict[str, Any] = {"name": attribute.name}
        if hasattr(attribute, "enum"):
            field["type"] = "enum"
            field["values"] = attribute.enum
        else:
            attribute_type = (
                reduce(lambda a, b: a or b, attribute.type)
                if isinstance(attribute.type, list)
                else attribute.type
            )
            field["type"] = _JSON_TYPE_TO_PORTEX_TYPE[attribute_type]

        attribute_params.append(field)
    return {"attributes": attribute_params}


def _get_category_and_attribute_params(subcatalog: Any) -> Dict[str, Any]:
    return {**_get_category_params(subcatalog), **_get_attribute_params(subcatalog)}


def _get_dtype(value: float, default_type: str = "int32") -> Dict[str, str]:
    dtype = _PYTHON_TYPE_TO_PORTEX_TYPE[type(value)]
    return {"dtype": dtype} if dtype != default_type else {}


class _ConverterBase:
    import_types: Any

    def __init__(self, label_type: str) -> None:
        _LABEL_PROCESSORS[label_type] = self

    def __call__(
        self, subcatalog: SubcatalogBase, labels: Union[_LabelBase, List[_LabelBase], None]
    ) -> Any:
        ...


class _ConverterClassification(_ConverterBase):
    _PROCESSORS: Dict[
        Tuple[str, str],
        Union[
            Callable[[Union[CategoriesMixin, MaskCategoriesMixin]], Dict[str, Any]],
            Callable[[AttributesMixin], Dict[str, Any]],
        ],
    ] = {
        ("category", "label.Category"): _get_category_params,
        ("attribute", "label.Attribute"): _get_attribute_params,
    }

    def __init__(self, label_type: str) -> None:
        super().__init__(label_type)
        self.import_types = []

    def __call__(  # type: ignore[override]
        self,
        subcatalog: ClassificationSubcatalog,
        labels: Union[_LabelBase, List[_LabelBase], None] = None,
    ) -> List[Dict[str, Any]]:
        fields = []
        for (field_name, field_type), processor in self._PROCESSORS.items():
            params = processor(subcatalog)
            if params:
                fields.append({"name": field_name, "type": field_type, **params})
                self.import_types.append({"name": field_type})
        return fields


class _ConverterCommon(_ConverterBase):
    def __init__(self, label_type: str, portex_type: str) -> None:
        super().__init__(label_type)
        self._portex_type = portex_type
        self.import_types = {"name": portex_type}


class _ConverterSingleLabel(_ConverterCommon):
    def __init__(self, label_type: str, portex_type: str) -> None:
        super().__init__(label_type, portex_type)
        self._field_name = label_type

    def __call__(
        self, subcatalog: SubcatalogBase, labels: Union[_LabelBase, List[_LabelBase], None]
    ) -> Dict[str, Any]:
        return {
            "name": self._field_name,
            "type": self._portex_type,
            **_get_category_and_attribute_params(subcatalog),
        }


class _ConverterInstanceMask(_ConverterSingleLabel):
    def __call__(  # type: ignore[override]
        self, subcatalog: InstanceMaskSubcatalog, labels: _LabelBase
    ) -> Dict[str, Any]:
        field = super().__call__(subcatalog, labels)
        is_tracking = subcatalog.is_tracking
        if is_tracking:
            field["isTracking"] = is_tracking
        return field


class _ConverterPanopticMask(_ConverterSingleLabel):
    def __call__(  # type: ignore[override]
        self, subcatalog: PanopticMaskSubcatalog, labels: _LabelBase
    ) -> Dict[str, Any]:
        field = super().__call__(subcatalog, labels)
        field["stuffIds"] = field.pop("categoryIds")
        return field


class _ConverterMultiLabel(_ConverterCommon):
    def __init__(self, label_type: str, portex_type: str) -> None:
        super().__init__(label_type, portex_type)
        self._field_name = label_type.upper() if label_type == "rle" else f"{label_type}s"

    def __call__(  # type: ignore[override]
        self, subcatalog: SubcatalogBase, labels: List[_LabelBase]
    ) -> Dict[str, Any]:
        items = {"type": self._portex_type, **_get_category_and_attribute_params(subcatalog)}
        return {"name": self._field_name, "type": "array", "items": items}


class _ConverterMultiGeometryLabel(_ConverterMultiLabel):
    def __init__(self, label_type: str, portex_type: str, label_value_level: int) -> None:
        super().__init__(label_type, portex_type)
        self._label_value_level = label_value_level

    def __call__(  # type: ignore[override]
        self, subcatalog: SubcatalogBase, labels: List[_LabelBase]
    ) -> Dict[str, Any]:
        field = super().__call__(subcatalog, labels)

        if labels:
            value = labels
            for _ in range(self._label_value_level):
                value = value[0]  # type: ignore[assignment]
            field["items"].update(_get_dtype(value))  # type: ignore[arg-type]

        return field


class _ConverterMultiKeypoints2D(_ConverterMultiGeometryLabel):
    def __call__(  # type: ignore[override]
        self, subcatalog: Keypoints2DSubcatalog, labels: List[LabeledKeypoints2D]
    ) -> Dict[str, Any]:
        field = super().__call__(subcatalog, labels)  # type: ignore[arg-type]

        items = field["items"]
        keypoints = subcatalog.keypoints[0]
        items["number"] = keypoints.number
        if hasattr(keypoints, "names"):
            items["names"] = keypoints.names
        if hasattr(keypoints, "skeleton"):
            items["skeleton"] = keypoints.skeleton
        if hasattr(keypoints, "visible"):
            items["visible"] = keypoints.visible

        return field


_ConverterClassification("classification")
_ConverterMultiGeometryLabel("box2d", "label.Box2D", 2)
_ConverterMultiLabel("box3d", "label.Box3D")
_ConverterMultiGeometryLabel("polygon", "label.Polygon", 3)
_ConverterMultiGeometryLabel("polyline2d", "label.Polyline2D", 3)
_ConverterMultiGeometryLabel("multi_polygon", "label.MultiPolygon", 4)
_ConverterMultiGeometryLabel("multi_polyline2d", "label.MultiPolyline2D", 4)
_ConverterMultiKeypoints2D("keypoints2d", "label.Keypoints2D", 3)
_ConverterMultiLabel("rle", "label.RLE")
_ConverterSingleLabel("semantic_mask", "label.file.SemanticMask")
_ConverterInstanceMask("instance_mask", "label.file.InstanceMask")
_ConverterPanopticMask("panoptic_mask", "label.file.PanopticMask")


def _get_file_field(data: RemoteData, notes: Notes) -> Dict[str, Any]:
    field = _SUFFIX_TO_FILE_FIELDS.get(Path(data.path).suffix, {"name": "file", "type": "binary"})

    if field["type"] == "file.PointCloudBin":
        bin_point_cloud_fields = notes.bin_point_cloud_fields
        if bin_point_cloud_fields:
            field["field"] = bin_point_cloud_fields

    if notes.is_continuous:
        field["isContinuous"] = True

    return field


def _get_label_fields(
    catalog: Catalog, label: Label
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    label_fields: List[Dict[str, Any]] = []
    label_import_types: List[Dict[str, Any]] = []
    for label_type, processor in _LABEL_PROCESSORS.items():
        if not hasattr(catalog, label_type):
            continue
        subcatalog = getattr(catalog, label_type)
        if label_type == "classification":
            label_fields.extend(processor(subcatalog, None))
            label_import_types.extend(processor.import_types)
        else:
            label_fields.append(processor(subcatalog, getattr(label, label_type)))
            label_import_types.append(processor.import_types)
    return label_fields, label_import_types


def catalog_to_schema(catalog: Catalog, data_sample: RemoteData, notes: Notes) -> Dict[str, Any]:
    """Convert the TensorBay Catalog to Graviti schema format.

    Arguments:
        catalog: The TensorBay Catalog of a dataset.
        data_sample: The data sample of the dataset.
        notes: The notes of the dataset.

    Returns:
        The YAML string of the dataset schema.

    """
    fields = [{"name": "filename", "type": "string"}]
    file_field = _get_file_field(data_sample, notes)
    fields.append(file_field)

    import_types = []
    file_field_type = file_field["type"]
    if file_field_type != "binary":
        import_types.append({"name": file_field_type})

    label = data_sample.label
    if label:
        label_fields, label_import_types = _get_label_fields(catalog, label)
        fields.extend(label_fields)
        import_types.extend(label_import_types)

    imports = [
        {
            "repo": "https://github.com/Project-OpenBytes/standard@v1.0.0",
            "types": import_types,
        }
    ]

    return {"imports": imports, "type": "record", "fields": fields}
