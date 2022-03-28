#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Schema to colomn extractors related methods."""


from functools import partial
from typing import Any, Callable, Dict, Iterator, List, Tuple, TypeVar, Union

from graviti import DataFrame
from graviti.utility.file import File
from graviti.utility.typing import NestedDict

_T = TypeVar("_T")
_D = Dict[str, Tuple[Iterator[_T], str]]
_Extractor = Tuple[Callable[[Dict[str, Any]], Iterator[_T]], str]
Extractors = NestedDict[str, _Extractor[Any]]


_PORTEX_TYPE_TO_NUMPY_DTYPE = {
    "boolean": "bool",
    "binary": "object",
    "string": "object",
    "enum": "object",
}


def _get_filename(*_: Any) -> _Extractor[str]:
    def extractor(data: Dict[str, Any]) -> Iterator[str]:
        for item in data["dataDetails"]:
            yield item["remotePath"]

    return extractor, "object"


def _get_file(*_: Any) -> _Extractor[File]:
    def extractor(data: Dict[str, Any]) -> Iterator[File]:
        for item in data["dataDetails"]:
            yield File(item["url"], item["checksum"])

    return extractor, "object"


def _get_category(*_: Any) -> _Extractor[Any]:
    def extractor(data: Dict[str, Any]) -> Iterator[Any]:
        for item in data["dataDetails"]:
            yield item["label"]["CLASSIFICATION"]["category"]

    return extractor, "object"


def _attribute_extractor(data: Dict[str, Any], name: str) -> Iterator[Any]:
    for item in data["dataDetails"]:
        yield item["label"]["CLASSIFICATION"]["attributes"][name]


def _get_attribute(schema: Dict[str, Any]) -> Extractors:
    attributes: Extractors = {}
    for attribute_info in schema["attributes"]:
        name = attribute_info["name"]
        type_ = attribute_info["type"]
        dtype = _PORTEX_TYPE_TO_NUMPY_DTYPE.get(type_, type_)
        attributes[name] = partial(_attribute_extractor, name=name), dtype
    return attributes


def _extract_attribute(data: List[Dict[str, Any]], schema: Dict[str, Any]) -> Dict[str, List[Any]]:
    attributes = {}
    for attribute_info in schema["attributes"]:
        name = attribute_info["name"]
        attributes[name] = [item["attributes"][name] for item in data]
    return attributes


def _extract_category_and_attribute(
    schema: Dict[str, Any], labels: List[Dict[str, Any]]
) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    if "categories" in schema:
        results["category"] = [label["category"] for label in labels]
    if "attributes" in schema:
        results["attribute"] = _extract_attribute(labels, schema)
    return results


def _get_box2ds(schema: Dict[str, Any]) -> _Extractor[DataFrame]:
    def extractor(data: Dict[str, Any]) -> Iterator[DataFrame]:
        for item in data["dataDetails"]:
            labels = item["label"]["BOX2D"]
            box2d: Dict[str, Any] = {
                "xmin": [label["box2d"]["xmin"] for label in labels],
                "xmax": [label["box2d"]["xmax"] for label in labels],
                "ymin": [label["box2d"]["ymin"] for label in labels],
                "ymax": [label["box2d"]["ymax"] for label in labels],
            }
            box2d.update(_extract_category_and_attribute(schema["items"], labels))
            yield DataFrame(box2d)

    return extractor, "object"


def _extract_vetices(
    pointlist: List[Dict[str, Any]], vertices_keys: Tuple[str, ...]
) -> Dict[str, List[Any]]:
    return {key: [point[key] for point in pointlist] for key in vertices_keys}


def _get_pointlists(schema: Dict[str, Any], key: str) -> _Extractor[DataFrame]:
    def extractor(data: Dict[str, Any]) -> Iterator[DataFrame]:
        for item in data["dataDetails"]:
            labels = item["label"][key.upper()]
            pointlists: Dict[str, Any] = {
                "vertices": [
                    DataFrame(_extract_vetices(label[key], ("x", "y"))) for label in labels
                ]
            }
            pointlists.update(_extract_category_and_attribute(schema["items"], labels))
            yield DataFrame(pointlists)

    return extractor, "object"


def _get_keypoints2ds(schema: Dict[str, Any]) -> _Extractor[DataFrame]:
    keypoints2d_schema = schema["items"]
    vertices_keys = ("x", "y", "v") if keypoints2d_schema.get("visible") else ("x", "y")
    vertices_extractor = partial(_extract_vetices, vertices_keys=vertices_keys)

    def extractor(data: Dict[str, Any]) -> Iterator[DataFrame]:
        for item in data["dataDetails"]:
            labels = item["label"]["KEYPOINTS2D"]
            keypoints2d: Dict[str, Any] = {
                "vertices": [
                    DataFrame(vertices_extractor(label["keypoints2d"])) for label in labels
                ]
            }
            keypoints2d.update(_extract_category_and_attribute(keypoints2d_schema, labels))
            yield DataFrame(keypoints2d)

    return extractor, "object"


def _mask_extractor(data: Dict[str, Any], key: str) -> Iterator[Any]:
    for item in data["dataDetails"]:
        label = item["label"][key]
        yield File(label["url"], label["checksum"])


def _info_extractor(data: Dict[str, Any], schema: Dict[str, Any], key: str) -> Iterator[DataFrame]:
    id_key = "categoryId" if key == "SEMANTIC_MASK" else "instanceId"
    for item in data["dataDetails"]:
        all_info = item["label"][key]["info"]
        mask_info = {
            "id": [info[id_key] for info in all_info],
            "attribute": _extract_attribute(all_info, schema),
        }
        if key == "PANOPTIC_MASK":
            mask_info["categoryId"] = [info["categoryId"] for info in all_info]
        yield DataFrame(mask_info)


def _get_mask(schema: Dict[str, Any], key: str) -> Extractors:
    mask: Dict[str, Any] = {"mask": (partial(_mask_extractor, key=key), "object")}
    if "attributes" in schema:
        mask["info"] = partial(_info_extractor, schema=schema, key=key), "object"
    return mask


def _get_multi_pointlists(schema: Dict[str, Any], key: str) -> _Extractor[DataFrame]:
    def extractor(data: Dict[str, Any]) -> Iterator[DataFrame]:
        for item in data["dataDetails"]:
            labels = item["label"][f"MULTI_{key.upper()}"]
            multi_pointlists: Dict[str, Any] = {
                f"{key}s": [
                    [
                        DataFrame(_extract_vetices(pointlist, ("x", "y")))
                        for pointlist in label[f"multi{key.capitalize()}"]
                    ]
                    for label in labels
                ]
            }
            multi_pointlists.update(_extract_category_and_attribute(schema["items"], labels))
            yield DataFrame(multi_pointlists)

    return extractor, "object"


def _get_rle(schema: Dict[str, Any]) -> _Extractor[DataFrame]:
    def extractor(data: Dict[str, Any]) -> Iterator[DataFrame]:
        for item in data["dataDetails"]:
            labels = item["label"]["RLE"]
            rle: Dict[str, Any] = {"code": [label["rle"] for label in labels]}
            rle.update(_extract_category_and_attribute(schema["items"], labels))
            yield DataFrame(rle)

    return extractor, "object"


_EXTRACTORS_GETTER: Dict[
    Tuple[str, str], Callable[[Dict[str, Any]], Union[_Extractor[Any], Extractors]]
] = {
    ("filename", "string"): _get_filename,
    ("image", "file.Image"): _get_file,
    ("point_cloud", "file.PointCloud"): _get_file,
    ("point_cloud", "file.PointCloudBin"): _get_file,
    ("audio", "file.Audio"): _get_file,
    ("file", "binary"): _get_file,
    ("attribute", "label.Attribute"): _get_attribute,
    ("category", "label.Category"): _get_category,
    ("box2ds", "array"): _get_box2ds,
    ("polygons", "array"): partial(_get_pointlists, key="polygon"),
    ("polyline2ds", "array"): partial(_get_pointlists, key="polyline2d"),
    ("keypoints2ds", "array"): _get_keypoints2ds,
    ("semantic_mask", "label.file.SemanticMask"): partial(_get_mask, key="SEMANTIC_MASK"),
    ("instance_mask", "label.file.InstanceMask"): partial(_get_mask, key="INSTANCE_MASK"),
    ("panoptic_mask", "label.file.PanopticMask"): partial(_get_mask, key="PANOPTIC_MASK"),
    ("multi_polygons", "array"): partial(_get_multi_pointlists, key="polygon"),
    ("multi_polyline2ds", "array"): partial(_get_multi_pointlists, key="polyline2d"),
    ("RLE", "array"): _get_rle,
}


def get_extractors(schema: Dict[str, Any]) -> Extractors:
    """Get the extractors and dtypes for colomns.

    Arguments:
        schema: The schema of a DataFrame.

    Returns:
        A dict containing the extractors and dtypes for all colomns.

    Examples:
        >>> import yaml
        >>>
        >>> from graviti.client import list_data_details
        >>> from graviti.utility.lazy import LazyFactory, LazyList
        >>> from graviti.schema import catalog_to_schema, get_extractors
        >>>
        >>> from tensorbay import GAS
        >>> from tensorbay.dataset import Dataset
        >>> ACCESSKEY = "ACCESSKEY-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        >>> URL = "https://gas.graviti.com/"
        >>> DATASET_NAME = "MNIST"
        >>> TOTAL_COUNT = 1000
        >>>
        >>> gas = GAS(ACCESSKEY)
        >>> dataset = Dataset(DATASET_NAME, gas)
        >>> dataset_client = gas.get_dataset(DATASET_NAME)
        >>>
        >>> getter = lambda offset, limit: list_data_details(
        ...     url=URL,
        ...     access_key=ACCESSKEY,
        ...     dataset_id=dataset_client.dataset_id,
        ...     segment_name="train",
        ...     commit=dataset_client.status.commit_id,
        ...     offset=offset,
        ...     limit=limit,
        ... )
        >>> factory = LazyFactory(TOTAL_COUNT, 128, getter)
        >>> schema = yaml.load(
        ...    catalog_to_schema(dataset.catalog, dataset["train"][0], dataset.notes), yaml.Loader
        ... )
        >>> extractors = get_extractors(schema)
        >>> lazy_lists = {}
        >>> for key, arguments in extractors.items():
        ...     lazy_lists[key] = factory.create_list(*arguments)
        >>> lazy_lists
        {'filename': LazyList [
           'train_image_00000.png',
           'train_image_00001.png',
           'train_image_00002.png',
           'train_image_00003.png',
           'train_image_00004.png',
           'train_image_00005.png',
           'train_image_00006.png',
           'train_image_00007.png',
           'train_image_00008.png',
           'train_image_00009.png',
           'train_image_00010.png',
           'train_image_00011.png',
           'train_image_00012.png',
           'train_image_00013.png',
           ... (985 items are folded),
           'train_image_00999.png'
         ],
         'image': LazyList [
           RemoteFileMixin("train_image_00000.png"),
           RemoteFileMixin("train_image_00001.png"),
           RemoteFileMixin("train_image_00002.png"),
           RemoteFileMixin("train_image_00003.png"),
           RemoteFileMixin("train_image_00004.png"),
           RemoteFileMixin("train_image_00005.png"),
           RemoteFileMixin("train_image_00006.png"),
           RemoteFileMixin("train_image_00007.png"),
           RemoteFileMixin("train_image_00008.png"),
           RemoteFileMixin("train_image_00009.png"),
           RemoteFileMixin("train_image_00010.png"),
           RemoteFileMixin("train_image_00011.png"),
           RemoteFileMixin("train_image_00012.png"),
           RemoteFileMixin("train_image_00013.png"),
           ... (985 items are folded),
           RemoteFileMixin("train_image_00999.png")
         ],
         'category': LazyList [
           '5',
           '0',
           '4',
           '1',
           '9',
           '2',
           '1',
           '3',
           '1',
           '4',
           '3',
           '5',
           '3',
           '6',
           ... (985 items are folded),
           '6'
         ]}

    """
    extractors: Extractors = {}
    for field in schema["fields"]:
        field_name, field_type = field["name"], field["type"]
        extractor_getter = _EXTRACTORS_GETTER[field_name, field_type]
        extractors[field_name] = extractor_getter(field)
    return extractors
