#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Schema to colomn extractors related methods."""


from functools import partial
from typing import Any, Callable, Dict, Iterator, Tuple, TypeVar, Union

from tensorbay.utility.file import URL
from tensorbay.utility.file import RemoteFileMixin as File

from graviti import DataFrame

_T = TypeVar("_T")
_D = Dict[str, Tuple[Iterator[_T], str]]
_Extractor = Tuple[Callable[[Dict[str, Any]], Iterator[_T]], str]
_Extractors = Dict[str, _Extractor[_T]]


_PORTEX_TYPE_TO_NUMPY_DTYPE = {
    "boolean": "bool",
    "bytes": "object",
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
            url = item["url"]
            yield File(
                item["remotePath"], url=URL(url, lambda: "update is not supported currently")
            )

    return extractor, "object"


def _get_category(*_: Any) -> _Extractor[Any]:
    def extractor(data: Dict[str, Any]) -> Iterator[Any]:
        for item in data["dataDetails"]:
            yield item["label"]["CLASSIFICATION"]["category"]

    return extractor, "object"


def _attribute_extractor(data: Dict[str, Any], name: str) -> Iterator[Any]:
    for item in data["dataDetails"]:
        yield item["label"]["CLASSIFICATION"]["attributes"][name]


def _get_attribute(schema: Dict[str, Any]) -> _Extractors[Any]:  # pylint: disable=unused-argument
    attributes: _Extractors[Any] = {}
    for attribute_info in schema["attributes"]:
        name = attribute_info["name"]
        type_ = attribute_info["type"]
        dtype = _PORTEX_TYPE_TO_NUMPY_DTYPE.get(type_, type_)
        attributes[name] = partial(_attribute_extractor, name=name), dtype
    return attributes


def _get_box2ds(
    schema: Dict[str, Any], *_: Any  # pylint: disable=unused-argument
) -> _Extractor[DataFrame]:
    def extractor(data: Dict[str, Any]) -> Iterator[DataFrame]:
        raise NotImplementedError

    return extractor, "object"


def _get_pointlists(
    schema: Dict[str, Any], key: str  # pylint: disable=unused-argument
) -> _Extractor[DataFrame]:
    def extractor(data: Dict[str, Any]) -> Iterator[DataFrame]:
        raise NotImplementedError

    return extractor, "object"


def _get_keypoints2ds(
    schema: Dict[str, Any]  # pylint: disable=unused-argument
) -> _Extractor[DataFrame]:
    def extractor(data: Dict[str, Any]) -> Iterator[DataFrame]:
        raise NotImplementedError

    return extractor, "object"


def _get_mask(
    schema: Dict[str, Any], key: str  # pylint: disable=unused-argument
) -> _Extractors[Any]:
    raise NotImplementedError


def _get_multi_pointlists(
    schema: Dict[str, Any], key: str  # pylint: disable=unused-argument
) -> _Extractor[DataFrame]:
    def extractor(data: Dict[str, Any]) -> Iterator[DataFrame]:
        raise NotImplementedError

    return extractor, "object"


def _get_rle(schema: Dict[str, Any]) -> _Extractor[DataFrame]:  # pylint: disable=unused-argument
    def extractor(data: Dict[str, Any]) -> Iterator[DataFrame]:
        raise NotImplementedError

    return extractor, "object"


_EXTRACTORS_GETTER: Dict[
    Tuple[str, str], Callable[[Dict[str, Any]], Union[_Extractor[Any], _Extractors[Any]]]
] = {
    ("filename", "string"): _get_filename,
    ("image", "file.Image"): _get_file,
    ("point_cloud", "file.PointCloud"): _get_file,
    ("point_cloud", "file.PointCloudBin"): _get_file,
    ("audio", "file.Audio"): _get_file,
    ("file", "bytes"): _get_file,
    ("attribute", "label.Attribute"): _get_attribute,
    ("category", "label.Category"): _get_category,
    ("box2ds", "array"): _get_box2ds,
    ("polygons", "array"): partial(_get_pointlists, key="polygons"),
    ("polyline2ds", "array"): partial(_get_pointlists, key="polyline2ds"),
    ("keypoints2d", "array"): _get_keypoints2ds,
    ("face-keypoints2d", "laebl.Keypoints2D"): _get_keypoints2ds,
    ("semantic_mask", "label.file.SemanticMask"): partial(_get_mask, key="semantic_mask"),
    ("instance_mask", "label.file.InstanceMask"): partial(_get_mask, key="instance_mask"),
    ("panoptic_mask", "label.file.PanopticMask"): partial(_get_mask, key="panoptic_mask"),
    ("multi_polygons", "array"): partial(_get_multi_pointlists, key="multi_polygons"),
    ("multi_polyline2ds", "array"): partial(_get_multi_pointlists, key="multi_polyline2ds"),
    ("RLE", "array"): _get_rle,
}


def get_extractors(schema: Dict[str, Any]) -> Dict[str, Union[_Extractor[Any], _Extractors[Any]]]:
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
    extractors: Dict[str, Union[_Extractor[Any], _Extractors[Any]]] = {}
    for field in schema["fields"]:
        field_name, field_type = field["name"], field["type"]
        extractor_getter = _EXTRACTORS_GETTER[field_name, field_type]
        extractors[field_name] = extractor_getter(field)
    return extractors
