#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the DatasetManager."""

from typing import Generator, Optional

from tensorbay.client.lazy import PagingList

from graviti.client.dataset import list_datasets
from graviti.dataset.dataset import Dataset
from graviti.exception import ResourceNotExistError


class DatasetManager:
    """This class defines the operations on the dataset on Graviti.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.

    """

    def __init__(self, access_key: str, url: str) -> None:
        self._access_key = access_key
        self._url = url

    def _generate(
        self, name: Optional[str] = None, offset: int = 0, limit: int = 128
    ) -> Generator[Dataset, None, int]:
        response = list_datasets(self._url, self._access_key, name=name, limit=limit, offset=offset)

        for item in response["datasets"]:
            yield Dataset(
                self._access_key,
                self._url,
                item["id"],
                item["name"],
                branch_name=item["defaultBranch"],
                commit_id=item["commitId"],
                alias=item["alias"],
            )

        return response["totalCount"]  # type: ignore[no-any-return]

    def create(
        self,
        name: str,
        alias: str = "",
        config_name: Optional[str] = None,
        is_public: bool = False,
    ) -> Dataset:
        """Create a Graviti dataset with given name.

        Arguments:
            name: The name of the dataset, unique for a user.
            alias: Alias of the dataset, default is "".
            config_name: The auth storage config name.
            is_public: Whether the dataset is a public dataset.

        Return:
            The created :class:`~graviti.dataset.dataset.Dataset` instance.

        """

    def get(self, name: str) -> Dataset:
        """Get a Graviti dataset with given name.

        Arguments:
            name: The name of the dataset, unique for a user.

        Returns:
            The requested :class:`~graviti.dataset.dataset.Dataset` instance.

        Raises:
            ResourceNotExistError: When the required dataset does not exist.

        """
        if not name:
            raise ResourceNotExistError(resource="dataset", identification=name)

        try:
            return next(self._generate(name))
        except StopIteration as error:
            raise ResourceNotExistError(resource="dataset", identification=name) from error

    def list(self) -> PagingList[Dataset]:
        """List Graviti datasets.

        Returns:
            The PagingList of :class:`~graviti.dataset.dataset.Dataset` instances.

        """
        return PagingList(
            lambda offset, limit: self._generate(None, offset, limit),
            128,
        )

    def delete(self, name: str) -> None:
        """Delete a Graviti dataset with given name.

        Arguments:
            name: The name of the dataset, unique for a user.

        """
