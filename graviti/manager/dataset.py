#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the DatasetManager."""

from typing import Optional

from tensorbay.client.lazy import PagingList

from graviti.dataset.dataset import Dataset


class DatasetManager:
    """This class defines the operations on the dataset on Graviti.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.

    """

    def __init__(self, access_key: str, url: str) -> None:
        pass

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

        Return:
            The requested :class:`~graviti.dataset.dataset.Dataset` instance.

        """

    def list(self) -> PagingList[Dataset]:
        """List Graviti datasets.

        Return:
            The PagingList of :class:`~graviti.dataset.dataset.Dataset` instances.

        """

    def delete(self, name: str) -> None:
        """Delete a Graviti dataset with given name.

        Arguments:
            name: The name of the dataset, unique for a user.

        """
