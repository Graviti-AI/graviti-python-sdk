#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Workspace."""

from graviti.manager import DatasetManager, StorageConfigManager
from graviti.openapi import get_current_workspace
from graviti.utility import urlnorm


class Workspace:  # pylint: disable=too-many-instance-attributes
    """This class defines the initial client to interact between local and server.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.

    """

    _DEFAULT_URL_CN = "https://api.graviti.cn"
    _DEFAULT_URL_COM = "https://api.graviti.com"

    def __init__(
        self,
        access_key: str,
        url: str = "",
    ) -> None:
        if access_key.startswith("Accesskey-"):
            url = url if url else Workspace._DEFAULT_URL_CN
        elif access_key.startswith("ACCESSKEY-"):
            url = url if url else Workspace._DEFAULT_URL_COM
        else:
            raise TypeError("Wrong accesskey format!")

        if not url.startswith("https://"):
            raise TypeError("Invalid url, only support url starts with 'https://'")

        url = urlnorm(url)

        self._access_key = access_key
        self._url = url

        response = get_current_workspace(access_key, url)

        self._id = response["id"]
        self.type = response["type"]

        name = response["name"]
        self.name = name
        self.alias = response["alias"]
        self.description = response["description"]
        self.email = response["email"]

        self._dataset_manager = DatasetManager(access_key, url, name)
        self._storage_config_manager = StorageConfigManager(access_key, url, name)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}("{self.name}")'

    @property
    def access_key(self) -> str:
        """Return the access key of the user.

        Returns:
            The access key of the user.

        """
        return self._access_key

    @property
    def url(self) -> str:
        """Return the url of the graviti website.

        Returns:
            The url of the graviti website.

        """
        return self._url

    @property
    def datasets(self) -> DatasetManager:
        """Get class :class:`~graviti.manager.dataset.DatasetManager` instance.

        Returns:
            Required :class:`~graviti.manager.dataset.DatasetManager` instance.

        """
        return self._dataset_manager

    @property
    def storage_configs(self) -> StorageConfigManager:
        """Get class :class:`~graviti.manager.storage_config.StorageConfigManager` instance.

        Returns:
            Required :class:`~graviti.manager.storage_config.StorageConfigManager` instance.

        """
        return self._storage_config_manager
