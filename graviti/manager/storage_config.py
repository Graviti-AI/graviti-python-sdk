#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the StorageConfig and StorageConfigManager."""

from typing import Any, Dict, Generator

from graviti.exception import ResourceNameError
from graviti.manager.common import LIMIT
from graviti.manager.lazy import LazyPagingList
from graviti.openapi import get_storage_config, list_storage_configs, update_storage_configs
from graviti.utility import ReprMixin, check_type


class StorageConfig(ReprMixin):
    """This class defines the structure of the storage config of Graviti Data Platform.

    Arguments:
        workspace: The name of the workspace.
        response: The response of the OpenAPI associated with the storage config::

                {
                    "id": <str>
                    "name": <str>
                    "config_type": <str>
                    "backend_type": <str>
                }

    Attributes:
        workspace: The name of the workspace.
        name: The name of the storage config.
        config_type: The config type of the storage config.
        backend_type: The backend tyep of the storage config.

    """

    _repr_attrs = ("workspace", "config_type", "backend_type")

    def __init__(self, workspace: str, response: Dict[str, Any]) -> None:
        self._id = response["id"]
        self.workspace = workspace
        self.name = response["name"]
        self.config_type = response["config_type"]
        self.backend_type = response["backend_type"]

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{self.name}")'


class StorageConfigManager:
    """This class defines the operations on the storage config in Graviti.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.
        workspace: The name of the workspace.

    """

    def __init__(self, access_key: str, url: str, workspace: str) -> None:
        self.access_key = access_key
        self.url = url
        self.workspace = workspace

    def _generate(self, offset: int, limit: int) -> Generator[StorageConfig, None, int]:
        access_key = self.access_key
        url = self.url
        response = list_storage_configs(access_key, url, self.workspace, limit=limit, offset=offset)

        for item in response["storage_configs"]:
            yield StorageConfig(self.workspace, item)

        return response["total_count"]  # type: ignore[no-any-return]

    @property
    def default_storage_config(self) -> str:
        """The default storage config of this worksapce.

        Returns:
            The default storage config of this worksapce.

        """
        response = list_storage_configs(
            self.access_key, self.url, self.workspace, limit=1, offset=0
        )
        return response["default_storage_config"]  # type: ignore[no-any-return]

    def get(self, name: str) -> StorageConfig:
        """Get a Graviti storage config with given name.

        Arguments:
            name: The name of the storage config.

        Returns:
            The requested :class:`~graviti.manager.storage_config.storage_config` instance.

        Raises:
            ResourceNameError: When the given storage config name is invalid.

        """
        check_type("name", name, str)
        if not name:
            raise ResourceNameError("storage config", name)

        response = get_storage_config(
            self.access_key, self.url, workspace=self.workspace, storage_config=name
        )

        return StorageConfig(self.workspace, response)

    def list(self) -> LazyPagingList[StorageConfig]:
        """List Graviti storage configs.

        Returns:
            The LazyPagingList of :class:`~graviti.manager.storage_config.StorageConfig` instances.

        """
        return LazyPagingList(self._generate, LIMIT)

    def edit(self, *, default_storage_config: str) -> None:
        """Update Graviti storage config related configs.

        Arguments:
            default_storage_config: The name of the new default storage config.

        Raises:
            ResourceNameError: When the given default storage config name is invalid.

        """
        check_type("default_storage_config", default_storage_config, str)
        if not default_storage_config:
            raise ResourceNameError("storage config", default_storage_config)

        update_storage_configs(
            self.access_key, self.url, self.workspace, default_storage_config=default_storage_config
        )
