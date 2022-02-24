#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Graviti."""

from graviti.dataset.manager import DatasetManager


class Graviti:
    """This class defines the initial client to interact between local and server.

    Arguments:
        access_key: User's access key.
        url: The URL of the graviti website.

    """

    def __init__(
        self,
        access_key: str,
        url: str = "",
    ) -> None:
        pass

    @property
    def url(self) -> str:
        """Return the url of the graviti website.

        Return:
            The url of the graviti website.

        """

    @property
    def access_key(self) -> str:
        """Return the access key of the user.

        Return:
            The access key of the user.

        """

    @property
    def datasets(self) -> DatasetManager:
        """Get class :class:`~graviti.manager.dataset.DatasetManager` instance.

        Return:
            Required :class:`~graviti.manager.dataset.DatasetManager` instance.

        """
