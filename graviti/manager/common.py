#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""Common tools."""

import warnings
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from graviti.manager.commit import Commit


LIMIT = 128


class DefaultValue:
    """This class defines the default value of parameters from methods in manager.

    Arguments:
        name: The name of the default value.

    """

    def __init__(self, name: str) -> None:
        self._name = name

    def __repr__(self) -> str:
        return self._name


CURRENT_COMMIT: Any = DefaultValue("CURRENT_COMMIT")
CURRENT_BRANCH: Any = DefaultValue("CURRENT_BRANCH")
ALL_BRANCHES: Any = DefaultValue("ALL_BRANCHES")


class StatusWarning(Warning):
    """This class defines the warning that the commit of dataset branch or tag is not up-to-date.

    Arguments:
       revision_type: The type of current dataset revision.
       name: The name of current dataset revision.

    """

    def __init__(self, revision_type: str, name: str):
        super().__init__()
        self._type = revision_type
        self._name = name

    def __str__(self) -> str:
        return (
            f"The commit id of {self._type} ({self._name}) of current dataset is not up-to-date. "
            "It is recommended to update the 'HEAD' of the dataset by the method "
            f"'checkout('{self._name}')'"
        )


def check_head_status(head: "Commit", remote_revision: str, remote_commit_id: str) -> None:
    """Check if the commit for the HEAD of the current dataset is up-to-date.

    Arguments:
        head: The current revision of the local dataset.
        remote_revision: The revision of the remote dataset in server.
        remote_commit_id: The commit id of the remote dataset in server.

    """
    if getattr(head, "name", None) == remote_revision and head.commit_id != remote_commit_id:
        warnings.warn(StatusWarning(head.__class__.__name__.lower(), remote_revision), stacklevel=2)
