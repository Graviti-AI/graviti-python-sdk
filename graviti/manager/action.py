#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Action and ActionManager."""

from typing import TYPE_CHECKING, Any, Dict, Generator, Optional

from graviti.exception import ResourceNameError
from graviti.manager.common import LIMIT
from graviti.manager.lazy import LazyPagingList
from graviti.openapi import create_action, delete_action, get_action, list_actions, update_action
from graviti.utility import ReprMixin, check_type
from graviti.utility.typing import SortParam

if TYPE_CHECKING:
    from graviti.manager.dataset import Dataset


class Action(ReprMixin):
    """This class defines the structure of an action.

    Arguments:
        dataset: Class :class:`~graviti.dataset.dataset.Dataset` instance.
        response: The response of the OpenAPI associated with the action::

                {
                    "id": <str>,
                    "name": <str>,
                    "edition": <int>,
                    "state": <str>,
                    "payload": <str>,
                },

    Attributes:
        name: The name of this action.
        edition: The edition of this action.
        state: The state of this action.
        payload: The payload of this action.

    """

    _repr_attrs = ("edition", "state")

    def __init__(self, dataset: "Dataset", response: Dict[str, Any]) -> None:
        self._dataset = dataset
        self._id: str = response["id"]
        self.name: str = response["name"]
        self.edition: int = response["edition"]
        self.state: str = response["state"]
        self.payload: str = response["payload"]

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{self.name}")'

    def _edit(
        self,
        *,
        name: Optional[str] = None,
        state: Optional[str] = None,
        payload: Optional[str] = None,
    ) -> None:
        _workspace = self._dataset.workspace
        response = update_action(
            _workspace.access_key,
            _workspace.url,
            _workspace.name,
            self._dataset.name,
            action=self.name,
            name=name,
            state=state,
            payload=payload,
        )

        self.name = response["name"]
        self.edition = response["edition"]
        self.state = response["state"]
        self.payload = response["payload"]

    def edit(self, *, name: Optional[str] = None, payload: Optional[str] = None) -> None:
        """Update the action.

        Arguments:
            name: The new name of the action.
            payload: The new paylaod of the action.

        """
        self._edit(name=name, payload=payload)

    def enable(self) -> None:
        """Enable the action."""
        self._edit(state="ENABLED")

    def disable(self) -> None:
        """Disable the action."""
        self._edit(state="DISABLED")


class ActionManager:
    """This class defines the operations on the action in Graviti.

    Arguments:
        dataset: :class:`~graviti.manager.dataset.Dataset` instance.

    """

    def __init__(self, dataset: "Dataset") -> None:
        self._dataset = dataset

    def _generate(
        self, offset: int, limit: int, *, sort: SortParam
    ) -> Generator[Action, None, int]:
        _dataset = self._dataset
        _workspace = _dataset.workspace
        response = list_actions(
            _workspace.access_key,
            _workspace.url,
            _workspace.name,
            _dataset.name,
            sort=sort,
            offset=offset,
            limit=limit,
        )

        for item in response["actions"]:
            yield Action(_dataset, item)

        return response["total_count"]  # type: ignore[no-any-return]

    def create(self, name: str, payload: str) -> Action:
        """Create an action.

        Arguments:
            name: The name of the action.
            payload: The payload of the action.

        Returns:
            The :class:`.Action` instance with the given name.

        """
        _dataset = self._dataset
        _workspace = _dataset.workspace
        response = create_action(
            _workspace.access_key,
            _workspace.url,
            _workspace.name,
            _dataset.name,
            name=name,
            payload=payload,
        )

        return Action(_dataset, response)

    def get(self, name: str) -> Action:
        """Get the action with the given name.

        Arguments:
            name: The required action name.

        Raises:
            ResourceNameError: When the given name is an empty string.

        Returns:
            The :class:`.Action` instance with the given name.

        """
        check_type("name", name, str)
        if not name:
            raise ResourceNameError("action", name)

        _dataset = self._dataset
        _workspace = _dataset.workspace
        response = get_action(
            _workspace.access_key,
            _workspace.url,
            _workspace.name,
            _dataset.name,
            action=name,
        )

        return Action(_dataset, response)

    def list(self, *, sort: SortParam = None) -> LazyPagingList[Action]:
        """List the information of actions.

        Arguments:
            sort: The column and the direction the list result sorted by.

        Returns:
            The LazyPagingList of :class:`actions<.Action>` instances.

        """
        return LazyPagingList(
            lambda offset, limit: self._generate(offset, limit, sort=sort),
            LIMIT,
        )

    def delete(self, name: str) -> None:
        """Delete an action.

        Arguments:
            name: The name of the action to be deleted.

        Raises:
            ResourceNameError: When the given name is an empty string.

        """
        check_type("name", name, str)
        if not name:
            raise ResourceNameError("action", name)

        _workspace = self._dataset.workspace
        delete_action(
            _workspace.access_key,
            _workspace.url,
            _workspace.name,
            self._dataset.name,
            action=name,
        )
