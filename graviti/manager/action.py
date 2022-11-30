#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""The implementation of the Action and ActionManager."""

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional

from graviti.exception import ResourceNameError
from graviti.manager.common import LIMIT
from graviti.manager.lazy import LazyPagingList
from graviti.openapi import (
    create_action,
    create_action_run,
    delete_action,
    get_action,
    get_action_run,
    list_action_runs,
    list_actions,
    update_action,
)
from graviti.utility import (
    CachedProperty,
    ReprMixin,
    SortParam,
    check_type,
    convert_iso_to_datetime,
)

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

    @property
    def runs(self) -> "RunManager":
        """Get class :class:`~graviti.manager.action.RunManager` instance.

        Returns:
            Required :class:`~graviti.manager.action.RunManager` instance.

        """
        return RunManager(self)

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


class Run(ReprMixin):  # pylint: disable=too-many-instance-attributes
    """This class defines the structure of an action run.

    Arguments:
        action: Class :class:`~graviti.manager.action.Action` instance.
        response: The response of the OpenAPI associated with the action run::

                {
                    "id": <str>,
                    "number": <int>,
                    "name": <str>,
                    "workflow_id": <str>,
                    "status": <int>,
                    "arguments": <dict>,
                    "started_at": <str>,
                    "ended_at": <str>,
                },

    Attributes:
        number: The number of this action run.
        name: The name of this action run.
        status: The status of this action run.
        arguments: The arguments of this action run.
        started_at: The start time of this action run.
        ended_at: The end time of this action run.
        duration: The duration of this action run.

    """

    _repr_attrs = ("status", "arguments", "started_at", "ended_at", "duration")

    def __init__(self, action: Action, response: Dict[str, Any]) -> None:
        self._action = action
        self._workflow_id = response["workflow_id"]
        self.number = response["number"]
        self.name = response["name"]
        self.status = response["status"]
        self.arguments: Dict[str, Any] = response["arguments"]
        self.started_at = convert_iso_to_datetime(response["started_at"])

        ended_at = response["ended_at"]
        if ended_at is not None:
            self.ended_at: Optional[datetime] = convert_iso_to_datetime(ended_at)
            self.duration: Optional[timedelta] = self.ended_at - self.started_at
        else:
            self.ended_at = None
            self.duration = None

        if "nodes" in response:
            nodes: Any = [Node(self, item) for item in response["nodes"]]
            self.nodes = nodes

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{self.name}")'

    @CachedProperty
    def nodes(self) -> List["Node"]:  # pylint: disable=method-hidden
        """Get the nodes of the action run.

        Returns:
            The nodes of the action run.

        """
        _action = self._action
        _dataset = _action._dataset  # pylint: disable=protected-access
        _workspace = _dataset.workspace

        response = get_action_run(
            _workspace.access_key,
            _workspace.url,
            _workspace.name,
            _dataset.name,
            action=_action.name,
            run_number=self.number,
        )
        return [Node(self, item) for item in response["nodes"]]

    @property
    def url(self) -> str:
        """Get the url of the action run.

        Returns:
            The url of the action run.

        Raises:
            ValueError: When the access key format is wrong.

        """
        _action = self._action
        _dataset = _action._dataset  # pylint: disable=protected-access
        _workspace = _dataset.workspace

        if _workspace.access_key.startswith("Accesskey-"):
            url = "https://gas.graviti.cn"
        elif _workspace.access_key.startswith("ACCESSKEY-"):
            url = "https://gas.graviti.com"
        else:
            raise ValueError("Wrong accesskey format!")

        return (
            f"{url}/dataset/{_workspace.name}/{_dataset.name}/actions/{_action.name}/"
            f"{self._workflow_id[len(_action.name)+1 :]}?fullId={self._workflow_id}"
        )


class RunManager:
    """This class defines the operations on the action run in Graviti.

    Arguments:
        action: :class:`~graviti.manager.action.Action` instance.

    """

    def __init__(self, action: Action) -> None:
        self._action = action

    def _generate(self, offset: int, limit: int, *, sort: SortParam) -> Generator[Run, None, int]:
        _action = self._action
        _dataset = _action._dataset  # pylint: disable=protected-access
        _workspace = _dataset.workspace
        response = list_action_runs(
            _workspace.access_key,
            _workspace.url,
            _workspace.name,
            _dataset.name,
            action=_action.name,
            sort=sort,
            offset=offset,
            limit=limit,
        )

        for item in response["runs"]:
            yield Run(_action, item)

        return response["total_count"]  # type: ignore[no-any-return]

    def create(self, arguments: Optional[Dict[str, Any]] = None) -> Run:
        """Run an action manually.

        Arguments:
            arguments: The arguments of the action run.

        Returns:
            The :class:`.Run` instance with the given arguments.

        """
        _action = self._action
        _dataset = _action._dataset  # pylint: disable=protected-access
        _workspace = _dataset.workspace
        response = create_action_run(
            _workspace.access_key,
            _workspace.url,
            _workspace.name,
            _dataset.name,
            action=_action.name,
            arguments=arguments,
        )

        return Run(_action, response)

    def get(self, run_number: int) -> Run:
        """Get the action with the given name.

        Arguments:
            run_number: The number of the action run.

        Returns:
            The :class:`.Action` instance with the given name.

        """
        check_type("run_number", run_number, int)

        _action = self._action
        _dataset = _action._dataset  # pylint: disable=protected-access
        _workspace = _dataset.workspace
        response = get_action_run(
            _workspace.access_key,
            _workspace.url,
            _workspace.name,
            _dataset.name,
            action=_action.name,
            run_number=run_number,
        )

        return Run(_action, response)

    def list(self, *, sort: SortParam = None) -> LazyPagingList[Run]:
        """List the information of action runs.

        Arguments:
            sort: The column and the direction the list result sorted by.

        Returns:
            The LazyPagingList of :class:`runs<.Run>` instances.

        """
        return LazyPagingList(
            lambda offset, limit: self._generate(offset, limit, sort=sort),
            LIMIT,
        )


class Node(ReprMixin):  # pylint: disable=too-many-instance-attributes
    """This class defines the structure of a node of action run.

    Arguments:
        run: Class :class:`~graviti.manager.action.Run` instance.
        response: The response of the OpenAPI associated with the action run node::

                {
                    "id": <str>,
                    "name": <str>,
                    "display_name": <str>,
                    "phase": <str>,
                    "started_at": <str>,
                    "ended_at": <str>,
                    "children": <list[str]>,
                },

    Attributes:
        node_id: The id of this action run node.
        name: The name of this action run node.
        display_name: The display name of this action run node.
        phase: The phase of this action run node.
        started_at: The start time of this action run node.
        ended_at: The end time of this action run node.
        duration: The duration of this action run node.
        children: The children of this action run node.

    """

    _repr_attrs = ("node_id", "phase", "started_at", "ended_at", "duration")

    def __init__(self, run: Run, response: Dict[str, Any]) -> None:
        self._run = run
        self.node_id: str = response["id"]
        self.name: str = response["name"]
        self.display_name: str = response["display_name"]
        self.phase: str = response["phase"]
        self.started_at = convert_iso_to_datetime(response["started_at"])

        ended_at = response["ended_at"]
        if ended_at is not None:
            self.ended_at: Optional[datetime] = convert_iso_to_datetime(ended_at)
            self.duration: Optional[timedelta] = self.ended_at - self.started_at
        else:
            self.ended_at = None
            self.duration = None

        self.children: List[str] = response["children"]

    def _repr_head(self) -> str:
        return f'{self.__class__.__name__}("{self.display_name}")'
