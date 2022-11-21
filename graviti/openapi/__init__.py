#!/usr/bin/env python3
#
# Copyright 2022 Graviti. Licensed under MIT License.
#

"""OpenAPI module."""

from graviti.openapi.action import (
    cancel_action_run,
    create_action,
    create_action_run,
    delete_action,
    get_action,
    get_action_run,
    get_action_run_node_log,
    list_action_runs,
    list_actions,
    update_action,
)
from graviti.openapi.branch import create_branch, delete_branch, get_branch, list_branches
from graviti.openapi.commit import commit_draft, get_commit, get_revision, list_commits
from graviti.openapi.data import (
    add_data,
    delete_data,
    list_commit_data,
    list_draft_data,
    update_data,
)
from graviti.openapi.dataset import (
    create_dataset,
    delete_dataset,
    get_dataset,
    list_datasets,
    update_dataset,
)
from graviti.openapi.draft import create_draft, get_draft, list_drafts, update_draft
from graviti.openapi.object import copy_objects, get_object_permission
from graviti.openapi.record import (
    add_records,
    delete_records,
    list_commit_records,
    list_draft_records,
    update_records,
)
from graviti.openapi.requests import do
from graviti.openapi.schema import update_schema
from graviti.openapi.search import (
    create_search_history,
    delete_search_history,
    get_search_history,
    get_search_record_count,
    list_search_histories,
    list_search_records,
)
from graviti.openapi.sheet import (
    create_sheet,
    delete_sheet,
    get_commit_sheet,
    get_draft_sheet,
    list_commit_sheets,
    list_draft_sheets,
)
from graviti.openapi.storage_config import (
    get_storage_config,
    list_storage_configs,
    update_storage_configs,
)
from graviti.openapi.tag import create_tag, delete_tag, get_tag, list_tags
from graviti.openapi.user import get_current_user
from graviti.openapi.workspace import get_current_workspace, get_workspace

RECORD_KEY = "__record_key"

__all__ = [
    "RECORD_KEY",
    "add_data",
    "add_records",
    "cancel_action_run",
    "commit_draft",
    "copy_objects",
    "create_action",
    "create_action_run",
    "create_branch",
    "create_dataset",
    "create_draft",
    "create_search_history",
    "create_sheet",
    "create_tag",
    "delete_action",
    "delete_branch",
    "delete_data",
    "delete_dataset",
    "delete_records",
    "delete_search_history",
    "delete_sheet",
    "delete_tag",
    "do",
    "get_action",
    "get_action_run",
    "get_action_run_node_log",
    "get_branch",
    "get_commit",
    "get_commit_sheet",
    "get_current_user",
    "get_current_workspace",
    "get_dataset",
    "get_draft",
    "get_draft_sheet",
    "get_object_permission",
    "get_revision",
    "get_search_history",
    "get_search_record_count",
    "get_storage_config",
    "get_tag",
    "get_workspace",
    "list_action_runs",
    "list_actions",
    "list_branches",
    "list_commit_data",
    "list_commit_records",
    "list_commit_sheets",
    "list_commits",
    "list_datasets",
    "list_draft_data",
    "list_draft_records",
    "list_draft_sheets",
    "list_drafts",
    "list_search_histories",
    "list_search_records",
    "list_storage_configs",
    "list_tags",
    "update_action",
    "update_data",
    "update_dataset",
    "update_draft",
    "update_records",
    "update_schema",
    "update_storage_configs",
]
