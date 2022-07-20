..
 Copyright 2022 Graviti. Licensed under MIT License.
 
########
 Draft
########

All operations related to modifying data require a draft first.
:class:`~graviti.manager.draft.Draft` can be viewed as a writable dataset in SDK.

The following sections will introduce the operations and precautions related to drafts
in the SDK. Of course, it is necessary to get a dataset first:

.. code:: python

   from graviti import Workspace

   ws = Workspace(f"{YOUR_ACCESSKEY}")
   dataset = ws.datasets.get(f"{DATASET_NAME}")

*****************
 Create a Draft
*****************

SDK provides method :meth:`~graviti.manager.draft.DraftManager.create` to support creating a
draft based on a branch:

.. code:: python

   dataset.drafts.create(f"{DRAFT_TITLE}", branch=f"{BRANCH_NAME}")

If no branch is specified, the created draft will be based on the current branch of the dataset:

.. code:: python

   dataset.drafts.create(f"{DRAFT_TITLE}")

In addition to title, it is also allowed to add description to the draft:

.. code:: python

   dataset.drafts.create(f"{DRAFT_TITLE}", f"{DRAFT_DESCRIPTION}")

.. important::
   Graviti use number, not title, to uniquely identify drafts.

*************
 List Drafts
*************

SDK provides method :meth:`~graviti.manager.draft.DraftManager.list` to support listing drafts.
Drafts can be filtered by state including ``OPEN``, ``COMMITTED``, ``CLOSED`` and ``ALL``:

.. code:: python

   dataset.drafts.list(f"{STATE}")

Drafts can also be filtered by the branch name:

.. code:: python

   dataset.drafts.list(branch=f"{BRANCH_NAME}")

If neither the state nor the branch name is specified, then all open drafts on all branches
will be returned:

.. code:: python

   dataset.drafts.list()

*************
 Get a Draft
*************

SDK provides method :meth:`~graviti.manager.draft.DraftManager.get` to support getting a draft
by number:

.. code:: python

   dataset.drafts.get(DRAFT_NUMBER)

****************
 Edit the Draft
****************

SDK provides method :meth:`~graviti.manager.draft.Draft.edit` to support changing the title and
description of the draft:

.. code:: python

   draft = dataset.drafts.get(DRAFT_NUMBER)
   draft.edit(f"{NEW_TITLE}", f"{NEW_DESCRIPTION}")

******************
 Upload the Draft
******************

SDK provides method :meth:`~graviti.manager.draft.Draft.upload` to support uploading the local
draft to Graviti. This step is essential if the user wants to save changes to the sheet
of the dataset.

.. code:: python

   draft = dataset.drafts.get(DRAFT_NUMBER)
   del draft["train"]
   draft.upload()

.. note::
   SDK supports specifying the max workers in multi-thread upload. The default is 8.

******************
 Commit the Draft
******************

SDK provides method :meth:`~graviti.manager.draft.Draft.commit` to support committing a draft.
This action means that a new commit will be created and all the changes from the draft will be
saved into this commit. Furthermore, it is not allowed to read or upload data on a committed draft:

.. code:: python

   draft = dataset.drafts.get(DRAFT_NUMBER)
   draft.commit(f"{COMMIT_TITLE}")

In addition to title, it is also allowed to add description to the commit:

.. code:: python

   draft.commit(f"{COMMIT_TITLE}", f"{COMMIT_DESCRIPTION}")

In this case, SDK will automatically update the version of the dataset after committing the draft.
And all modifications on the dataset will be lost.

.. code:: python

   >>> dataset = ws.datasets.get("Graviti-dataset-demo")
   >>> dataset.HEAD.name  # The version of the dataset is Branch("main").
   "main"
   >>> dataset.HEAD.commit_id
   "524d110ecae7474eaec9471f4a6c28b0"
   >>> draft = dataset.drafts.create("draft-4", branch="dev")
   >>> draft.commit("commit-4")
   Branch("dev")(
     (commit_id): '3db73ac2876a42c0bf43a0489ce1756a',
     (parent): Commit("1b21a40f03ab4cec814ec47ee0d10b24"),
     (title): 'commit-4',
     (committer): 'graviti-example',
     (committed_at): 2022-07-21 04:23:45+00:00
   )
   >>> dataset.HEAD.name  # The version of the dataset has been updated to Branch("dev").
   "dev"
   >>> dataset.HEAD.commit_id
   "3db73ac2876a42c0bf43a0489ce1756a"

Users can avoid the automatic update by setting ``update_dataset_head`` to False:

.. code:: python

   >>> dataset = ws.datasets.get("Graviti-dataset-demo")
   >>> dataset.HEAD.name  # The version of the dataset is Branch("main").
   "main"
   >>> dataset.HEAD.commit_id
   "524d110ecae7474eaec9471f4a6c28b0"
   >>> draft = dataset.drafts.create("draft-5", branch="dev")
   >>> draft.commit("commit-5", update_dataset_head=False)
   Branch("dev")(
     (commit_id): '781007a41d1641859c87cb00f8e32bf3',
     (parent): Commit("3db73ac2876a42c0bf43a0489ce1756a"),
     (title): 'commit-5',
     (committer): 'graviti-example',
     (committed_at): 2022-07-21 04:24:45+00:00
   )
   >>> dataset.HEAD.name  # The version of the dataset has not been updated.
   "main"
   >>> dataset.HEAD.commit_id
   "524d110ecae7474eaec9471f4a6c28b0"

*****************
 Close the Draft
*****************

SDK provides method :meth:`~graviti.manager.draft.Draft.close` to support closing a draft:
This action means that all changes made on this draft will be dropped. And the closed draft
cannot be reopened. Furthermore, it is not allowed to read or upload data on a closed draft:

.. code:: python

   draft = dataset.drafts.get(DRAFT_NUMBER)
   draft.close()
