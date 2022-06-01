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
draft based on a branch. The title, description and branch of the draft can be specified, but
only title is required:

.. code:: python

   dataset.drafts.create(f"{DRAFT_TITLE}")
   dataset.drafts.create(f"{DRAFT_TITLE}", f"{DRAFT_DESCRIPTION}")
   dataset.drafts.create(f"{DRAFT_TITLE}", branch=f"{BRANCH_NAME}")

.. note::
   If no branch is specified, the created draft will be based on the current branch of the
   dataset, which can be viewed by ``dataset.HEAD``.

.. important::
   Graviti use number, not title, to uniquely identify drafts.

*************
 List Drafts
*************

SDK provides method :meth:`~graviti.manager.draft.DraftManager.list` to support listing drafts.
Drafts can be filtered by state (``OPEN``, ``COMMITTED``, ``CLOSED`` or ``ALL``) and branch.

.. code:: python

   dataset.drafts.list()
   dataset.drafts.list(f"{STATE}")
   dataset.drafts.list(branch=f"{BRANCH_NAME}")

.. note::
   If neither the state nor the branch name is specified, then all open drafts on all branches
   will be returned.

*************
 Get a Draft
*************

SDK provides method :meth:`~graviti.manager.draft.DraftManager.get` to support getting a draft:

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
saved into this commit.

.. code:: python

   draft = dataset.drafts.get(DRAFT_NUMBER)
   draft.commit(f"{COMMIT_TITLE}")
   draft.commit(f"{COMMIT_TITLE}", f"{COMMIT_DESCRIPTION}")

.. warning::
   It is not allowed to read or upload data on a committed draft.

*****************
 Close the Draft
*****************

SDK provides method :meth:`~graviti.manager.draft.Draft.close` to support closing a draft:
This action means that all changes made on this draft will be dropped. And the closed draft
cannot be reopened.

.. code:: python

   draft = dataset.drafts.get(DRAFT_NUMBER)
   draft.close()

.. warning::
   It is not allowed to read or upload data on a closed draft.