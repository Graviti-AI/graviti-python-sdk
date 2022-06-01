..
 Copyright 2022 Graviti. Licensed under MIT License.
 
########
 Branch
########

Each dataset is created with a default branch ``main``. When getting a dataset through the SDK,
its version is the latest commit of the default branch.

The following sections will introduce the operations and precautions related to branches
in the SDK. Of course, it is necessary to get a dataset first:

.. code:: python

   from graviti import Workspace

   ws = Workspace(f"{YOUR_ACCESSKEY}")
   dataset = ws.datasets.get(f"{DATASET_NAME}")

.. note::
   In most cases, a branch can be thought of as a named commit. But if there is a dataset that
   has just been created and has no commit history, the commit id of its default branch is
   ``None``.


*****************
 Create a Branch
*****************

SDK provides method :meth:`~graviti.manager.branch.BranchManager.create` to support creating a
branch based on a commit:

.. code:: python

   dataset.branches.create(f"{BRANCH_NAME}")
   dataset.branches.create(f"{BRANCH_NAME}", f"{BRANCH_NAME}")
   dataset.branches.create(f"{BRANCH_NAME}", f"{COMMIT_ID}")
   dataset.branches.create(f"{BRANCH_NAME}", f"{TAG_NAME}")

.. note::
   If no revision is specified, the created branch will be based on the current commit of the
   dataset, which can be viewed by ``dataset.HEAD.commit_id``.

.. warning::
   It is not allowed to create a new branch based on a branch with no commit history, for
   example:

   .. code:: python

      dataset = ws.datasets.create(f"{DATASET_NAME}")
      dataset.branches.create(f"{BRANCH_NAME}")

***************
 List Branches
***************

SDK provides method :meth:`~graviti.manager.branch.BranchManager.list` to support listing
branches:

.. code:: python

   dataset.branches.list()

**************
 Get a Branch
**************

SDK provides method :meth:`~graviti.manager.branch.BranchManager.get` to support getting a branch:

.. code:: python

   dataset.branches.get(f"{BRANCH_NAME}")

*****************
 Delete a Branch
*****************

SDK provides method :meth:`~graviti.manager.branch.BranchManager.delete` to support deleting a
branch:

.. code:: python

   dataset.branches.delete(f"{BRANCH_NAME}")

*****************
 Checkout Branch
*****************

SDK provides method :meth:`~graviti.manager.dataset.Dataset.checkout` to support switching the
version of the dataset by branches. The version of dataset can be viewed by ``dataset.HEAD``.

.. code:: python

   dataset.checkout(f"{BRANCH_NAME}")
   dataset.HEAD

.. note::
   This ``checkout`` method is often used to update the version of the local dataset, for example:

   .. code:: python

      dataset.checkout(f"{BRANCH_NAME}")
      draft = dataset.drafts.create(f"{DRAFT_TITLE}")
      draft.commit(f"{COMMIT_TITLE}")
      dataset.checkout(f"{BRANCH_NAME}")  # update dataset.HEAD