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
branch based on a revision. The revision can be one commit ID:

.. code:: python

   dataset.branches.create(f"{BRANCH_NAME}", f"{COMMIT_ID}")

The revision can also be the branch name. In this situation, the new branch will be created based
on the latest commit of the source branch:

.. code:: python

   dataset.branches.create(f"{BRANCH_NAME}", f"{SOURCE_BRANCH_NAME}")

The revision can also be the tag name:

.. code:: python

   dataset.branches.create(f"{BRANCH_NAME}", f"{TAG_NAME}")

If no revision is specified, the created branch will be based on the current commit of the
dataset:

.. code:: python

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

SDK provides method :meth:`~graviti.manager.branch.BranchManager.get` to support getting a branch
by name:

.. code:: python

   dataset.branches.get(f"{BRANCH_NAME}")

*****************
 Delete a Branch
*****************

SDK provides method :meth:`~graviti.manager.branch.BranchManager.delete` to support deleting a
branch by name:

.. code:: python

   dataset.branches.delete(f"{BRANCH_NAME}")

*****************
 Checkout Branch
*****************

SDK provides method :meth:`~graviti.manager.dataset.Dataset.checkout` to support switching the
version of the dataset by branches:

.. code:: python

   dataset.checkout(f"{BRANCH_NAME}")
   dataset.HEAD  # Check whether the dataset version is correct.

In addition, this ``checkout`` method is often used to update the version of the local dataset
without getting the dataset again, for example:

.. code:: python

   # Other users committed a draft on the Branch("main").
   dataset.checkout("main")  # Update the version of the dataset.
