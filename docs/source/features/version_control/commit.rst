..
 Copyright 2022 Graviti. Licensed under MIT License.
 
########
 Commit
########

:class:`~graviti.manager.commit.Commit` is the basic element of Graviti version control system.
Each commit of the dataset represents a **read-only** version.

The following sections will introduce the operations related to commits in the SDK.
First of all, it is necessary to get a dataset:

.. code:: python

   from graviti import Workspace

   ws = Workspace(f"{YOUR_ACCESSKEY}")
   dataset = ws.datasets.get(f"{DATASET_NAME}")

**************
 List Commits
**************

SDK provides method :meth:`~graviti.manager.commit.CommitManager.list` to support listing
commits preceding the given revision:

.. code:: python

   dataset.commits.list()
   dataset.commits.list(f"{COMMIT_ID}")
   dataset.commits.list(f"{BRANCH_NAME}")
   dataset.commits.list(f"{TAG_NAME}")

.. note::
   If no revision is specified, all commits preceding the current commit wiil be returned.

**************
 Get a Commit
**************

SDK provides method :meth:`~graviti.manager.commit.CommitManager.get` to support getting a
commit by revision:

.. code:: python

   dataset.commits.get()
   dataset.commits.get(f"{COMMIT_ID}")
   dataset.commits.get(f"{BRANCH_NAME}")
   dataset.commits.get(f"{TAG_NAME}")

.. note::
   If no revision is specified, the current commit of dataset wiil be returned.

*****************
 Checkout Commit
*****************

SDK provides method :meth:`~graviti.manager.dataset.Dataset.checkout` to support switching the
version of the dataset by commits. The version of dataset can be viewed by ``dataset.HEAD``.

.. code:: python

   dataset.checkout(f"{COMMIT_ID}")
   dataset.HEAD
