..
   Copyright 2022 Graviti. Licensed under MIT License.

########
 Commit
########

:class:`~graviti.manager.commit.Commit` is the basic element of Graviti version control system. Each
commit of the dataset represents a **read-only** version.

The following sections will introduce the operations related to commits in the SDK. First of all, it
is necessary to get a dataset:

.. code:: python

   from graviti import Workspace

   ws = Workspace(f"{YOUR_ACCESSKEY}")
   dataset = ws.datasets.get(f"{DATASET_NAME}")

**************
 List Commits
**************

SDK provides method :meth:`~graviti.manager.commit.CommitManager.list` to support listing commits
preceding the given revision. The revision can be one commit ID:

.. code:: python

   dataset.commits.list(f"{COMMIT_ID}")

Or the branch name:

.. code:: python

   dataset.commits.list(f"{BRANCH_NAME}")

Or the tag name:

.. code:: python

   dataset.commits.list(f"{TAG_NAME}")

If no revision is specified, all commits preceding the current commit will be returned:

.. code:: python

   dataset.commits.list()

**************
 Get a Commit
**************

SDK provides method :meth:`~graviti.manager.commit.CommitManager.get` to support getting a commit by
revision. The revision can be one commit ID:

.. code:: python

   dataset.commits.get(f"{COMMIT_ID}")

Or the branch name:

.. code:: python

   dataset.commits.get(f"{BRANCH_NAME}")

Or the tag name:

.. code:: python

   dataset.commits.get(f"{TAG_NAME}")

If no revision is specified, the current commit of dataset will be returned:

.. code:: python

   dataset.commits.get()

*****************
 Checkout Commit
*****************

SDK provides method :meth:`~graviti.manager.dataset.Dataset.checkout` to support switching the
version of the dataset by commits:

.. code:: python

   dataset.checkout(f"{COMMIT_ID}")
