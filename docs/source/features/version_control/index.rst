..
 Copyright 2022 Graviti. Licensed under MIT License.

#################
 Version Control
#################

Version control is one of the important features of Graviti. It can help teams or individual
users develop datasets in parallel and trace the history of the data.

******
 HEAD
******

SDK supports viewing the version of the dataset through property
:attr:`~graviti.manager.dataset.Dataset.HEAD`:

.. code:: python

   >>> from graviti import Workspace
   >>> ws = Workspace(f"{YOUR_ACCESSKEY}")
   >>> dataset = ws.datasets.get("Graviti-dataset-demo")
   >>> dataset.HEAD
   Branch("main")(
     (commit_id): '47293b32f28c4008bc0f25b847b97d6f',
     (parent_commit_id): None,
     (title): 'Commit-1',
     (committer): 'graviti-example',
     (committed_at): '2022-05-26T02:57:00Z'
   )
   >>> dataset.HEAD.name
   "main"
   >>> dataset.HEAD.commit_id
   "47293b32f28c4008bc0f25b847b97d6f"

**********
 Checkout
**********

SDK provides method :meth:`~graviti.manager.dataset.Dataset.checkout` to support switching the
version of the dataset. This method will modify the ``HEAD`` of the dataset and discard the
previous modification on the dataset.

Checkout the ``Branch("dev")``:

.. code:: python

   >>> from graviti import Workspace
   >>> ws = Workspace(f"{YOUR_ACCESSKEY}")
   >>> dataset = ws.datasets.get("Graviti-dataset-demo")
   >>> dataset.HEAD
   Branch("main")(
     (commit_id): '47293b32f28c4008bc0f25b847b97d6f',
     (parent_commit_id): None,
     (title): 'Commit-1',
     (committer): 'graviti-example',
     (committed_at): '2022-05-26T02:57:00Z'
   )
   >>> dataset.checkout("dev")
   Branch("dev")(
     (commit_id): '781007a41d1641859c87cb00f8e32bf3',
     (parent_commit_id): '3db73ac2876a42c0bf43a0489ce1756a',
     (title): 'commit-5',
     (committer): 'graviti-example',
     (committed_at): '2022-06-17T08:01:37Z'
   )

Checkout the ``Tag("v1.0")``:

.. code:: python

   >>> dataset.checkout("v1.0")
   Tag("v1.0")(
     (commit_id): '2cd44960e0bf486c950536f7eeebc482',
     (parent_commit_id): 'e8dc893eb2344b9a98bddce71a1c0eab',
     (title): 'commit-7',
     (committer): 'graviti-example',
     (committed_at): '2022-06-09T09:34:46Z'
   )

Checkout the ``Commit("2cd4496")``:

.. code:: python
 
   >>> dataset.checkout("2cd44960e0bf486c950536f7eeebc482")
   Commit("2cd44960e0bf486c950536f7eeebc482")(
     (parent_commit_id): 'e8dc893eb2344b9a98bddce71a1c0eab',
     (title): 'commit-7',
     (committer): 'graviti-example',
     (committed_at): '2022-06-09T09:34:46Z'
   )

**************
 More Details
**************

More details about the version control methods are as follows:

.. toctree::
   :maxdepth: 1

   commit
   branch
   draft
   tag
