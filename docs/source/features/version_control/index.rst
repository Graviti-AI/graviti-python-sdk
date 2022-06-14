..
 Copyright 2022 Graviti. Licensed under MIT License.

#################
 Version Control
#################

Version control is one of the important features of Graviti. It can help teams or individual
users develop datasets in parallel and trace the history of the data.

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

More details about the version control methods are as follows:

.. toctree::
   :maxdepth: 1

   commit
   branch
   draft
   tag
