..
 Copyright 2022 Graviti. Licensed under MIT License.

#################
 Version Control
#################

GravitiSDK supports version control of a dataset on commits, drafts, branches and tags.
All the version control operations are performed throught related managers.

preparation
===========

Create a dataset on the authorized workspace.

.. code:: python

   from graviti import Workspace

   # Please visit `https://gas.graviti.cn/tensorbay/developer` to get the AccessKey.
   ws = Workspace(f"{YOUR_ACCESSKEY}")
   dataset = gas.create_dataset(f"{DATASET_NAME}")


********
 Drafts
********

A draft represents a writable workspace.
When a dataset is first created, there are no commits or data, a draft should be created to add data.
And the changes of the draft can be committed into a read-only version.

create draft
============

A draft can be created based on the default branch or the given branch of the dataset.

.. code:: python

   draft = dataset.drafts.create(f"{DRAFT_TITLE}", f"{DRAFT_DESCRIPTION}")
   draft = dataset.drafts.create(f"{DRAFT_TITLE}", branch=f"{BRANCH_NAME}")

list drafts
===========

The drafts of a dataset with a specified state or branch can be listed. The draft state includes "OPEN", "CLOSED", "COMMITTED", "ALL".
By default, open drafts on the default branch are listed.


.. code:: python

   dataset.drafts.list()
   dataset.drafts.list("COMMITTED", f"{BRANCH_NAME}")

get draft
=========

A draft can be obtained with its draft number.

.. code:: python

   draft_number = 1
   draft = dataset.drafts.get(draft_number)

commit draft
============

After done managing data of the draft, the changes can be committed into a read-only version.

.. code:: python

   commit = draft.commit(f"{COMMIT_MESSAGE}")


*********
 Commits
*********

A commit of a dataset is a **read-only** version and comes from a draft.

list commits
============

The history commits of a dataset on the default branch or before a specific revision can be listed.

.. code:: python

   dataset.commits.list()
   dataset.commits.list(f"{BRANCH_NAME}")
   dataset.commits.list(f"{COMMIT_ID}")
   dataset.commits.list(f"{TAG_NAME}")

get commit
==========

The commit of a dataset on the current or a specified revision can be obtained.

.. code:: python

   commit = dataset.commits.get()
   commit = dataset.commits.get(f"{COMMIT_ID}")
   commit = dataset.commits.get(f"{BRANCH_NAME}")
   commit = dataset.commits.get(f"{TAG_NAME}")


**********
 Branches
**********

GravitiSDK supports branches to diverge from the main line of development.

create branch
=============

A branch of a dataset on the current or a specified revision can be created.

.. code:: python

   branch = dataset.branches.create(f"{BRANCH_NAME}")
   branch = dataset.branches.create(f"{BRANCH_NAME}", f"{BRANCH_NAME}")
   branch = dataset.branches.create(f"{BRANCH_NAME}", f"{COMMIT_ID}")
   branch = dataset.branches.create(f"{BRANCH_NAME}", f"{TAG_NAME}")

list branches
=============

The branches of a dataset can be listed.

.. code:: python

   dataset.branches.list()

get branch
==========

A specific branch of a dataset can be obtained by its name.

.. code:: python

   branch = dataset.branches.get(f"{BRANCH_NAME}")


delete branch
=============

A specific branch of a dataset can be deleted by its name.

.. code:: python

   dataset.branches.delete(f"{BRANCH_NAME}")


******
 Tags
******

Graviti data platform supports tagging specific commits in a dataset’s history as being important. Typically, tags are used to mark release revisions (v1.0, v2.0 and so on).

create tag
==========

A tag of a dataset on the current or a specified revision can be created.

.. code:: python

   tag = dataset.tags.create(f"{TAG_NAME}")
   tag = dataset.tags.create(f"{TAG_NAME}", f"{BRANCH_NAME}")
   tag = dataset.tags.create(f"{TAG_NAME}", f"{COMMIT_ID}")
   tag = dataset.tags.create(f"{TAG_NAME}", f"{TAG_NAME}")

list tags
=========

The tags of a dataset can be listed.

.. code:: python

   dataset.tags.list()

get tag
=======

A specific tag of a dataset can be obtained by its name.

.. code:: python

   tag = dataset.tags.get(f"{TAG_NAME}")


delete tag
==========

A specific tag of a dataset can be deleted by its name.

.. code:: python

   dataset.tags.delete(f"{TAG_NAME}")
