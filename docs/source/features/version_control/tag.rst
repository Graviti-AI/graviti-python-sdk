..
 Copyright 2022 Graviti. Licensed under MIT License.
 
#####
 Tag
#####

Graviti supports tagging specific commits in a dataset's history as being important, for
example, to mark release revisions (v1.0, v2.0 and so on).

Before operating tags, a dataset with existing commits is needed:

.. code:: python

   from graviti import Workspace

   ws = Workspace(f"{YOUR_ACCESSKEY}")
   dataset = ws.datasets.get(f"{DATASET_NAME}")

**************
 Create a Tag
**************

SDK provides method :meth:`~graviti.manager.tag.TagManager.create` to support creating a
tag based on a commit:

.. code:: python

   dataset.tags.create(f"{TAG_NAME}")
   dataset.tags.create(f"{TAG_NAME}", f"{BRANCH_NAME}")
   dataset.tags.create(f"{TAG_NAME}", f"{COMMIT_ID}")
   dataset.tags.create(f"{TAG_NAME}", f"{TAG_NAME}")

.. note::
   If no tag is specified, the created tag will be based on the current commit of the
   dataset, which can be viewed by ``dataset.HEAD.commit_id``.

.. warning::
   It is not allowed to create a new tag based on a branch with no commit history, for example:

   .. code:: python

      dataset = ws.datasets.create(f"{DATASET_NAME}")
      dataset.tags.create(f"{TAG_NAME}")

***********
 List Tags
***********

SDK provides method :meth:`~graviti.manager.tag.TagManager.list` to support listing tags:

.. code:: python

   dataset.tags.list()

***********
 Get a Tag
***********

SDK provides method :meth:`~graviti.manager.tag.TagManager.get` to support getting a tag:

.. code:: python

   dataset.tags.get(f"{TAG_NAME}")

**************
 Delete a Tag
**************

SDK provides method :meth:`~graviti.manager.tag.TagManager.delete` to support deleting a
tag:

.. code:: python

   dataset.tags.delete(f"{TAG_NAME}")

**************
 Checkout Tag
**************

SDK provides method :meth:`~graviti.manager.dataset.Dataset.checkout` to support switching the
version of the dataset by tags. The version of dataset can be viewed by ``dataset.HEAD``.

.. code:: python

   dataset.checkout(f"{TAG_NAME}")
   dataset.HEAD
