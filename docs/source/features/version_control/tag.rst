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
tag based on a revision. The revision can be one commit ID:

.. code:: python

   dataset.tags.create(f"{TAG_NAME}", f"{COMMIT_ID}")

The revision can also be the branch name. In this situation, the tag will be created based
on the latest commit of the branch:

.. code:: python

   dataset.tags.create(f"{TAG_NAME}", f"{BRANCH_NAME}")

The revision can also be the tag name. SDK supports creating multiple tags based on the same commit:

.. code:: python

   dataset.tags.create(f"{TAG_NAME}", f"{SOURCE_TAG_NAME}")

If no tag is specified, the created tag will be based on the current commit of the dataset:

.. code:: python

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

SDK provides method :meth:`~graviti.manager.tag.TagManager.get` to support getting a tag by name:

.. code:: python

   dataset.tags.get(f"{TAG_NAME}")

**************
 Delete a Tag
**************

SDK provides method :meth:`~graviti.manager.tag.TagManager.delete` to support deleting a tag by
name:

.. code:: python

   dataset.tags.delete(f"{TAG_NAME}")

**************
 Checkout Tag
**************

SDK provides method :meth:`~graviti.manager.dataset.Dataset.checkout` to support switching the
version of the dataset by tags:

.. code:: python

   dataset.checkout(f"{TAG_NAME}")
   # Check whether the dataset version is correct.
   dataset.HEAD
