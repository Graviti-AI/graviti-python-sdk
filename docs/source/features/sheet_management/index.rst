..
 Copyright 2022 Graviti. Licensed under MIT License.

##################
 Sheet Management
##################

In the Graviti SDK, ``Sheet`` and ``DataFrame`` are interpretations of the same thing at
different levels.

Sheet refers to the form of data organization that is one level lower than the dataset. One
dataset can have many different sheets, such as train, test, or frame-by-frame pictures from
different videos. Each sheet has its own schema.

Graviti SDK organizes the data of a sheet into a DataFrame format, which makes it more convenient
and intuitive to get and modify the data.

More details about the ``Sheet`` and ``Schema`` are as follows:

.. toctree::
   :maxdepth: 1

   dataframe
   schema

The following will introduce the sheet-related operations in the SDK. Of course, it is
necessary to get a dataset first:

.. code:: python

   from graviti import Workspace

   ws = Workspace(f"{YOUR_ACCESSKEY}")
   dataset = ws.datasets.get(f"{DATASET_NAME}")

****************
 Create a Sheet
****************

Create a sheet on the open draft:

.. code:: python

   from graviti import DataFrame

   draft = dataset.drafts.create(f"{DRAFT_TITLE}")
   draft[f"{SHEET_NAME}"] = DataFrame(data=data, schema=schema)
   draft[f"{SHEET_NAME}"] = DataFrame(schema=schema)
   draft.upload()

.. note::
   Only changes made on the open draft can be synchronized to Graviti via the method
   :meth:`~graviti.manager.draft.Draft.upload`.

.. important::
   When setting sheets, if the sheet with the corresponding name already exists, SDK will delete
   the original one and create a new one.

*************
 List Sheets
*************

List sheets for the specified version of the dataset:

.. code:: python

   dataset.checkout(f"{COMMIT_ID}")
   list(dataset.keys())

List sheets of an open draft:

.. code:: python

   draft = dataset.drafts.get({DRAFT_NUMBER})
   list(draft.keys())

SDK also supports listing the commit/tag/branch sheets directly:

.. code:: python

   commit = dataset.commits.get()
   list(commit.keys())

   branch = dataset.branches.get(f"{BRANCH_NAME}")
   list(branch.keys())

   tag = dataset.tags.get(f"{TAG_NAME}")
   list(tag.keys())

*************
 Get a Sheet
*************

Get the sheet for the specified version of the dataset:

.. code:: python

   dataset.checkout(f"{COMMIT_ID}")
   dataset[f"{SHEET_NAME}"]

Get a sheet of an open draft:

.. code:: python

   draft = dataset.drafts.get({DRAFT_NUMBER})
   draft[f"{SHEET_NAME}"]

SDK also supports getting the commit/tag/branch sheet directly:

.. code:: python

   commit = dataset.commits.get()
   commit[f"{SHEET_NAME}"]

   branch = dataset.branches.get(f"{BRANCH_NAME}")
   branch[f"{SHEET_NAME}"]

   tag = dataset.tags.get(f"{TAG_NAME}")
   tag[f"{SHEET_NAME}"]

****************
 Delete a Sheet
****************

Delete a sheet of the open draft:

.. code:: python

   del draft[f"{SHEET_NAME}"]
   draft.pop(f"{SHEET_NAME}")
