..
 Copyright 2022 Graviti. Licensed under MIT License.

##################
 Sheet Management
##################

In the Graviti SDK, Sheet and DataFrame are interpretations of the same thing at different levels.

Sheet refers to the form of data organization that is one level lower than the dataset. One
dataset can have many different sheets, such as train, test, or frame-by-frame pictures from
different videos. Each sheet has its own schema.

Graviti SDK organizes the data of a sheet into a DataFrame format, which makes it more convenient
and intuitive to get and modify the data.

More details about the ``DataFrame`` and ``Schema`` are as follows:

.. toctree::
   :maxdepth: 1

   dataframe
   schema

In the Graviti SDK, the relationship between sheet name, DataFrame and dataset/draft is like the
relationship between key, value and dict. Thus, SDK supports managing sheets like manipulating the
dict in python.

The following will introduce more details about the sheet management methods in the SDK. First of
all, it is necessary to get a dataset:

.. code:: python

   from graviti import Workspace

   ws = Workspace(f"{YOUR_ACCESSKEY}")
   dataset = ws.datasets.get(f"{DATASET_NAME}")

****************
 Create a Sheet
****************

SDK supports managing sheets of open drafts. Thus, it is necessary to create or get an open
draft:

.. code:: python

   draft = dataset.drafts.create(f"{DRAFT_TITLE}")

Then users need to :ref:`features/sheet_management/dataframe:Initialize a DataFrame` like:

.. code:: python

   >>> from graviti import DataFrame
   >>> data = [
   ...    {"filename": "a.jpg"},
   ...    {"filename": "b.jpg"},
   ...    {"filename": "c.jpg"},
   ... ]
   >>> df1 = DataFrame(data)
   >>> df1
      filename
   0  a.jpg
   1  b.jpg
   2  c.jpg
 
Or getting a dataframe by copying other sheets:

.. code:: python

   >>> list(draft.keys())
   ["train"]
   >>> df2 = draft["train"].copy()
   >>> df2
      filename
   0  a.jpg
   1  b.jpg
   2  c.jpg

Next, users can create a new sheet:

.. code:: python

   draft[f"{SHEET_NAME}"] = df1
   draft.upload()

.. note::
   Only changes made on the open draft can be synchronized to Graviti via the method
   :meth:`~graviti.manager.draft.Draft.upload`.

Or replacing the old sheet:

.. code:: python

   draft["train"] = df2
   draft.upload()

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

*************
 Get a Sheet
*************

Get a sheet for the specified version of the dataset by name:

.. code:: python

   dataset.checkout(f"{REVISION}")
   dataset[f"{SHEET_NAME}"]

Get a sheet of an open draft by name:

.. code:: python

   draft = dataset.drafts.get({DRAFT_NUMBER})
   draft[f"{SHEET_NAME}"]

****************
 Delete a Sheet
****************

Delete a sheet of the open draft by name:

.. code:: python

   del draft[f"{SHEET_NAME}"]
   draft.upload()
