..
   Copyright 2022 Graviti. Licensed under MIT License.

##################
 Sheet Management
##################

In the Graviti SDK, Sheet and DataFrame are interpretations of the same thing at different levels.

Sheet refers to the form of data organization that is one level lower than the dataset. One dataset
can have many different sheets, such as train, test, or frame-by-frame pictures from different
videos. Each sheet has its own schema.

Graviti SDK organizes the data of a sheet into a DataFrame format, which makes it more convenient
and intuitive to get and modify the data.

More details about the ``DataFrame``, ``Binary Files``, ``Schema`` and ``Search`` are as follows:

.. toctree::
   :maxdepth: 1

   dataframe
   binary_files
   schema
   search

In the Graviti SDK, the relationship between sheet name, DataFrame and dataset is like the
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

First :ref:`features/sheet_management/dataframe:Initialize a DataFrame`:

.. code:: python

   >>> from graviti import DataFrame
   >>> data = [
   ...    {"filename": "a.jpg"},
   ...    {"filename": "b.jpg"},
   ...    {"filename": "c.jpg"},
   ... ]
   >>> df = DataFrame(data)
   >>> df
      filename
   0  a.jpg
   1  b.jpg
   2  c.jpg

Set the DataFrame into dataset, then commit it:

.. code:: python

   dataset[f"{SHEET_NAME}"] = df
   dataset.commit(f"{COMMIT_MESSAGE}")

*************
 List Sheets
*************

List sheet names :

.. code:: python

   list(dataset.keys())

*************
 Get a Sheet
*************

Get a sheet by name:

.. code:: python

   dataset[f"{SHEET_NAME}"]

****************
 Delete a Sheet
****************

Delete a sheet by name:

.. code:: python

   del dataset[f"{SHEET_NAME}"]
   dataset.commit(f"{COMMIT_MESSAGE}")
