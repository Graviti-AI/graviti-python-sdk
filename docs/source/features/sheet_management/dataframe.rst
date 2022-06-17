..
 Copyright 2022 Graviti. Licensed under MIT License.

###########
 DataFrame
###########

:class:`~graviti.dataframe.frame.DataFrame` is an integrated data structure
with an easy-to-use API for simplifying data processing in Dataset.
A Graviti DataFrame contains 2-dimensional tabular data and a Protex schema describing the names and types of each column.

************************
 Initialize a DataFrame
************************

Initialize a Dataframe from a list of dicts:

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

Initialize a DataFrame with multi-level column names:

.. code:: python

   >>> from graviti import DataFrame
   >>> data = [
   ...      {"attribute": {"weather": "sunny", "color": "red"}},
   ...      {"attribute": {"weather": "rainy", "color": "black"}},
   ...      {"attribute": {"weather": "sunny", "color": "white"}},
   ... ]
   >>> df = DataFrame(data)
   >>> df
      attribute
      color      weather
   0  red        sunny
   1  black      rainy
   2  white      sunny

Initialize a DataFrame with nested DataFrame construction:

.. code:: python

   >>> from graviti import DataFrame
   >>> data = [
   ...    {"points": [{"xmin": 1, "ymin": 3}, {"xmin": 5, "ymin": 8}]},
   ...    {"points": [{"xmin": 6, "ymin": 10}]},
   ...    {"points": [{"xmin": 1, "ymin": 3}, {"xmin": 5, "ymin": 8}, {"xmin": 1, "ymin": 9}]},
   ... ]
   >>> df = DataFrame(data)
   >>> df
      points
   0  DataFrame(2, 2)
   1  DataFrame(1, 2)
   2  DataFrame(3, 2)
   >>> df["points"][0]
      xmin  ymin
   0  1     3
   1  5     8

********************
 Read the DataFrame
********************

Read data by row:

.. code:: python

   df.loc[0]

Read data by column:

.. code:: python

   df[f"{COLUMN_NAME}"]

Read a DataFrame cell:

.. code:: python

   df.loc[0][f"{COLUMN_NAME}"]
   df[f"{COLUMN_NAME}"][0]

********************
 Edit the DataFrame
********************

DataFrame supports method :py:meth:`~graviti.dataframe.frame.DataFrame.extend`.

Extend rows to the end of the DataFrame:

.. code:: python

   df.extend([{"filename": "a.jpg"}])

Extend another Dataframe to the end of the DataFrame:

.. code:: python

   df1 = DataFrame([{"filename": "a.jpg"}])
   df.extend(df1)

****************
 File Operation
****************

Graviti SDK use the :class:`~graviti.utility.file.File` and :class:`~graviti.utility.file.RemoteFile`
to represent a specific file.

Load the local file into DataFrame:

.. code:: python

   import graviti.portex as pt
   from graviti import DataFrame
   from graviti.utility import File

   standard = pt.build_package("https://github.com/Project-OpenBytes/standard", "main")
   schema = pt.record({"file": standard.file.RemoteFile()})
   data = [
       {"file": File("PATH/TO/YOUR/FILE1")},
       {"file": File("PATH/TO/YOUR/FILE2")},
   ]
   df = DataFrame(data, schema)

Read the file in DataFrame:

.. code:: python

   file = df["file"][0]
   file.open().read()
