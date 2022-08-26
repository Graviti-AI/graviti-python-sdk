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

Edit Rows
=========

Edit one row:

.. code:: python

   df.loc[0] = {"filename": "d.jpg"}

Edit multiple rows:

.. code:: python

   df.loc[0:2] = [{"filename": "d.jpg"}, {"filename": "e.jpg"}]

Edit the Items of Column
========================

Edit one item:

.. code:: python

   df[f"{COLUMN_NAME}"][0] = "d.jpg"

Edit multiple items:

.. code:: python

   df[f"{COLUMN_NAME}"][0:2] = ["d.jpg", "e.jpg"]

Delete Rows
===========

Delete one row:

.. code:: python

   del df.loc[0]

Delete multiple rows:

.. code:: python

   del df.loc[0:2]

Extend Rows
===========

DataFrame supports method :py:meth:`~graviti.dataframe.frame.DataFrame.extend`.

Extend rows to the end of the DataFrame:

.. code:: python

   df.extend([{"filename": "a.jpg"}])

Extend another Dataframe to the end of the DataFrame:

.. code:: python

   df1 = DataFrame([{"filename": "a.jpg"}])
   df.extend(df1)

Add Columns
===========

DataFrame supports adding columns by ``setitem``:

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
   >>> df["caption"] = ["a", "b", "c"]
   >>> df
      filename  caption
   0  a.jpg     a
   1  b.jpg     b
   2  c.jpg     c
   >>> df.schema
   record(
     fields={
       'filename': string(),
       'caption': string(),
     },
   )

The above example shows adding a column of data with no specified type,
and the schema of the column will be inferred.
In this case, the column schema can only be Portex :ref:`features/sheet_management/schema:Primitive Types`.

If specific Portex type is required, please add a Series as the column to the DataFrame.

.. code:: python

   >>> from graviti import DataFrame, Series
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
   >>> df["category"] = Series(["cat", "dog", "cat"], pt.enum(["cat", "dog"]))
   >>> df
      filename  category
   0  a.jpg     cat
   1  b.jpg     dog
   2  c.jpg     cat
   >>> df.schema
   record(
     fields={
       'filename': string(),
       'category': enum(
         values=['cat', 'dog'],
       ),
     },
   )

Note that not all DataFrame can be modified.
Only if the fields of the schema are from given arguments, the DataFrame can be changed, like the above example.
If the fields are defined in a template, the DataFrame cannot be changed, and ``TypeError`` will be raised:

.. code:: python

   >>> from graviti import DataFrame, Workspace
   >>> import graviti.portex as pt

   >>> std = pt.build_package("https://github.com/Project-OpenBytes/portex-standard", "main")
   >>> box2ds = std.label.Box2D(
   ...     categories=["boat", "car"],
   ...     attributes={
   ...         "difficult": pt.boolean(),
   ...         "occluded": pt.boolean(),
   ...     },
   ... )
   >>> df = DataFrame(
   ...     [
   ...         {
   ...             "xmin": 1,
   ...             "ymin": 1,
   ...             "xmax": 4,
   ...             "ymax": 5,
   ...             "category": "boat",
   ...             "attribute": {
   ...                 "difficult": False,
   ...                 "occluded": False,
   ...             },
   ...         }
   ...     ],
   ...     schema=box2ds
   ... )
   >>> df
      xmin  ymin  xmax  ymax  category  attribute
                                        difficult  occluded
   0  1.0   1.0   4.0   5.0   boat      False      False
   >>> df["caption"] = ["a"]
   TypeError: Cannot set item 'caption' in ImmutableFields
