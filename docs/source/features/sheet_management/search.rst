..
 Copyright 2022 Graviti. Licensed under MIT License.

########
 Search
########

This topic describes DataFrame search methods:

* :py:meth:`~graviti.dataframe.frame.DataFrame.query`
* :py:meth:`~graviti.dataframe.frame.DataFrame.apply`

********************
Dataset Preparation
********************

Take the following DataFrame as an example:

.. code:: python

   from graviti import DataFrame
   import graviti.portex as pt

   points_schema = pt.array(
       pt.record(
           {
               "xmin": pt.int32(),
               "ymin": pt.int32(),
               "category": pt.enum(["boat", "car"]),
           }
       )
   )
   schema = pt.record(
       {
           "filename": pt.string(),
           "points": points_schema,
       }
   )
   data = []
   for filename in ("a.jpg", "b.jpg", "c.jpg"):
       row_data = {
           "filename": filename,
           "points": [
               {
                   "xmin": 1,
                   "ymin": 1,
                   "category": "boat",
               },
               {
                   "xmin": 100,
                   "ymin": 100,
                   "category": "car" if filename == "a.jpg" else "boat",
               },
           ],
       }
       data.append(row_data)
   df = DataFrame(data, schema)

.. code:: python

   >>> df
      filename  points
   0  a.jpg     DataFrame(2, 3)
   1  b.jpg     DataFrame(2, 3)
   2  c.jpg     DataFrame(2, 3)

Upload the DataFrame:

.. code:: python

   from graviti import Workspace
   ws = Workspace(f"{YOUR_ACCESSKEY}")
   dataset = ws.datasets.create("Graviti-dataset-demo")
   draft = dataset.drafts.create("Draft-1")
   draft["train"] = DataFrame(data=data, schema=schema)
   draft.upload()
   draft.commit("Commit-1")

Get the uploaded DataFrame:

.. code:: python

   df = dataset["train"]

*******
 Query
*******

The query operation will use the lambda function to evaluate each rows,
and return the True rows.
The lambda function must return a boolean value.

SDK uses the ``engine.online()`` to start online searching.
For example, search for all rows with filename as "a.jpg":

.. code:: python

   >>> from graviti import engine
   >>> with engine.online():
   ...    result = df.query(lambda x: x["filename"] == "a.jpg")
   >>> result
      filename  points
   0  a.jpg     DataFrame(2, 3)

SDK use ``any()`` to match points in rows where at least one category is boat:

.. code:: python

   >>> from graviti import engine
   >>> with engine.online():
   ...    result = df.query(lambda x: (x["points"]["category"]=="boat").any())
   >>> result
      filename  points
   0  a.jpg     DataFrame(2, 3)
   1  b.jpg     DataFrame(2, 3)
   2  c.jpg     DataFrame(2, 3)

*******
 Apply
*******

The apply operation will apply the lambda function to DataFrame row by row.

Search all points with the categories of "car":

.. code:: python

   >>> from graviti import engine
   >>> with engine.online():
   ...    result = df.apply(lambda x: x["points"].query(lambda y: y["category"]=="car"))
   >>> result
   0  DataFrame(1, 3)
   1  DataFrame(0, 3)
   2  DataFrame(0, 3)

***************
 Query & Apply
***************

SDK also supports calling ``apply()`` after the ``query()``.

Search all rows with the points category has "car" and remove null rows:

.. code:: python

   >>> from graviti import engine
   >>> with engine.online():
   ...     result = df.query(lambda x: (x["points"]["category"] == "car").any()).apply(
   ...         lambda x: x["points"].query(lambda y: y["category"] == "car")
   ...     )
   >>> result
   0  DataFrame(1, 3)
