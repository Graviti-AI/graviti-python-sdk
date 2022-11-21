..
   Copyright 2022 Graviti. Licensed under MIT License.

########
 Search
########

This topic describes DataFrame search methods:

-  :py:meth:`~graviti.dataframe.frame.DataFrame.query`
-  :py:meth:`~graviti.dataframe.frame.DataFrame.apply`

*********************
 Dataset Preparation
*********************

Take the following DataFrame as an example:

.. code:: python

   from graviti import DataFrame
   import graviti.portex as pt

   std = pt.build_package("https://github.com/Project-OpenBytes/portex-standard", "main")
   schema = pt.record(
       {
           "filename": pt.string(),
           "box2ds": pt.array(
               std.label.Box2D(
                   categories=["boat", "car"],
               )
           )
       }
   )

   data = []
   for filename in ("a.jpg", "b.jpg", "c.jpg"):
       data.append(
           {
               "filename": filename,
               "box2ds": [
                   {
                       "xmin": 10,
                       "ymin": 10,
                       "xmax": 100,
                       "ymax": 100,
                       "category": "boat",
                   },
                   {
                       "xmin": 20,
                       "ymin": 20,
                       "xmax": 200,
                       "ymax": 200,
                       "category": "car" if filename == "a.jpg" else "boat",
                   },
               ],
           }
       )

   df = DataFrame(data, schema)

.. code:: python

   >>> df
      filename  box2ds
   0  a.jpg     DataFrame(2, 5)
   1  b.jpg     DataFrame(2, 5)
   2  c.jpg     DataFrame(2, 5)

Upload the DataFrame:

.. code:: python

   from graviti import Workspace

   ws = Workspace(f"{YOUR_ACCESSKEY}")

   dataset = ws.datasets.create("search_demo")
   dataset["train"] = df
   dataset.commit("initial commit")

Get the uploaded DataFrame:

.. code:: python

   df = dataset["train"]

*******
 Query
*******

The query operation will use the lambda function to evaluate each rows, and return the True rows.
The lambda function must return a boolean value.

SDK uses the ``engine.online()`` to start online searching. For example, search for all rows with
filename as "a.jpg":

.. code:: python

   >>> from graviti import engine
   >>> with engine.online():
   ...    result = df.query(lambda x: x["filename"] == "a.jpg")
   >>> result
      filename  box2ds
   0  a.jpg     DataFrame(2, 5)

SDK use ``any()`` to match box2ds in rows where at least one category is boat:

.. code:: python

   >>> from graviti import engine
   >>> with engine.online():
   ...    result = df.query(lambda x: (x["box2ds"]["category"]=="boat").any())
   >>> result
      filename  box2ds
   0  a.jpg     DataFrame(2, 5)
   1  b.jpg     DataFrame(2, 5)
   2  c.jpg     DataFrame(2, 5)

SDK use ``all()`` to match box2ds in rows whose category are all boat:

.. code:: python

   >>> from graviti import engine
   >>> with engine.online():
   ...    result = df.query(lambda x: (x["box2ds"]["category"]=="boat").all())
   >>> result
      filename  box2ds
   0  b.jpg     DataFrame(2, 5)
   1  c.jpg     DataFrame(2, 5)

*******
 Apply
*******

The apply operation will apply the lambda function to DataFrame row by row.

Search all box2ds with the categories of "car":

.. code:: python

   >>> from graviti import engine
   >>> with engine.online():
   ...    result = df.apply(lambda x: x["box2ds"].query(lambda y: y["category"]=="car"))
   >>> result
   0  DataFrame(1, 5)
   1  DataFrame(0, 5)
   2  DataFrame(0, 5)

***************
 Query & Apply
***************

SDK also supports calling ``apply()`` after the ``query()``.

Search all rows with the box2ds category has "car" and remove null rows:

.. code:: python

   >>> from graviti import engine
   >>> with engine.online():
   ...     result = df.query(lambda x: (x["box2ds"]["category"] == "car").any()).apply(
   ...         lambda x: x["box2ds"].query(lambda y: y["category"] == "car")
   ...     )
   >>> result
   0  DataFrame(1, 5)
