..
 Copyright 2022 Graviti. Licensed under MIT License.

#######################
 10 Minutes to Graviti
#######################

This is a simple introductory tutorial for beginners.

**************
 Registration
**************

Before using Graviti SDK, please finish the following registration steps:

- Please visit `Graviti`_ to sign up.
- Please visit `Graviti Developer Tools`_ to get an AccessKey.

.. _Graviti: https://www.graviti.com
.. _Graviti Developer Tools: https://gas.graviti.com/tensorbay/developer

.. note::
   An AccessKey is needed to authenticate identity when using Graviti SDK.

*********************
 Dataset Preparation
*********************

This step is only for users who do not have datasets in their workspace. By running the code
below, users can create a very simple dataset to experience Graviti SDK.

.. code:: python

   from graviti import DataFrame, Workspace
   import graviti.portex as pt

   ws = Workspace(f"{YOUR_ACCESSKEY}")
   dataset = ws.datasets.create("Graviti-dataset-demo")
   draft = dataset.drafts.create("Draft-1")

   standard = pt.build_package("https://github.com/Project-OpenBytes/standard", "main")
   box2ds = standard.label.Box2D(
       categories=["boat", "car"],
       attributes={
           "difficult": pt.boolean(),
           "occluded": pt.boolean(),
       },
   )
   schema = pt.record(
       {
           "filename": pt.string(),
           "box2ds": pt.array(box2ds),
       }
   )

   filenames = ["a.jpg", "b.jpg", "c.jpg"]
   data = []
   for filename in filenames:
       row_data = {
           "filename": filename,
           "box2ds": DataFrame(
               [
                   {
                       "xmin": 1,
                       "ymin": 1,
                       "xmax": 4,
                       "ymax": 5,
                       "category": "boat",
                       "attribute": {
                           "difficult": False,
                           "occluded": False,
                       },
                   }
               ]
           ),
       }
       data.append(row_data)
   draft["train"] = DataFrame(data=data, schema=schema)
   draft.upload()

   draft.commit("Commit-1")

   dataset.checkout("main")
   dataset.tags.create("v1.0")
   dataset.branches.create("dev")

***************
 Get a Dataset
***************

Workspace initialization:

.. code:: python

   from graviti import Workspace
   ws = Workspace(f"{YOUR_ACCESSKEY}")

List datasets on the workspace:

.. code:: python

   >>> ws.datasets.list()
   LazyPagingList [
     Dataset("changjun/Graviti-dataset-demo")(...)
   ]

Get one dataset:

.. code:: python

   >>> dataset = ws.datasets.get("Graviti-dataset-demo")
   >>> dataset
   Dataset("changjun/Graviti-dataset-demo")(
     (alias): '',
     (default_branch): 'main',
     (created_at): '2022-05-26T02:55:36Z',
     (updated_at): '2022-05-26T02:57:55Z',
     (is_public): False,
     (config): 'AmazonS3-us-west-1'
   )

***********************************
 Switch Between Different Versions
***********************************

View the current version of the dataset:

.. code:: python

   >>> dataset.HEAD
   Branch("main")(
     (commit_id): '47293b32f28c4008bc0f25b847b97d6f',
     (parent_commit_id): None,
     (title): 'Commit-1',
     (committer): 'czhual',
     (committed_at): '2022-05-26T02:57:00Z'
   )

List history commits:

.. code:: python

   >>> dataset.commits.list()
   LazyPagingList [
     Commit("47293b32f28c4008bc0f25b847b97d6f")(...)
   ]

List all branches:

.. code:: python

   >>> dataset.branches.list()
   LazyPagingList [
     Branch("main")(...),
     Branch("dev")(...)
   ]

List all tags:

.. code:: python

   >>> dataset.tags.list()
   LazyPagingList [
     Tag("v1.0")(...)
   ]

Checkout commit/branch/tag:

.. code:: python

   >>> dataset.checkout("47293b32f28c4008bc0f25b847b97d6f")  # commit id
   >>> dataset.HEAD
   Commit("47293b32f28c4008bc0f25b847b97d6f")(
     (parent_commit_id): None,
     (title): 'Commit-1',
     (committer): 'czhual',
     (committed_at): '2022-05-26T02:57:00Z'
   )

   >>> dataset.checkout("dev")  # branch name
   >>> dataset.HEAD
   Branch("dev")(
     (commit_id): '47293b32f28c4008bc0f25b847b97d6f',
     (parent_commit_id): None,
     (title): 'Commit-1',
     (committer): 'czhual',
     (committed_at): '2022-05-26T02:57:00Z'
   )

   >>> dataset.checkout("v1.0")  # tag name
   >>> dataset.HEAD
   Tag("v1.0")(
     (commit_id): '47293b32f28c4008bc0f25b847b97d6f',
     (parent_commit_id): None,
     (title): 'Commit-1',
     (committer): 'czhual',
     (committed_at): '2022-05-26T02:57:00Z'
   )


*************
 Get a Sheet
*************

List all sheets:

.. code:: python

   >>> list(dataset.keys())
   ['train']

Get a sheet:

.. code:: python

   >>> dataset["train"]
      filename  box2ds
   0  a.jpg     DataFrame(1, 6)
   1  b.jpg     DataFrame(1, 6)
   2  c.jpg     DataFrame(1, 6)

**************
 Get the Data
**************

Get the DataFrame:

.. code:: python

   >>> df = dataset["train"]
   >>> df
      filename  box2ds
   0  a.jpg     DataFrame(1, 6)
   1  b.jpg     DataFrame(1, 6)
   2  c.jpg     DataFrame(1, 6)


View the schema of the sheet:

.. code:: python

   >>> df.schema
   record(
     fields={
       'filename': string(),
       'box2ds': array(
         items=label.Box2D(
           coords=float32(),
           categories=['boat', 'car'],
           attributes={
             'difficult': boolean(),
             'occluded': boolean(),
           },
         ),
       ),
     },
   )

Get the data by rows or columns:

.. code:: python

   >>> df.loc[0]
   filename  a.jpg
   box2ds    DataFrame(1, 6)

.. code:: python

   >>> df["box2ds"]
   0  DataFrame(1, 6)
   1  DataFrame(1, 6)
   2  DataFrame(1, 6)

.. code:: python

   >>> df.loc[0]["box2ds"]
      xmin  ymin  xmax  ymax  category  attribute
                                        difficult  occluded
   0  1.0   1.0   4.0   5.0   boat      False      False

.. code:: python

   >>> df["box2ds"][0]
      xmin  ymin  xmax  ymax  category  attribute
                                        difficult  occluded
   0  1.0   1.0   4.0   5.0   boat      False      False


*****************
 Search the Data
*****************

Search the data within a specified sheet:

.. code:: python

   >>> criteria = {
   ...     "opt": "or",
   ...     "value": [
   ...         {
   ...              "opt": "eq",
   ...              "key": "filename",
   ...              "value": "a.jpg"
   ...          },
   ...         {
   ...              "opt": "eq",
   ...              "key": "filename",
   ...              "value": "b.jpg"
   ...          },
   ...     ]
   ... }
   >>> dataset.search("train", criteria)
      filename  box2ds
   0  a.jpg     DataFrame(1, 6)
   1  b.jpg     DataFrame(1, 6)
