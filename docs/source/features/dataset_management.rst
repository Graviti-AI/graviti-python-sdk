..
 Copyright 2022 Graviti. Licensed under MIT License.

####################
 Dataset Management
####################

:class:`~graviti.manager.dataset.Dataset` is the most basic concept in Graviti SDK. Almost all
operations require a dataset first. Of course, it is necessary to initialize a
:class:`~graviti.workspace.Workspace` before managing the dataset:

.. code:: python

   from graviti import Workspace
   ws = Workspace(f"{YOUR_ACCESSKEY}")

******************
 Create a Dataset
******************

SDK provides method :meth:`~graviti.manager.dataset.DatasetManager.create` to support creating
a dataset. In addition to the name, alias and storage config can also be specified:

.. code:: python

   ws.datasets.create(f"{DATASET_NAME}")

.. note::
   Unlike the operation on the web page, here SDK will not create an empty draft after
   creating the dataset.

***************
 List Datasets
***************

SDK provides method :meth:`~graviti.manager.dataset.DatasetManager.list` to support listing
datasets on the workspace:

.. code:: python

   ws.datasets.list()

***************
 Get a Dataset
***************

SDK provides method :meth:`~graviti.manager.dataset.DatasetManager.get` to support getting
a dataset:

.. code:: python

   ws.datasets.get(f"{DATASET_NAME}")

******************
 Delete a Dataset
******************

SDK provides method :meth:`~graviti.manager.dataset.DatasetManager.delete` to support deleting
a dataset:

.. code:: python

   ws.datasets.delete(f"{DATASET_NAME}")

******************
 Edit the Dataset
******************

SDK provides method :meth:`~graviti.manager.dataset.Dataset.edit` to support changing the name,
alias and default branch of the dataset.

.. code:: python

   dataset = ws.datasets.get(f"{DATASET_NAME}")
   dataset.edit(
       name=f"{NEW_DATASET_NAME}",
       alias=f"{NEW_ALIAS}",
       default_branch=f"{NEW_DEFAULT_BRANCH}"
   )

*****************
 Search the Data
*****************

SDK provides method :meth:`~graviti.manager.dataset.Dataset.search` to support searching the data
within a specified sheet:

.. code:: python

   dataset = ws.datasets.get(f"{DATASET_NAME}")
   criteria = {
       "opt": "or",
       "value": [
           {
               "opt": "eq",
               "key": "filename",
               "value": "0000f77c-6257be58.jpg"
           },
           {
               "opt": "and",
               "value": [
                   {
                       "opt": "eq",
                       "key": "attribute.weather",
                       "value": "clear"
                   },
                   {
                       "opt": "eq",
                       "key": "attribute.timeofday",
                       "value": "daytime"
                   }
               ]
           }
       ]
   }
   dataset.search("train", criteria)

.. note::
   The search criteria consist of three parts:

   * | opt: The operational relationship between the key and value, or the logical relationship
       between different search criteria.
   * key: The column name of the DataFrame. Multiple indexes can be connected with ``.``.
   * value: The value corresponding to the operator.

   .. list-table:: All supported operators and their meanings.
      :widths: auto
      :header-rows: 1

      * - opt
        - meaning
      * - and
        - Meet multiple search criteria.
      * - or
        - Meet at least one search criterion.
      * - eq
        - The value of the column is ``equal to`` the value of the criterion.
      * - ne
        - The value of the column is not ``equal to`` the value of the criterion.
      * - gt
        - The value of the column is ``greater than`` the value of the criterion.
      * - gte
        - The value of the column is ``greater than or equal to`` the value of the criterion.
      * - lt
        - The value of the column is ``less than`` the value of the criterion.
      * - lte
        - The value of the column is ``less than or equal to`` the value of the criterion.
      * - in
        - The value of the column ``belongs to`` the value of the criterion.
      * - nin
        - The value of the column ``doesn't belong to`` the value of the criterion.
