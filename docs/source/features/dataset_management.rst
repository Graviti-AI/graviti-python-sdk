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
a dataset based on the given name:

.. code:: python

   ws.datasets.create(f"{DATASET_NAME}")

In addition to name, alias and storage config can also be specified:

.. code:: python

   ws.datasets.create(f"{DATASET_NAME}", f"{DATASET_ALIAS}", f"{STORAGE_CONFIG}")

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
a dataset by name:

.. code:: python

   ws.datasets.get(f"{DATASET_NAME}")

******************
 Delete a Dataset
******************

SDK provides method :meth:`~graviti.manager.dataset.DatasetManager.delete` to support deleting
a dataset by name:

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
