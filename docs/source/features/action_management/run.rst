..
   Copyright 2022 Graviti. Licensed under MIT License.

#####
 Run
#####

Graviti SDK use class :class:`~graviti.manager.action.Run` to represent an action run. The run
``number`` is used as the unique identifier of an action.

Action run is a action level resource, it is necessary to get an action first:

.. code:: python

   from graviti import Workspace

   ws = Workspace(f"{YOUR_ACCESSKEY}")

   dataset = ws.datasets.get(f"{DATASET_NAME}")
   action = dataset.actions.get(f"{ACTION_NAME}")

**********************
 Create an Action Run
**********************

"Create an Action Run" is equivalent to "Run an Action".

SDK provides method :meth:`~graviti.manager.action.RunManager.create` to create an action run:

.. code:: python

   # Run an action with the default arguments:
   action.runs.create()

   # Run an action with given arguments:
   action.runs.create({f"{ARGUMENT_NAME}": f"{ARGUMENT_VALUE}"})

******************
 List Action Runs
******************

SDK provides method :meth:`~graviti.manager.action.RunManager.list` to list action runs:

.. code:: python

   action.runs.list()

*******************
 Get an Action Run
*******************

SDK provides method :meth:`~graviti.manager.action.RunManager.get` to get an action run:

.. code:: python

   action.runs.get(f"{RUN_NUMBER}")
