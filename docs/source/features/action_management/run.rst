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

**********************
 Cancel an Action Run
**********************

SDK provides method :meth:`Run.cancel() <graviti.manager.action.Run.cancel>` to cancel an action
run:

.. code:: python

   run = action.runs.get(f"{RUN_NUMBER}")
   run.cancel()

*****************
 Get the Run Log
*****************

Get the log of an action run is not supported directly in SDK so far. The log can be get from the
Graviti webpage which url can be found in the property :attr:`Run.url
<graviti.manager.action.Run.url>`.

.. code:: python

   run = action.runs.get(f"{RUN_NUMBER}")
   print(run.url)
