..
   Copyright 2022 Graviti. Licensed under MIT License.

########
 Action
########

Graviti SDK use class :class:`~graviti.manager.action.Action` to represent an action. The action
``name`` is used as the unique identifier of an action. And the data usage workflow is defined in
the action ``payload`` which follows the yaml syntax.

Action is a dataset level resource, it is necessary to get a dataset first:

.. code:: python

   from graviti import Workspace

   ws = Workspace(f"{YOUR_ACCESSKEY}")

   dataset = ws.datasets.get(f"{DATASET_NAME}")

******************
 Create an Action
******************

SDK provides method :meth:`~graviti.manager.action.ActionManager.create` to create an action:

.. code:: python

   with open(f"{PAYLOAD_YAML_FILE_PATH}") as fp:
       payload = fp.read()

   dataset.actions.create(f"{ACTION_NAME}", payload)

**************
 List Actions
**************

SDK provides method :meth:`~graviti.manager.action.ActionManager.list` to list actions:

.. code:: python

   dataset.actions.list()

***************
 Get an Action
***************

SDK provides method :meth:`~graviti.manager.action.ActionManager.get` to get an action:

.. code:: python

   dataset.actions.get(f"{ACTION_NAME}")

******************
 Update an Action
******************

SDK provides method :meth:`~graviti.manager.action.Action.edit` to update an action:

Rename the action:

.. code:: python

   action = dataset.actions.get(f"{ACTION_NAME}")
   action.edit(name=f"{NEW_ACTION_NAME}")

Update the action payload:

.. code:: python

   with open(f"{PAYLOAD_YAML_FILE_PATH}") as fp:
       new_payload = fp.read()

   action.edit(payload=f"{NEW_ACTION_NAME}")

.. note::

   Everytime the ``payload`` is updated, the :attr:`Action.edition
   <graviti.manager.action.Action.edition>` will be incremented by one.

*****************************
 Disable or Enable an Action
*****************************

SDK provides methods :meth:`Action.disable() <graviti.manager.action.Action.disable>` and
:meth:`Action.enable() <graviti.manager.action.Action.enable>` to disable and enable an action:

Once an action is disabled, it cannot be triggered automately and manually.

.. code:: python

   >>> action = dataset.actions.get(f"{ACTION_NAME}")
   >>> action.state
   'ENABLED'

   >>> action.disable()
   >>> action.state
   'DISABLED'

   >>> action.enable()
   >>> action.state
   'ENABLED'

******************
 Delete an Action
******************

SDK provides method :meth:`~graviti.manager.action.ActionManager.delete` to delete an action:

.. code:: python

   dataset.actions.delete(f"{ACTION_NAME}")

***************
 Run an Action
***************

"Run an Action" is equivalent to "Create an Action Run", check
:ref:`features/action_management/run:Create An Action Run` to run an action.
