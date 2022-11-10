..
   Copyright 2022 Graviti. Licensed under MIT License.

################
 Storage Config
################

The binary files in Graviti Data Platform are stored in Object Storage Services, such as
``S3(AWS)``, ``OSS(Aliyun)`` and ``AZURE(Mircosoft)``.

Graviti uses "Storage Config" to store the information of the Object Storage Services. Graviti not
only provides default storage configs (``"GRAVITI"`` config), but also supports adding storage
configs which belong to the customers (``"AUTHORIZED"`` config). And the different storage configs
can be used to create different datasets.

Storage Config is a workspace level resource, it is necessary to get a workspace first:

.. code:: python

   from graviti import Workspace

   ws = Workspace(f"{YOUR_ACCESSKEY}")

**********************
 List Storage Configs
**********************

SDK provides method :meth:`~graviti.manager.storage_config.StorageConfigManager.list` to list
storage configs:

.. code:: python

   ws.storage_configs.list()

**********************
 Get a Storage Config
**********************

SDK provides method :meth:`~graviti.manager.storage_config.StorageConfigManager.get` to get a
storage config:

.. code:: python

   ws.storage_configs.get(f"{STORAGE_CONFIG_NAME}")

************************
 Default Storage Config
************************

A workspace has a default storage config, the default storage config will be used to create datasets
which the storage config is not provided by the creator.

SDK provides property
:attr:`~graviti.manager.storage_config.StorageConfigManager.default_storage_config` to get the
default storage config.

.. code:: python

   ws.storage_configs.default_storage_config

SDK provides method :meth:`~graviti.manager.storage_config.StorageConfigManager.edit` to set the
default storage config.

.. code:: python

   ws.storage_configs.edit(default_storage_config=f"{STORAGE_CONFIG_NAME}")

********************************************
 Create Dataset with Specific StorageConfig
********************************************

Method :meth:`DatasetManager.create() <graviti.manager.dataset.DatasetManager.create>` provides
``storage_config`` parameter to create dataset with specific storage config.

.. code:: python

   # Create a dataset with specific storage config
   dataset = ws.datasets.create(f"{DATASET_NAME}", storage_config=f"{STORAGE_CONFIG_NAME}")

   # The default storage config will be used if the "storage_config" is not provided
   dataset = ws.datasets.create(f"{DATASET_NAME}")

And the attr :attr:`Dataset.storage_config <graviti.manager.dataset.Dataset.storage_config>` is
provided to get the ``storage_config`` of the dataset.

.. code:: python

   storage_config = dataset.storage_config

*********************************
 Create and Delete StorageConfig
*********************************

Create and delete storage config is not provided in Graviti SDK so far. Please visit the Website
`Graviti Storage Config`_ for creation and deletion.

.. _graviti storage config: https://gas.graviti.com/tensorbay/data-storage-list
