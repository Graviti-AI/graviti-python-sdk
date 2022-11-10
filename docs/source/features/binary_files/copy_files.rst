..
   Copyright 2022 Graviti. Licensed under MIT License.

############
 Copy Files
############

Graviti Data Platform supports copy binary files across different datasets.

Graviti stores binary files in the Object Storage Services, not in the Graviti database. The
database only stores the access info of the binary files. The binary file copy operation only copies
the access info, the real binary files will not be copied. Which means copy will not create
additional storage space of Object Storage Services.

.. important::

   The binary files can only be copied across the datasets in the same workspace with the same
   storage config

.. note::

   The copy can be understood as the linux ``ln`` operation.
