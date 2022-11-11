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

**************
 Example Code
**************

This example downsample the source dataset with binary files, and copy the downsampled DataFrame to
a new dataset.

Get the DataFrame from the source dataset:

.. code:: python

   from graviti import Workspace

   # initialize the Workspace
   ws = Workplace(f"{ACCESS_KEY}")

   # get the source dataset
   src_ds = ws.datasets.get("source_dataset")

   # get the "data" sheet from the source dataset
   src_df = src_ds["data"]

The "data" sheet in the source dataset contains binary files:

.. code:: python

   >>> src_df.schema
   record(
       fields={
           'filename': string(),
           'file': file.File(),
      },
   )

   >>> src_df
      filename  file
   0  0000.txt  RemoteFile("9cf96ce")
   1  0001.txt  RemoteFile("d31c5f0")
   2  0002.txt  RemoteFile("5f83d98")
   3  0003.txt  RemoteFile("272c9a9")
   4  0004.txt  RemoteFile("d25c42d")
   5  0005.txt  RemoteFile("b6e904a")
   6  0006.txt  RemoteFile("019fad7")
   7  0007.txt  RemoteFile("7100110")
   8  0008.txt  RemoteFile("945b3a8")
   9  0009.txt  RemoteFile("59a0f9a")

Downsample the source dataframe and add it into the target dataset:

.. code:: python

   >>> # create the target dataset
   >>> dst_ds = ws.datasets.create("target_dataset")

   >>> # use the dataframe slice feature to downsample the source dataframe
   >>> dst_df = src_df.iloc[::2]
   >>> dst_df
      filename  file
   0  0000.txt  RemoteFile("9cf96ce")
   1  0002.txt  RemoteFile("5f83d98")
   2  0004.txt  RemoteFile("d25c42d")
   3  0006.txt  RemoteFile("019fad7")
   4  0008.txt  RemoteFile("945b3a8")

   >>> # add the downsampled dataframe to the target dataset
   >>> dst_ds["downsampled"] = dst_df

Commit:

.. code:: python

   >>> dst_ds.commit("copy files from source_dataset")
   Draft("#1: copy files from source_dataset") created successfully
   uploading structured data: 100%|██████████████████████████| 5/5 [00:03<00:00,  1.38it/s]
   uploading binary files: 100%|██████████████████████████| 5/5 [00:03<00:00,  1.38it/s]
   Draft("#1: copy files from source_dataset") uploaded successfully
   Draft("#1: copy files from source_dataset") committed successfully
   The HEAD of the dataset after commit:
   Branch("main")(
     (commit_id): '913b44d7aebe43a18265c27a20d2decf',
     (parent): None,
     (title): 'copy files from source_dataset',
     (committer): 'linjiX',
     (committed_at): 2022-11-11 18:52:19+08:00
   )

After commit, the downsampled dataframe with binary files is copyied to the target dataset:

.. code:: python

   >>> # read the data from the target dataset
   >>> dst_ds["downsampled"]
      filename  file
   0  0000.txt  RemoteFile("9cf96ce")
   1  0002.txt  RemoteFile("5f83d98")
   2  0004.txt  RemoteFile("d25c42d")
   3  0006.txt  RemoteFile("019fad7")
   4  0008.txt  RemoteFile("945b3a8")
