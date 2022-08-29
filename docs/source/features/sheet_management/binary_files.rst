..
 Copyright 2022 Graviti. Licensed under MIT License.

##############
 Binary Files
##############

Graviti SDK use the :class:`~graviti.file.base.File` and :class:`~graviti.file.base.RemoteFile`
to represent a specific file.

In addition, SDK also provides several commonly used file formats, including :class:`~graviti.file.image.Image`,
:class:`~graviti.file.audio.Audio` and :class:`~graviti.file.point_cloud.PointCloud`.

******
 File
******

SDK supports all various binary files including video files and text files by :class:`~graviti.file.base.File`.

Load the local text files to DataFrame:

.. code:: python

   import graviti.portex as pt
   from graviti import DataFrame
   from graviti.file import File

   std = pt.build_package("https://github.com/Project-OpenBytes/portex-standard", "main")
   schema = pt.record(
       {
           "filename": pt.string(),
           "file": std.file.File(),
       }
   )

   data = [
       {
           "filename": "EXAMPLE1.text",
           "file": File("PATH/TO/YOUR/EXAMPLE1.text")
       },
       {
           "filename": "EXAMPLE2.text",
           "file": File("PATH/TO/YOUR/EXAMPLE2.text")
       },
   ]

   df = DataFrame(data, schema)

Read the text in DataFrame:

.. code:: python

   text = df["text"][0]
   with text.open() as fp:
       fp.read().decode("utf-8")

For all binary files, SDK supports viewing their basic information, including extension,
size and checksum:

.. code:: python

   text.extension
   text.size
   text.get_checksum()

*******
 Image
*******

Load the local image into DataFrame:

.. code:: python

   import graviti.portex as pt
   from graviti import DataFrame
   from graviti.file import Image

   std = pt.build_package("https://github.com/Project-OpenBytes/portex-standard", "main")
   schema = pt.record(
       {
           "filename": pt.string(),
           "image": std.file.Image(),
       }
   )

   data = [
       {
           "filename": "EXAMPLE1.png",
           "image": File("PATH/TO/YOUR/EXAMPLE1.png")
       },
       {
           "filename": "EXAMPLE2.png",
           "image": File("PATH/TO/YOUR/EXAMPLE2.png")
       },
   ]

   df = DataFrame(data, schema)

Read the image in DataFrame:

.. code:: python

   import PIL

   image = df["image"][0]
   with image.open() as fp:
       PIL.Image.open(fp)

For image files, SDK supports viewing their height and width:

.. code:: python

   image.height
   image.width

*******
 Audio
*******

Load the local audio into DataFrame:

.. code:: python

   import graviti.portex as pt
   from graviti import DataFrame
   from graviti.file import Audio

   std = pt.build_package("https://github.com/Project-OpenBytes/portex-standard", "main")
   schema = pt.record(
       {
           "filename": pt.string(),
           "audio": std.file.Image(),
       }
   )

   data = [
       {
           "filename": "EXAMPLE1.mp3",
           "audio": File("PATH/TO/YOUR/EXAMPLE1.mp3")
       },
       {
           "filename": "EXAMPLE2.mp3",
           "audio": File("PATH/TO/YOUR/EXAMPLE2.mp3")
       },
   ]

   df = DataFrame(data, schema)

Read the audio in DataFrame:

.. code:: python

   audio = df["audio"][0]
   with audio.open() as fp:
       fp.read()

*************
 Point Cloud
*************

Load the local point_cloud into DataFrame:

.. code:: python

   import graviti.portex as pt
   from graviti import DataFrame
   from graviti.file import PointCloud

   std = pt.build_package("https://github.com/Project-OpenBytes/portex-standard", "main")
   schema = pt.record(
       {
           "filename": pt.string(),
           "point_cloud": std.file.Image(),
       }
   )

   data = [
       {
           "filename": "EXAMPLE1",
           "point_cloud": File("PATH/TO/YOUR/EXAMPLE1")
       },
       {
           "filename": "EXAMPLE2",
           "point_cloud": File("PATH/TO/YOUR/EXAMPLE2")
       },
   ]

   df = DataFrame(data, schema)

Read the point_cloud in DataFrame:

.. code:: python

   point_cloud = df["point_cloud"][0]
   with point_cloud.open() as fp:
       fp.read()
