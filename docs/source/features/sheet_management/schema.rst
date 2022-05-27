..
 Copyright 2022 Graviti. Licensed under MIT License.

########
 Schema
########

Each sheet has a record type schema to describe the name and the type of each column.
Graviti use `Portex`_ schema language to define the schema, please refer to its documentation for the grammar.

.. _Portex: https://github.com/Project-OpenBytes/portex

Graviti SDK supports the python interaction with the Portex schema.


*****************
 Primitive Types
*****************

Graviti SDK provides classes to initialize Portex primitive types:

.. code:: python

   >>> import graviti.portex as pt
   >>> pt.boolean()
   boolean()
   >>> pt.binary()
   binary()
   >>> pt.string()
   string()
   >>> pt.int32()
   int32()
   >>> pt.int64()
   int64()
   >>> pt.float32()
   float32()
   >>> pt.float64()
   float64()

***************
 Complex Types
***************

Graviti SDK provides classes to initialize Portex complex types.

enum
====

An enum type can be created by giving the values:

.. code:: python

   >>> import graviti.portex as pt
   >>> enum = pt.enum(values=["a", "b", "c"])
   >>> enum.values
   ['a', 'b', 'c']

.. note::
   The values of the enum type must be strings with the format ``[A-Za-z_][A-Za-z0-9_]*``.

array
=====

The array type can be created by giving the item type.
Param ``length`` can be specified to fix the length of array.

.. code:: python

   >>> import graviti.portex as pt
   >>> array = pt.array(pt.int32())
   >>> array
   array(
     items=int32(),
   )
   >>> array = pt.array(pt.boolean(), length=2)
   >>> array.items
   boolean()
   >>> array.length
   2

tensor
======

The tensor type can be created by giving the item type and shape:

.. code:: python

   >>> import graviti.portex as pt
   >>> tensor = pt.tensor((2, 2), pt.int32())
   >>> tensor
   tensor(
     shape=(2, 2),
     items=int32(),
   )
   >>> tensor.items
   int32()
   >>> tensor.shape
   (2, 2)


record
======

The record type can be created by giving the fields, including names and types.
The record type is used to describe the name and type of each column of the tabular data,
and all the primitive and complex types mentioned above can be used here for each column type.

The names and types can be accessed by ``fields``,
which acts like a dict whose key is the column name and the value is column type.

.. code:: python

   >>> import graviti.portex as pt
   # Init record with list
   >>> record = pt.record(
   ...    [
   ...       ("x", pt.int32()),
   ...       ("y", pt.int32()),
   ...       ("categories", pt.enum(values=["cat", "dog"]))
   ...    ]
   ... )
   >>> record
   record(
     fields={
       'x': int32(),
       'y': int32(),
       'categories': enum(
         values=['cat', 'dog'],
       ),
     },
   )
   # Init record with dict
   >>> record = pt.record(
   ...    {
   ...       "x": pt.int32(),
   ...       "y": pt.int32(),
   ...       "categories": pt.enum(values=["cat", "dog"]),
   ...    }
   ... )
   >>> record
   record(
     fields={
       'x': int32(),
       'y': int32(),
       'categories': enum(
         values=['cat', 'dog'],
       ),
     },
   )
   >>> record.fields
   {
     'x': int32(),
     'y': int32(),
     'categories': enum(
       values=['cat', 'dog'],
     ),
   }


***************
 Template Type
***************

The template type can be created by giving the parameters and the declaration. And the type can be instantiated by giving the arguments.

.. code:: python

   >>> import graviti.portex as pt
   >>> vector_template = {
   ...     "type": "template",
   ...     "parameters": [
   ...         {
   ...             "name": "coords",
   ...             "default": {"type": "int32"},
   ...         },
   ...         {
   ...             "name": "labels",
   ...             "default": None,
   ...         },
   ...     ],
   ...     "declaration": {
   ...         "type": "record",
   ...         "fields": [
   ...             {
   ...                 "name": "x",
   ...                 "+": "$coords",
   ...             },
   ...             {
   ...                 "name": "y",
   ...                 "+": "$coords",
   ...             },
   ...             {
   ...                 "name": "label",
   ...                 "exist_if": "$labels",
   ...                 "type": "enum",
   ...                 "values": "$labels",
   ...             },
   ...         ],
   ...     },
   ... }
   >>> Vector = pt.template.template("Vector", vector_template)
   >>> Vector
   <class 'graviti.portex.template.Vector'>
   >>> vector = Vector(coords=pt.float32(), labels=["cat", "dog"])
   >>> vector
   Vector(
     coords=float32(),
     labels=['cat', 'dog'],
   )


**************
 Schema Files
**************

Graviti SDK provides :func:`~graviti.portex.base.read_yaml` and :func:`~graviti.portex.base.read_json` to read the Portex type from a yaml or a json file.
Take the `VOC2012Detection schema file`_ as an example.

.. _VOC2012Detection schema file: https://github.com/Project-OpenBytes/standard/blob/main/example/VOC2012Detection.yaml

.. code:: python

   >>> import graviti.portex as pt
   >>> schema = pt.read_yaml("standard/example/VOC2012Detection.yaml")
   Cloning repo 'https://github.com/Project-OpenBytes/standard@main'
   Cloned to '/tmp/portex/abe871c44b7983baa2d135a72529230a'
   >>> schema
   record(
     fields={
       'filename': string(),
       'image': file.RemoteFile(),
       'box2ds': array(
         items=label.Box2D(
           coords=int32(),
           categories=['aeroplane', 'bicycle', 'bird', 'boat', 'bottle', 'bus', 'car', 'cat', 'chair', 'cow', 'diningtable', 'dog', 'horse', 'motorbike', 'person', 'pottedplant', 'sheep', 'sofa', 'train', 'tvmonitor'],
           attributes={
             'difficult': boolean(),
             'occluded': boolean(),
             'truncated': boolean(),
             'pose': enum(
               values=['Frontal', 'Left', 'Rear', 'Right', 'Unspecified'],
             ),
           },
         ),
       ),
     },
   )

****************
 Schema Package
****************

Graviti SDK supports to use external packages defined under a repo. By giving the repo and revision, the package can be initialized and used locally.

SDK provides :func:`~graviti.portex.builder.build_package` to build an external Portex type package from the repo.
Take `standard`_ as an example, which is used as the standard external package by Graviti.

.. _standard: https://github.com/Project-OpenBytes/standard

.. code:: python

   >>> import graviti.portex as pt
   >>> standard = pt.build_package("https://github.com/Project-OpenBytes/standard", "main")
   Cloning repo 'https://github.com/Project-OpenBytes/standard@main'
   Cloned to '/tmp/portex/2a656e669aea0b88dca87784a3963215'
   >>> standard
   ExternalPackage {
     'calibration.Intrinsic': <class 'graviti.portex.builder.calibration.Intrinsic'>,
     'calibration.Extrinsic': <class 'graviti.portex.builder.calibration.Extrinsic'>,
     'geometry.Vector3D': <class 'graviti.portex.builder.geometry.Vector3D'>,
     'geometry.Quaternion': <class 'graviti.portex.builder.geometry.Quaternion'>,
     'geometry.Keypoint2D': <class 'graviti.portex.builder.geometry.Keypoint2D'>,
     'geometry.Vector2D': <class 'graviti.portex.builder.geometry.Vector2D'>,
     'geometry.PointList2D': <class 'graviti.portex.builder.geometry.PointList2D'>,
     'label.file.SemanticMask': <class 'graviti.portex.builder.label.file.SemanticMask'>,
     'label.file.InstanceMask': <class 'graviti.portex.builder.label.file.InstanceMask'>,
     'label.file.RemoteInstanceMask': <class 'graviti.portex.builder.label.file.RemoteInstanceMask'>,
     'label.file.PanopticMask': <class 'graviti.portex.builder.label.file.PanopticMask'>,
     'label.file.RemoteSemanticMask': <class 'graviti.portex.builder.label.file.RemoteSemanticMask'>,
     'label.tensor.SemanticMask': <class 'graviti.portex.builder.label.tensor.SemanticMask'>,
     'label.tensor.InstanceMask': <class 'graviti.portex.builder.label.tensor.InstanceMask'>,
     ... (25 items are folded),
     'tensor.Image': <class 'graviti.portex.builder.tensor.Image'>
   }
   >>> box2d = standard.label.Box2D(categories=["cat", "dog"])
   >>> box2d
   label.Box2D(
     coords=float32(),
     categories=['cat', 'dog'],
   )

.. note::
   Using branch as the revision to build the external package is unstable, since the latest commit may change.

   Tag name or commit ID as revision is recommended.


****************
 Schema Methods
****************

PyArrow
=======

Graviti SDK provides methods to convert PyArrow schema to Portex schema and convert Portex schema to PyArrow schema:

.. code:: python

   >>> import graviti.portex as pt
   >>> import pyarrow as pa
   >>> record = pt.record([("x", pt.int32()), ("y", pt.int32()), ("categories", pt.enum(values=["cat", "dog"]))])
   >>> record
   record(
     fields={
       'x': int32(),
       'y': int32(),
       'categories': enum(
         values=['cat', 'dog'],
       ),
     },
   )

   # convert Portex Type to PyArrow builtin types
   >>> pa_struct = record.to_pyarrow()
   >>> pa_struct
   StructType(struct<x: int32, y: int32, categories: dictionary<values=string, indices=int32, ordered=0>>)

   # convert record type to PyArrow schema
   >>> pa_schema = pa.schema(record.fields.to_pyarrow())
   >>> pa_schema
   x: int32
   y: int32
   categories: dictionary<values=string, indices=int32, ordered=0>

   # convert PyArrow type to Portex type
   >>> pa_type = pa.struct([pa.field("x", pa.float32()), pa.field("y", pa.float32())])
   >>> pa_type
   StructType(struct<x: float, y: float>)
   >>> portex_type = pt.PortexType.from_pyarrow(pa_type)
   >>> portex_type
   record(
     fields={
       'x': float32(),
       'y': float32(),
     },
   )


Expand
======

For better comprehension and operations, SDK provides methods to expand external Portex type to builtin types:

.. code:: python

   >>> import graviti.portex as pt
   >>> standard = pt.build_package("https://github.com/Project-OpenBytes/standard", "main")
   >>> box2d = standard.label.Box2D(categories=["cat", "dog"])
   # Expand the first layer of the external type
   >>> box2d.internal_type
   label._Label(
     geometry={
       'xmin': float32(),
       'ymin': float32(),
       'xmax': float32(),
       'ymax': float32(),
     },
     categories=['cat', 'dog'],
   )
   # Expand the top level of the external type to internal type
   >>> box2d.to_builtin()
   record(
     fields={
       'xmin': float32(),
       'ymin': float32(),
       'xmax': float32(),
       'ymax': float32(),
       'category': label.Category(
         categories=['cat', 'dog'],
       ),
     },
   )
