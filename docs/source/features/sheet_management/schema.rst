..
 Copyright 2022 Graviti. Licensed under MIT License.

########
 Schema
########

Each sheet has a record type schema to describe the name and the type of each column.
Graviti use `Portex`_ schema language to define the schema, please refer to its documentation for the syntax.

.. _Portex: https://portex.readthedocs.io/en/latest/?badge=latest

Graviti SDK supports the python interaction with the Portex schema.


*****************
 Primitive Types
*****************

Graviti SDK provides classes to initialize Portex primitive types:

.. tabs::

   .. tab:: boolean

      .. code:: python

         >>> import graviti.portex as pt
         >>> pt.boolean()
         boolean()

   .. tab:: binary

      .. code:: python

         >>> import graviti.portex as pt
         >>> pt.binary()
         binary()

   .. tab:: string

      .. code:: python

         >>> import graviti.portex as pt
         >>> pt.string()
         string()

   .. tab:: int32

      .. code:: python

         >>> import graviti.portex as pt
         >>> pt.int32()
         int32()

   .. tab:: int64

      .. code:: python

         >>> import graviti.portex as pt
         >>> pt.int64()
         int64()

   .. tab:: float32

      .. code:: python

         >>> import graviti.portex as pt
         >>> pt.float32()
         float32()

   .. tab:: float64

      .. code:: python

         >>> import graviti.portex as pt
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


record
======

The record type can be created by giving the fields, including names and types.
The record type is used to describe the name and type of each column of the tabular data,
and all the primitive and complex types mentioned above can be used here for each column type.

The names and types can be accessed by ``fields``,
which acts like a dict whose key is the column name and the value is column type.


.. tabs::

   .. tab:: Init record with list

      .. code:: python
      
         >>> import graviti.portex as pt
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
         >>> record.fields
         {
           'x': int32(),
           'y': int32(),
           'categories': enum(
             values=['cat', 'dog'],
           ),
         }


   .. tab:: Init record with dict

      .. code:: python

         >>> import graviti.portex as pt
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

.. tabs::

   .. tab:: YAML File

      Take the following ``schema.yaml`` file as an example:

      .. code:: yaml

         ---
         type: record
         fields:
           - name: filename
             type: string

           - name: category
             type: int32

           - name: attribute
             type: record
             fields:
               - name: weather
                 type: enum
                 values: ["sunny", "rainy", "windy"]

               - name: distorted
                 type: boolean

      .. code:: python

         >>> import graviti.portex as pt
         >>> schema = pt.read_yaml("schema.yaml")
         >>> schema
         record(
           fields={
             'filename': string(),
             'category': int32(),
             'attribute': record(
               fields={
                 'weather': enum(
                   values=['sunny', 'rainy', 'windy'],
                 ),
                 'distorted': boolean(),
               },
             ),
           },
         )

   .. tab:: JSON File

      Take the following ``schema.json`` file as an example:

      .. code:: yaml

         {
             "type": "record",
             "fields": [
                 {
                     "name": "filename",
                     "type": "string"
                 },
                 {
                     "name": "category",
                     "type": "int32"
                 },
                 {
                     "name": "attribute",
                     "type": "record",
                     "fields": [
                         {
                             "name": "weather",
                             "type": "enum",
                             "values": [
                                 "sunny",
                                 "rainy",
                                 "windy"
                             ]
                         },
                         {
                             "name": "distorted",
                             "type": "boolean"
                         }
                     ]
                 }
             ]
         }

      .. code:: python

         >>> import graviti.portex as pt
         >>> schema = pt.read_json("schema.json")
         >>> schema
         record(
           fields={
             'filename': string(),
             'category': int32(),
             'attribute': record(
               fields={
                 'weather': enum(
                   values=['sunny', 'rainy', 'windy'],
                 ),
                 'distorted': boolean(),
               },
             ),
           },
         )

****************
 Schema Package
****************

Graviti SDK supports to use external packages defined under a repo. By giving the repo and revision, the package can be initialized and used locally.

SDK provides :func:`~graviti.portex.builder.build_package` to build an external Portex type package from the repo.
Take `standard`_ as an example, which is used as the standard external package by Graviti.

.. _standard: https://github.com/Project-OpenBytes/portex-standard

.. code:: python

   >>> import graviti.portex as pt
   >>> std = pt.build_package("https://github.com/Project-OpenBytes/portex-standard", "main")
   Cloning repo 'https://github.com/Project-OpenBytes/portex-standard@main'
   Cloned to '/tmp/portex/2a656e669aea0b88dca87784a3963215'
   >>> std
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
   >>> box2d = std.label.Box2D(categories=["cat", "dog"])
   >>> box2d
   label.Box2D(
     coords=float32(),
     categories=['cat', 'dog'],
   )

.. note::
   Using branch as the revision to build the external package is unstable, since the latest commit may change.

   Tag name or commit ID as revision is recommended.

**************
 Binary Files
**************

SDK supports adding and uploading binary files, whose schema must be of type `file.RemoteFile` in `standard`_ package.


.. code:: python

   >>> import graviti.portex as pt
   >>> std = pt.build_package("https://github.com/Project-OpenBytes/portex-standard", "main")
   Cloning repo 'https://github.com/Project-OpenBytes/portex-standard@main'
   Cloned to '/tmp/portex/2a656e669aea0b88dca87784a3963215'
   >>> record = pt.record(
   ...    {
   ...       "filename": pt.string(),
   ...       "image": std.file.RemoteFile,
   ...    }
   ... )

When using the record in the above example as the schema of a DataFrame,
the column of "image" stores binary files.
Please see :ref:`features/sheet_management/dataframe:File Operation` for more details about data.

****************
 Schema Methods
****************

Convert
=======

PortexType provides methods to convert to or init from python object, json string and yaml string.
Take the following schema as an example:

.. code:: python

   >>> import graviti.portex as pt
   >>> schema = pt.record(
   ...    {
   ...       "x": pt.int32(),
   ...       "y": pt.int32(),
   ...       "categories": pt.enum(values=["cat", "dog"]),
   ...    }
   ... )
   >>> schema
   record(
     fields={
       'x': int32(),
       'y': int32(),
       'categories': enum(
         values=['cat', 'dog'],
       ),
     },
   )

.. tabs::

   .. tab:: Python Object

      .. code:: python

         >>> pyobj = schema.to_pyobj()
         >>> pyobj
         {'type': 'record',
          'fields': [{'name': 'x', 'type': 'int32'},
           {'name': 'y', 'type': 'int32'},
           {'name': 'categories', 'type': 'enum', 'values': ['cat', 'dog']}]}
      
         >>> pt.PortexType.from_pyobj(pyobj)
         record(
           fields={
             'x': int32(),
             'y': int32(),
             'categories': enum(
               values=['cat', 'dog'],
             ),
           },
         )

   .. tab:: JSON String

      .. code:: python

          >>> json_string = schema.to_json()
          >>> json_string
          '{"type": "record", "fields": [{"name": "x", "type": "int32"}, {"name": "y", "type": "int32"}, {"name": "categories", "type": "enum", "values": ["cat", "dog"]}]}'

          >>> pt.PortexType.from_json(json_string)
          record(
            fields={
              'x': int32(),
              'y': int32(),
              'categories': enum(
                values=['cat', 'dog'],
              ),
            },
          )

   .. tab:: YAML String

      .. code:: python

          >>> yaml_string = schema.to_yaml()
          >>> yaml_string
          'type: record\nfields:\n- name: x\n  type: int32\n- name: y\n  type: int32\n- name: categories\n  type: enum\n  values:\n  - cat\n  - dog\n'

          >>> pt.PortexType.from_yaml(yaml_string)
          record(
            fields={
              'x': int32(),
              'y': int32(),
              'categories': enum(
                values=['cat', 'dog'],
              ),
            },
          )

Expand
======

For better comprehension and operations, SDK provides methods to expand external Portex type to builtin types:

.. code:: python

   >>> import graviti.portex as pt
   >>> std = pt.build_package("https://github.com/Project-OpenBytes/portex-standard", "main")
   >>> box2d = std.label.Box2D(categories=["cat", "dog"])
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
