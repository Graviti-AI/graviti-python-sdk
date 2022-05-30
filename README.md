# Graviti Python SDK

[![Pre-commit](https://github.com/Graviti-AI/graviti-python-sdk/actions/workflows/pre-commit.yaml/badge.svg)](https://github.com/Graviti-AI/graviti-python-sdk/actions/workflows/pre-commit.yaml)
[![Documentation Status](https://readthedocs.org/projects/graviti-python-sdk/badge/?version=latest)](https://graviti-python-sdk.readthedocs.io/en/latest/?badge=latest)
[![GitHub](https://img.shields.io/github/license/Graviti-AI/graviti-python-sdk)](https://github.com/Graviti-AI/graviti-python-sdk/blob/main/LICENSE)
[![PyPI](https://img.shields.io/pypi/v/graviti)](https://pypi.org/project/graviti/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/graviti)](https://pypi.org/project/graviti/)
[![Downloads](https://pepy.tech/badge/graviti/month)](https://pepy.tech/project/graviti)

Graviti Python SDK is a python library to access [Graviti](https://www.graviti.com) workspace and
manage your datasets. It provides a pythonic way to access your datasets by Graviti OpenAPI.

## Installation

Graviti can be installed from PyPI:

```console
pip3 install graviti
```

Or from source:

```console
git clone https://github.com/Graviti-AI/graviti-python-sdk.git
cd graviti-python-sdk
pip install -e .
```

## Documentation

More information can be found on the [documentation site](https://graviti-python-sdk.readthedocs.io)

## Usage

Before using Graviti SDK, please finish the following registration steps:

-   Please visit [Graviti](https://www.graviti.com) to sign up.
-   Please visit [Graviti Developer Tools](https://gas.graviti.com/tensorbay/developer) to get an **AccessKey**.

### Get a Dataset

Workspace initialization:

```python
from graviti import Workspace
ws = Workspace(f"{YOUR_ACCESSKEY}")
```

List datasets on the workspace:

```python
>>> ws.datasets.list()
LazyPagingList [
  Dataset("czhual/Graviti-dataset-demo")(...)
]
```

Get one dataset:

```python
>>> dataset = ws.datasets.get("Graviti-dataset-demo")
>>> dataset
Dataset("czhual/Graviti-dataset-demo")(
  (alias): '',
  (default_branch): 'main',
  (created_at): '2022-05-26T02:55:36Z',
  (updated_at): '2022-05-26T02:57:55Z',
  (is_public): False,
  (config): 'AmazonS3-us-west-1'
)
```

### Switch Between Different Versions

View the current version of the dataset:

```python
>>> dataset.HEAD
Branch("main")(
  (commit_id): '47293b32f28c4008bc0f25b847b97d6f',
  (parent_commit_id): None,
  (title): 'Commit-1',
  (committer): 'czhual',
  (committed_at): '2022-05-26T02:57:00Z'
)
```

List history commits:

```python
>>> dataset.commits.list()
LazyPagingList [
  Commit("47293b32f28c4008bc0f25b847b97d6f")(...)
]
```

List all branches:

```python
>>> dataset.branches.list()
LazyPagingList [
  Branch("main")(...),
  Branch("dev")(...)
]
```

List all tags:

```python
>>> dataset.tags.list()
LazyPagingList [
  Tag("v1.0")(...)
]
```

Checkout commit/branch/tag:

```python
>>> dataset.checkout("47293b32f28c4008bc0f25b847b97d6f")  # commit id
>>> dataset.HEAD
Commit("47293b32f28c4008bc0f25b847b97d6f")(
  (parent_commit_id): None,
  (title): 'Commit-1',
  (committer): 'czhual',
  (committed_at): '2022-05-26T02:57:00Z'
)
```

```python
>>> dataset.checkout("dev")  # branch name
>>> dataset.HEAD
Branch("dev")(
  (commit_id): '47293b32f28c4008bc0f25b847b97d6f',
  (parent_commit_id): None,
  (title): 'Commit-1',
  (committer): 'czhual',
  (committed_at): '2022-05-26T02:57:00Z'
)
```

```python
>>> dataset.checkout("v1.0")  # tag name
>>> dataset.HEAD
Commit("47293b32f28c4008bc0f25b847b97d6f")(
  (parent_commit_id): None,
  (title): 'Commit-1',
  (committer): 'czhual',
  (committed_at): '2022-05-26T02:57:00Z'
)
```

### Get a Sheet

List all sheets:

```python
>>> list(dataset.keys())
['train']
```

Get a sheet:

```python
>>> dataset["train"]
   filename  box2ds
0  a.jpg     DataFrame(1, 6)
1  b.jpg     DataFrame(1, 6)
2  c.jpg     DataFrame(1, 6)
```

### Get the Data

Get the DataFrame:

```python
>>> df = dataset["train"]
>>> df
   filename  box2ds
0  a.jpg     DataFrame(1, 6)
1  b.jpg     DataFrame(1, 6)
2  c.jpg     DataFrame(1, 6)
```

View the schema of the sheet:

```python
>>> df.schema
record(
  fields={
    'filename': string(),
    'box2ds': array(
      items=label.Box2D(
        coords=float32(),
        categories=['boat', 'car'],
        attributes={
          'difficult': boolean(),
          'occluded': boolean(),
        },
      ),
    ),
  },
)
```

Get the data by rows or columns:

```python
>>> df.loc[0]
filename  a.jpg
box2ds    DataFrame(1, 6)
```

```python
>>> df["box2ds"]
0  DataFrame(1, 6)
1  DataFrame(1, 6)
2  DataFrame(1, 6)
```

```python
>>> df.loc[0]["box2ds"]
   xmin  ymin  xmax  ymax  category  attribute
                                     difficult  occluded
0  1.0   1.0   4.0   5.0   boat      False      False
```

```python
>>> df["box2ds"][0]
   xmin  ymin  xmax  ymax  category  attribute
                                     difficult  occluded
0  1.0   1.0   4.0   5.0   boat      False      False
```
