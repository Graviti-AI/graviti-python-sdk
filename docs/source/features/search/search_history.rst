..
   Copyright 2022 Graviti. Licensed under MIT License.

################
 Search History
################

Graviti Data Platform stores the search histories.

Graviti SDK use class :class:`~graviti.manager.search.SearchHistory` to represent a search history.
The search ID is used as the unique identifier of a search history. It can be accessed through SDK
attr :attr:`SearchHistory.search_id <graviti.manager.search.SearchHistory.search_id>` or copied from
Graviti website.

Search History is a dataset level resource, it is necessary to get a dataset first:

.. code:: python

   from graviti import Workspace

   ws = Workspace(f"{YOUR_ACCESSKEY}")

   dataset = ws.datasets.get(f"{DATASET_NAME}")

***********************
 List Search Histories
***********************

SDK provides method :meth:`~graviti.manager.search.SearchManager.list` to list search histories:

.. code:: python

   dataset.searches.list()

**********************
 Get a Search History
**********************

SDK provides method :meth:`~graviti.manager.search.SearchManager.get` to get a search history:

.. code:: python

   dataset.searches.get(f"{SEARCH_ID}")

*************************
 Delete a Search History
*************************

SDK provides method :meth:`~graviti.manager.search.SearchManager.delete` to delete a search history:

.. code:: python

   dataset.searches.delete(f"{SEARCH_ID}")

**************
 Run a Search
**************

SDK provides method :meth:`~SearchHistory.run <graviti.manager.search.SearchHistory.run>` to run and
get the search result.

.. code:: python

   search_history = dataset.searches.get(f"{SEARCH_ID}")

   search_result_df = search_history.run()
