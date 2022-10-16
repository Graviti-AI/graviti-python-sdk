..
   Copyright 2022 Graviti. Licensed under MIT License.

####################
 Pandas Integration
####################

Graviti SDK provides converting methods to pandas DataFrame and Series.

*******************
 Graviti to Pandas
*******************

Graviti SDK provides :meth:`DataFrame.to_pandas() <graviti.dataframe.frame.DataFrame.to_pandas>` and
:meth:`Series.to_pandas() <graviti.dataframe.column.series.Series.to_pandas>` methods to convert
graviti :class:`~graviti.dataframe.frame.DataFrame` and
:class:`~graviti.dataframe.column.series.Series` to pandas.

.. code:: python

   >>> from graviti import DataFrame

   >>> df = DataFrame(
   ...     [
   ...         {
   ...             "A": i,
   ...             "B": f"data{i}",
   ...             "C": bool(i % 2),
   ...         }
   ...         for i in range(10)
   ...     ]
   ... )
   >>> pandas_df = df.to_pandas()
   >>> pandas_df
      A      B      C
   0  0  data0  False
   1  1  data1   True
   2  2  data2  False
   3  3  data3   True
   4  4  data4  False
   5  5  data5   True
   6  6  data6  False
   7  7  data7   True
   8  8  data8  False
   9  9  data9   True

   >>> type(pandas_df)
   pandas.core.frame.DataFrame

.. code:: python

   >>> from graviti import Series

   >>> series = Series(range(10))
   >>> pandas_series = series.to_pandas()
   >>> pandas_series
   0    0
   1    1
   2    2
   3    3
   4    4
   5    5
   6    6
   7    7
   8    8
   9    9
   dtype: int64
   >>> type(pandas_series)
   pandas.core.series.Series

*******************
 Pandas to Graviti
*******************

Graviti SDK provides :meth:`DataFrame.from_pandas() <graviti.dataframe.frame.DataFrame.from_pandas>`
and :meth:`Series.from_pandas() <graviti.dataframe.column.series.Series.from_pandas>` methods to
convert pandas ``DataFrame`` and ``Series`` to graviti.

.. code:: python

   >>> import pandas as pd
   >>> from graviti import DataFrame

   >>> pandas_df = pd.DataFrame(
   ...     [
   ...         {
   ...             "A": i,
   ...             "B": f"data{i}",
   ...             "C": bool(i % 2),
   ...         }
   ...         for i in range(10)
   ...     ]
   ... )
   >>> df = DataFrame.from_pandas(pandas_df)
   >>> df
      A      B      C
   0  0  data0  False
   1  1  data1   True
   2  2  data2  False
   3  3  data3   True
   4  4  data4  False
   5  5  data5   True
   6  6  data6  False
   7  7  data7   True
   8  8  data8  False
   9  9  data9   True

   >>> type(df)
   graviti.dataframe.frame.DataFrame

.. code:: python

   >>> import pandas as pd
   >>> from graviti import Series

   >>> pandas_series = pd.Series(range(10))
   >>> series = Series.from_pandas(pandas_series)
   >>> series
   0  0
   1  1
   2  2
   3  3
   4  4
   5  5
   6  6
   7  7
   8  8
   9  9
   >>> type(series)
   graviti.dataframe.column.series.NumberSeries
