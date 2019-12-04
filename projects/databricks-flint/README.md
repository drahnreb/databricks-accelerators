# Flint: A Time Series Library for Apache Spark

This repository contains a Databricks specific version of Two Sigmas's Flint Library along with a sample notebook for use.

Two Sigma's Flint repo can be found at https://github.com/twosigma/flint.

API documentation is available at http://ts-flint.readthedocs.io/en/latest/.

## To use

Create a Python 3 cluster in Databricks and attach the JAR.  The JAR can be downloaded locally and uploaded via the [Library UI](https://docs.databricks.com/user-guide/libraries.html#upload-a-java-jar-or-scala-jar)


# flint for Azure Databricks
This is a fork with adadpations to work under scala 2.11 and spark 2.4 (Databricks Runtime 5.3 and above).

### Documentation of usage in Databricks:
https://databricks.com/blog/2018/09/11/introducing-flint-a-time-series-library-for-apache-spark.html

### Official ts-flint Documentation:
https://ts-flint.readthedocs.io/en/latest/

### Version officially built by Databricks (does **not** work):
https://github.com/databricks/databricks-accelerators/tree/master/projects/databricks-flint

## Requirements
| Dependency         | Version           | Tested  |
| ------------------ | ----------------- | ------- |
| Spark              |  2.4              | 2.4.4   |
| Scala              |  2.11             | 2.11.12 |
| Python             |  3.5 and above    | 3.8     |
| pyarrow            |  0.12.0 or above  | 0.13.0  |
| Databricks Runtime | 5.3 or above      | 6.1 ML  |

## Installation:
1. Download build .jar library `flint-0.6.1_spark2.4.4_scala2.11.12.jar` from [/lib](https://github.com/drahnreb/databricks-accelerators/blob/master/projects/databricks-flint/lib/flint-0.6.1_spark2.4.4_scala2.11.12.jar).
2. Install library in your desired (supported) Databricks Cluster via [Library UI](https://docs.databricks.com/libraries.html#upload-a-java-jar-or-scala-jar)

## Build your own version:
0. Requirements: [install sbt](https://www.scala-sbt.org/release/docs/Installing-sbt-on-Windows.html)
1. git clone official ts-flint repository with latest commit [40fd887](https://github.com/twosigma/flint/commit/40fd887bebbf5a5d42365b914a7c99a665ccf8c7).
2. cd into cloned repo top-level-dir and check specified versions in build.sbt
e.g. changes made:
line 34:
`  scalaVersion := "2.11.12",`
line 50 (windows specific):
`    "Local Maven Repository" at "file:///" + Path.userHome.absolutePath + "/.m2/repository",`
line 61:
`  val spark = "2.4.4"`
line 65:
`  val arrow = "0.12.0"`

3. add env variable (based on [#71](https://github.com/twosigma/flint/issues/71):
```bash
export TERM=xterm-color
```
4. build from source (scala):
```bash
sbt assemblyNoTest
```

## Getting started:
### Starting Point: `TimeSeriesRDD` and `TimeSeriesDataFrame`
The entry point into all functionalities for time series analysis in Flint is `TimeSeriesRDD` (for Scala) and `TimeSeriesDataFrame` (for Python). In high level, a `TimeSeriesRDD` contains an `OrderedRDD` which could be used to represent a sequence of ordering key-value pairs. A `TimeSeriesRDD` uses `Long` to represent timestamps in nanoseconds since epoch as keys and `InternalRow`s as values for `OrderedRDD` to represent a time series data set.

### Create `TimeSeriesRDD`
Applications can create a `TimeSeriesRDD` from an existing `RDD`, from an `OrderedRDD`, from a `DataFrame`, or from a single csv file.
As an example, the following creates a `TimeSeriesRDD` from a pandas DataFrame.
To create a `TimeSeriesRDD` from a `DataFrame`, you have to make sure the `DataFrame` contains a column named "time" of type `LongType`.

```python
from datetime import datetime
import numpy as np

import ts.flint
from ts.flint import FlintContext
flintContext = FlintContext(sqlContext)

values = {'time': [1543672800000, 1543672800100, 1543672800260],
         'F_1': [22, 38, 26],
         'F.2': [7.3, 71.3, 7.9]}

states = {'time': [1543672800100, 1543672800200, 1543672800300],
         'types': ["0", "24", "42"],
         'state': ["False", "True", "True"]}

stops = {'time': [1543672800150, 1543672800200, 1543672800300, 43672800360]}

states_pd = pd.DataFrame(states, columns=states.keys())
values_pd = pd.DataFrame(values, columns=values.keys())
stops_pd = pd.DataFrame(stops, columns=stops.keys())
states_pd['time'] = pd.to_datetime(states_pd['time'], unit='ms', origin='unix')
values_pd['time'] = pd.to_datetime(values_pd['time'], unit='ms', origin='unix')
stops_pd['time'] = pd.to_datetime(stops_pd['time'], unit='ms', origin='unix')

states_df = spark.createDataFrame(states_pd)
values_df = spark.createDataFrame(values_pd)
stops_df = spark.createDataFrame(stops_pd)

# Convert to Flint DataFrame
flint_df1, flint_states, flint_stops = [flintContext.read \
              .option("isSorted", False) \
              .option("timeColumn", 'time') \
              .option("timeUnit", 'ms') \
              .dataframe(
                # https://github.com/twosigma/flint:
                # 'To create a TimeSeriesRDD from a DataFrame, you have to make sure the DataFrame contains a column named "time" of type LongType'
                df.withColumn("time", (df.time.cast('double')*1000).cast("long"))
              ) for df in [values_df, states_df, stops_df]]
```

```python
states_df.show(truncate=False)
```
```console
+---------------------+-----+-----+
|time                 |types|state|
+---------------------+-----+-----+
|2018-12-01 14:00:00.1|0    |False|
|2018-12-01 14:00:00.2|24   |True |
|2018-12-01 14:00:00.3|42   |True |
+---------------------+-----+-----+
```
```python
values_df.show(truncate=False)
```
```console
+----------------------+---+----+
|time                  |F_1|F.2 |
+----------------------+---+----+
|2018-12-01 14:00:00   |22 |7.3 |
|2018-12-01 14:00:00.1 |38 |71.3|
|2018-12-01 14:00:00.26|26 |7.9 |
+----------------------+---+----+
```

```console
stops_df.show(truncate=False)
```
```console
+----------------------+
|time                  |
+----------------------+
|2018-12-01 14:00:00.15|
|2018-12-01 14:00:00.2 |
|2018-12-01 14:00:00.3 |
|1971-05-21 11:20:00.36|
+----------------------+
```

It is also possible to create a `TimeSeriesRDD` from a dataset stored as parquet format file(s). The `TimeSeriesRDD.fromParquet()` function provides the option to specify which columns and/or the time range you are interested, e.g.


### Temporal Join Functions

A temporal join function is a join function defined by a matching criteria over time. A `tolerance` in temporal join matching criteria specifies how much it should look past or look futue.

- `leftJoin` A function performs the temporal left-join to the right `TimeSeriesRDD`, i.e. left-join using inexact timestamp matches.  For each row in the left, append the most recent row from the right at or before the same time. An example to join two `TimeSeriesRDD`s is as follows.

- `futureLeftJoin` A function performs the temporal future left-join to the right `TimeSeriesRDD`, i.e. left-join using inexact timestamp matches. For each row in the left, appends the closest future row from the right at or after the same time.

```python
### combine data
tolerance = '100ms' # exact: 0 or e.g. '100ms'
data_joined = flint_df1.futureLeftJoin(flint_states, tolerance=tolerance)
```

```python
data_joined.show(truncate=False)
```
```console
+----------------------+---+----+-----+-----+
|time                  |F_1|F.2 |types|state|
+----------------------+---+----+-----+-----+
|2018-12-01 14:00:00   |22 |7.3 |0    |False|
|2018-12-01 14:00:00.1 |38 |71.3|0    |False|
|2018-12-01 14:00:00.26|26 |7.9 |42   |True |
+----------------------+---+----+-----+-----+
```

## Group functions

A group function is to group rows with nearby (or exactly the same) timestamps.

- `groupByCycle` A function to group rows within a cycle, i.e. rows with exactly the same timestamps.
- `addWindows` For each row, this function adds a new column whose value for a row is a list of rows within its `window`.
- `groupByInterval` A function to group rows whose timestamps fall into an interval. Intervals could be defined by another `TimeSeriesRDD`. Its timestamps will be used to defined intervals, i.e. two sequential timestamps define an interval. For example,

```python
### cut into bins
data_binned = data_joined.groupByInterval(flint_stops.select('time'),
                                                 inclusion='begin') # automatically uses col 'time'
```

```python
data_binned.show(truncate=False)
```
```console
+----------------------+---------------------------------------------------------------------------------------+
|time                  |rows                                                                                   |
+----------------------+---------------------------------------------------------------------------------------+
|2018-12-01 14:00:00.15|[[2018-12-01 14:00:00, 22, 7.3, 0, False], [2018-12-01 14:00:00.1, 38, 71.3, 0, False]]|
|2018-12-01 14:00:00.3 |[[2018-12-01 14:00:00.26, 26, 7.9, 42, True]]                                          |
+----------------------+---------------------------------------------------------------------------------------+

```
