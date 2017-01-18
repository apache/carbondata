<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Quick Start
This tutorial provides a quick introduction to using CarbonData.

## Getting started with Apache CarbonData

* [Installation](#installation)
* [Prerequisites](#prerequisites)
* [Interactive Analysis with Spark Shell Version 2.1](#interactive-analysis-with-spark-shell)
  - Basics
  - Executing Queries
      * Creating a Table
      * Loading Data to a Table
      * Query Data from a Table
* Interactive Analysis with Spark Shell Version 1.6
   - Basics
   - Executing Queries
     * Creating a Table
     * Loading Data to a Table
     * Query Data from a Table
* [Building CarbonData](#building-carbondata)


##  Installation
* Download a released package of [Spark 1.6.2 or 2.1.0](http://spark.apache.org/downloads.html).
* Download and install [Apache Thrift 0.9.3](http://thrift-tutorial.readthedocs.io/en/latest/installation.html), make sure Thrift is added to system path.
* Download [Apache CarbonData code](https://github.com/apache/incubator-carbondata) and build it. Please visit [Building CarbonData And IDE Configuration](https://github.com/apache/incubator-carbondata/blob/master/build/README.md) for more information.

##  Prerequisites

* Create a sample.csv file using the following commands. The CSV file is required for loading data into CarbonData.

```
$ cd carbondata
$ cat > sample.csv << EOF
  id,name,city,age
  1,david,shenzhen,31
  2,eason,shenzhen,27
  3,jarry,wuhan,35
  EOF
```


## Interactive Analysis with Spark Shell

## Version 2.1

Apache Spark Shell provides a simple way to learn the API, as well as a powerful tool to analyze data interactively. Please visit [Apache Spark Documentation](http://spark.apache.org/docs/latest/) for more details on Spark shell.

#### Basics

Start Spark shell by running the following command in the Spark directory:

```
./bin/spark-shell --jars <carbondata jar path>
```

In this shell, SparkSession is readily available as 'spark' and Spark context is readily available as 'sc'.

In order to create a CarbonSession we will have to configure it explicitly in the following manner :

* Import the following :

```
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.CarbonSession._
```

* Create a CarbonSession :

```
val carbon = SparkSession.builder().config(sc.getConf).getOrCreateCarbonSession()
```

#### Executing Queries

##### Creating a Table

```
scala>carbon.sql("create table if not exists test_table
                (id string, name string, city string, age Int)
                STORED BY 'carbondata'")
```

##### Loading Data to a Table

```
scala>carbon.sql(s"load data inpath '${new java.io.File("../carbondata/sample.csv").getCanonicalPath}' into table test_table")
```

###### Query Data from a Table

```
scala>spark.sql("select * from test_table").show

scala>spark.sql("select city, avg(age),
sum(age) from test_table group by city").show
```


## Interactive Analysis with Spark Shell
## Version 1.6


#### Basics

Start Spark shell by running the following command in the Spark directory:

```
./bin/spark-shell --jars <carbondata jar path>
```

NOTE: In this shell, SparkContext is readily available as sc.

* In order to execute the Queries we need to import CarbonContext:

```
import org.apache.spark.sql.CarbonContext
```

* Create an instance of CarbonContext in the following manner :

```
val cc = new CarbonContext(sc)
```

NOTE: By default store location is pointed to "../carbon.store", user can provide own store location to CarbonContext like new CarbonContext(sc, storeLocation).

#### Executing Queries

##### Creating a Table

```
scala>cc.sql("create table if not exists test_table (id string, name string, city string, age Int) STORED BY 'carbondata'")
```
To see the table created :

```
scala>cc.sql("show tables").show
```

##### Loading Data to a Table

```
scala>cc.sql(s"load data inpath '${new java.io.File("../carbondata/sample.csv").getCanonicalPath}' into table test_table")
```

##### Query Data from a Table

```
scala>cc.sql("select * from test_table").show
scala>cc.sql("select city, avg(age), sum(age) from test_table group by city").show
```

## Building CarbonData

To get started, get CarbonData from the [downloads](http://carbondata.incubator.apache.org/) section on the [http://carbondata.incubator.apache.org.](http://carbondata.incubator.apache.org.)
CarbonData uses Hadoop’s client libraries for HDFS and YARN and Spark's libraries. Downloads are pre-packaged for a handful of popular Spark versions.

If you’d like to build CarbonData from source, visit [Building CarbonData And IDE Configuration](https://github.com/apache/incubator-carbondata/blob/master/build/README.md).