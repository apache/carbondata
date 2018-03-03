# CarbonData Pre-aggregate DataMap
  
* [Quick Example](#quick-example)
* [DataMap Management](#datamap-management)
* [Pre-aggregate Table](#preaggregate-datamap-introduction)
* [Loading Data](#loading-data)
* [Querying Data](#querying-data)
* [Compaction](#compacting-pre-aggregate-tables)
* [Data Management](#data-management-with-pre-aggregate-tables)

## Quick example
Download and unzip spark-2.2.0-bin-hadoop2.7.tgz, and export $SPARK_HOME

Package carbon jar, and copy assembly/target/scala-2.11/carbondata_2.11-x.x.x-SNAPSHOT-shade-hadoop2.7.2.jar to $SPARK_HOME/jars
```shell
mvn clean package -DskipTests -Pspark-2.2
```

Start spark-shell in new terminal, type :paste, then copy and run the following code.
```scala
 import java.io.File
 import org.apache.spark.sql.{CarbonEnv, SparkSession}
 import org.apache.spark.sql.CarbonSession._
 import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}
 import org.apache.carbondata.core.util.path.CarbonStorePath
 
 val warehouse = new File("./warehouse").getCanonicalPath
 val metastore = new File("./metastore").getCanonicalPath
 
 val spark = SparkSession
   .builder()
   .master("local")
   .appName("preAggregateExample")
   .config("spark.sql.warehouse.dir", warehouse)
   .getOrCreateCarbonSession(warehouse, metastore)

 spark.sparkContext.setLogLevel("ERROR")

 // drop table if exists previously
 spark.sql(s"DROP TABLE IF EXISTS sales")
 
 // Create main table
 spark.sql(
   s"""
      | CREATE TABLE sales (
      | user_id string,
      | country string,
      | quantity int,
      | price bigint)
      | STORED BY 'carbondata'
    """.stripMargin)
 
 // Create pre-aggregate table on the main table
 // If main table already have data, following command 
 // will trigger one immediate load to the pre-aggregate table
 spark.sql(
   s"""
      | CREATE DATAMAP agg_sales
      | ON TABLE sales
      | USING "preaggregate"
      | AS
      | SELECT country, sum(quantity), avg(price)
      | FROM sales
      | GROUP BY country
    """.stripMargin)
      
  import spark.implicits._
  import org.apache.spark.sql.SaveMode
  import scala.util.Random
 
  // Load data to the main table, it will also
  // trigger immediate load to pre-aggregate table.
  // These two loading operation is carried out in a
  // transactional manner, meaning that the whole 
  // operation will fail if one of the loading fails
  val r = new Random()
  spark.sparkContext.parallelize(1 to 10)
   .map(x => ("ID." + r.nextInt(100000), "country" + x % 8, x % 50, x % 60))
   .toDF("user_id", "country", "quantity", "price")
   .write
   .format("carbondata")
   .option("tableName", "sales")
   .option("compress", "true")
   .mode(SaveMode.Append)
   .save()
      
  spark.sql(
    s"""
       |SELECT country, sum(quantity), avg(price)
       | from sales GROUP BY country
     """.stripMargin).show

  spark.stop
```

#### DataMap Management
DataMap can be created using following DDL
  ```
  CREATE DATAMAP [IF NOT EXISTS] datamap_name
  ON TABLE main_table
  USING "datamap_provider"
  DMPROPERTIES ('key'='value', ...)
  AS
    SELECT statement
  ```
The string followed by USING is called DataMap Provider, in this version CarbonData supports two 
kinds of DataMap: 
1. preaggregate, for pre-aggregate table. No DMPROPERTY is required for this DataMap
2. timeseries, for timeseries roll-up table. Please refer to [Timeseries DataMap](https://github.com/apache/carbondata/blob/master/docs/datamap/timeseries-datamap-guide.md)

DataMap can be dropped using following DDL
  ```
  DROP DATAMAP [IF EXISTS] datamap_name
  ON TABLE main_table
  ```
To show all DataMaps created, use:
  ```
  SHOW DATAMAP 
  ON TABLE main_table
  ```
It will show all DataMaps created on main table.


## Preaggregate DataMap Introduction
  Pre-aggregate tables are created as DataMaps and managed as tables internally by CarbonData. 
  User can create as many pre-aggregate datamaps required to improve query performance, 
  provided the storage requirements and loading speeds are acceptable.
  
  Once pre-aggregate datamaps are created, CarbonData's SparkSQL optimizer extension supports to 
  select the most efficient pre-aggregate datamap and rewrite the SQL to query against the selected 
  datamap instead of the main table. Since the data size of pre-aggregate datamap is smaller, 
  user queries are much faster. In our previous experience, we have seen 5X to 100X times faster 
  in production SQLs.
    
  For instance, main table called **sales** which is defined as 
  
  ```
  CREATE TABLE sales (
    order_time timestamp,
    user_id string,
    sex string,
    country string,
    quantity int,
    price bigint)
  STORED BY 'carbondata'
  ```
  
  User can create pre-aggregate tables using the Create DataMap DDL
  
  ```
  CREATE DATAMAP agg_sales
  ON TABLE sales
  USING "preaggregate"
  AS
    SELECT country, sex, sum(quantity), avg(price)
    FROM sales
    GROUP BY country, sex
  ```
  
#### Functions supported in pre-aggregate table

| Function | Rollup supported |
|----------|:----------------:|
| SUM      |Yes               |
| AVG      |Yes               |
| MAX      |Yes               |
| MIN      |Yes               |
| COUNT    |Yes               |


#### How pre-aggregate tables are selected
When a user query is submitted, during query planning phase, CarbonData will collect all matched 
pre-aggregate tables as candidates according to Relational Algebra transformation rules. Then, the 
best pre-aggregate table for this query will be selected among the candidates based on cost. 
For simplicity, current cost estimation is based on the data size of the pre-aggregate table. (We 
assume that query will be faster on smaller table)

For the main table **sales** and pre-aggregate table **agg_sales** created above, following queries 
```
SELECT country, sex, sum(quantity), avg(price) from sales GROUP BY country, sex

SELECT sex, sum(quantity) from sales GROUP BY sex

SELECT sum(price), country from sales GROUP BY country
``` 

will be transformed by CarbonData's query planner to query against pre-aggregate table 
**agg_sales** instead of the main table **sales**

However, for following queries
```
SELECT user_id, country, sex, sum(quantity), avg(price) from sales GROUP BY user_id, country, sex

SELECT sex, avg(quantity) from sales GROUP BY sex

SELECT country, max(price) from sales GROUP BY country
```

will query against main table **sales** only, because it does not satisfy pre-aggregate table 
selection logic. 

## Loading data
For existing table with loaded data, data load to pre-aggregate table will be triggered by the 
CREATE DATAMAP statement when user creates the pre-aggregate table. For incremental loads after 
aggregates tables are created, loading data to main table triggers the load to pre-aggregate tables 
once main table loading is complete. 

These loads are transactional 
meaning that data on main table and pre-aggregate tables are only visible to the user after all 
tables are loaded successfully, if one of these loads fails, new data are not visible in all tables 
as if the load operation is not happened.   

## Querying data
As a technique for query acceleration, Pre-aggregate tables cannot be queries directly. 
Queries are to be made on main table. While doing query planning, internally CarbonData will check 
associated pre-aggregate tables with the main table, and do query plan transformation accordingly. 

User can verify whether a query can leverage pre-aggregate table or not by executing `EXPLAIN`
command, which will show the transformed logical plan, and thus user can check whether pre-aggregate
table is selected.


## Compacting pre-aggregate tables
Running Compaction command (`ALTER TABLE COMPACT`) on main table will **not automatically** 
compact the pre-aggregate tables created on the main table. User need to run Compaction command 
separately on each pre-aggregate table to compact them.

Compaction is an optional operation for pre-aggregate table. If compaction is performed on
main table but not performed on pre-aggregate table, all queries still can benefit from 
pre-aggregate tables. To further improve the query performance, compaction on pre-aggregate tables 
can be triggered to merge the segments and files in the pre-aggregate tables. 

## Data Management with pre-aggregate tables
In current implementation, data consistence need to maintained for both main table and pre-aggregate
tables. Once there is pre-aggregate table created on the main table, following command on the main 
table
is not supported:
1. Data management command: `UPDATE/DELETE/DELETE SEGMENT`. 
2. Schema management command: `ALTER TABLE DROP COLUMN`, `ALTER TABLE CHANGE DATATYPE`, 
`ALTER TABLE RENAME`. Note that adding a new column is supported, and for dropping columns and 
change datatype command, CarbonData will check whether it will impact the pre-aggregate table, if 
 not, the operation is allowed, otherwise operation will be rejected by throwing exception.   
3. Partition management command: `ALTER TABLE ADD/DROP PARTITION`

However, there is still way to support these operations on main table, in current CarbonData 
release, user can do as following:
1. Remove the pre-aggregate table by `DROP DATAMAP` command
2. Carry out the data management operation on main table
3. Create the pre-aggregate table again by `CREATE DATAMAP` command
Basically, user can manually trigger the operation by re-building the datamap.


