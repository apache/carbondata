# CarbonData Pre-aggregate tables
  
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
 // Create target carbon table and populate with initial data
 spark.sql(
   s"""
      | CREATE TABLE sales (
      | user_id string,
      | country string,
      | quantity int,
      | price bigint)
      | STORED BY 'carbondata'""".stripMargin)
      
 spark.sql(
   s"""
      | CREATE DATAMAP agg_sales
      | ON TABLE sales
      | USING "preaggregate"
      | AS
      | SELECT country, sum(quantity), avg(price)
      | FROM sales
      | GROUP BY country""".stripMargin)
      
 import spark.implicits._
 import org.apache.spark.sql.SaveMode
 import scala.util.Random
 
 val r = new Random()
 val df = spark.sparkContext.parallelize(1 to 10)
   .map(x => ("ID." + r.nextInt(100000), "country" + x % 8, x % 50, x % 60))
   .toDF("user_id", "country", "quantity", "price")

 // Create table with pre-aggregate table
 df.write.format("carbondata")
   .option("tableName", "sales")
   .option("compress", "true")
   .mode(SaveMode.Append).save()
      
 spark.sql(
      s"""
    |SELECT country, sum(quantity), avg(price)
    | from sales GROUP BY country""".stripMargin).show

 spark.stop
```

##PRE-AGGREGATE TABLES  
  Carbondata supports pre aggregating of data so that OLAP kind of queries can fetch data 
  much faster.Aggregate tables are created as datamaps so that the handling is as efficient as 
  other indexing support.Users can create as many aggregate tables they require as datamaps to 
  improve their query performance,provided the storage requirements and loading speeds are 
  acceptable.
  
  For main table called **sales** which is defined as 
  
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
  
  user can create pre-aggregate tables using the DDL
  
  ```
  CREATE DATAMAP agg_sales
  ON TABLE sales
  USING "preaggregate"
  AS
  SELECT country, sex, sum(quantity), avg(price)
  FROM sales
  GROUP BY country, sex
  ```
  

  
<b><p align="left">Functions supported in pre-aggregate tables</p></b>

| Function | Rollup supported |
|-----------|----------------|
| SUM | Yes |
| AVG | Yes |
| MAX | Yes |
| MIN | Yes |
| COUNT | Yes |


##### How pre-aggregate tables are selected
For the main table **sales** and pre-aggregate table **agg_sales** created above, queries of the 
kind
```
SELECT country, sex, sum(quantity), avg(price) from sales GROUP BY country, sex

SELECT sex, sum(quantity) from sales GROUP BY sex

SELECT sum(price), country from sales GROUP BY country
``` 

will be transformed by Query Planner to fetch data from pre-aggregate table **agg_sales**

But queries of kind
```
SELECT user_id, country, sex, sum(quantity), avg(price) from sales GROUP BY user_id, country, sex

SELECT sex, avg(quantity) from sales GROUP BY sex

SELECT country, max(price) from sales GROUP BY country
```

will fetch the data from the main table **sales**

##### Loading data to pre-aggregate tables
For existing table with loaded data, data load to pre-aggregate table will be triggered by the 
CREATE DATAMAP statement when user creates the pre-aggregate table.
For incremental loads after aggregates tables are created, loading data to main table triggers 
the load to pre-aggregate tables once main table loading is complete. These loads are automic 
meaning that data on main table and aggregate tables are only visible to the user after all tables 
are loaded

##### Querying data from pre-aggregate tables
Pre-aggregate tables cannot be queries directly. Queries are to be made on main table. Internally 
carbondata will check associated pre-aggregate tables with the main table, and if the 
pre-aggregate tables satisfy the query condition, the plan is transformed automatically to use 
pre-aggregate table to fetch the data.

##### Compacting pre-aggregate tables
Compaction command (ALTER TABLE COMPACT) need to be run separately on each pre-aggregate table.
Running Compaction command on main table will **not automatically** compact the pre-aggregate 
tables.Compaction is an optional operation for pre-aggregate table. If compaction is performed on
main table but not performed on pre-aggregate table, all queries still can benefit from 
pre-aggregate tables. To further improve performance on pre-aggregate tables, compaction can be 
triggered on pre-aggregate tables directly, it will merge the segments inside pre-aggregate table. 

##### Update/Delete Operations on pre-aggregate tables
This functionality is not supported.

  NOTE (<b>RESTRICTION</b>):
  Update/Delete operations are <b>not supported</b> on main table which has pre-aggregate tables 
  created on it. All the pre-aggregate tables <b>will have to be dropped</b> before update/delete 
  operations can be performed on the main table. Pre-aggregate tables can be rebuilt manually 
  after update/delete operations are completed
 
##### Delete Segment Operations on pre-aggregate tables
This functionality is not supported.

  NOTE (<b>RESTRICTION</b>):
  Delete Segment operations are <b>not supported</b> on main table which has pre-aggregate tables 
  created on it. All the pre-aggregate tables <b>will have to be dropped</b> before update/delete 
  operations can be performed on the main table. Pre-aggregate tables can be rebuilt manually 
  after delete segment operations are completed
  
##### Alter Table Operations on pre-aggregate tables
This functionality is not supported.

  NOTE (<b>RESTRICTION</b>):
  Adding new column in new table does not have any affect on pre-aggregate tables. However if 
  dropping or renaming a column has impact in pre-aggregate table, such operations will be 
  rejected and error will be thrown. All the pre-aggregate tables <b>will have to be dropped</b> 
  before Alter Operations can be performed on the main table. Pre-aggregate tables can be rebuilt 
  manually after Alter Table operations are completed
  
### Supporting timeseries data (Alpha feature in 1.3.0)
Carbondata has built-in understanding of time hierarchy and levels: year, month, day, hour, minute.
Multiple pre-aggregate tables can be created for the hierarchy and Carbondata can do automatic 
roll-up for the queries on these hierarchies.

  ```
  CREATE DATAMAP agg_year
  ON TABLE sales
  USING "timeseries"
  DMPROPERTIES (
  'event_time'='order_time',
  'year_granualrity'='1',
  ) AS
  SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
   avg(price) FROM sales GROUP BY order_time, country, sex
    
  CREATE DATAMAP agg_month
  ON TABLE sales
  USING "timeseries"
  DMPROPERTIES (
  'event_time'='order_time',
  'month_granualrity'='1',
  ) AS
  SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
   avg(price) FROM sales GROUP BY order_time, country, sex
    
  CREATE DATAMAP agg_day
  ON TABLE sales
  USING "timeseries"
  DMPROPERTIES (
  'event_time'='order_time',
  'day_granualrity'='1',
  ) AS
  SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
   avg(price) FROM sales GROUP BY order_time, country, sex
        
  CREATE DATAMAP agg_sales_hour
  ON TABLE sales
  USING "timeseries"
  DMPROPERTIES (
  'event_time'='order_time',
  'hour_granualrity'='1',
  ) AS
  SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
   avg(price) FROM sales GROUP BY order_time, country, sex
  
  CREATE DATAMAP agg_minute
  ON TABLE sales
  USING "timeseries"
  DMPROPERTIES (
  'event_time'='order_time',
  'minute_granualrity'='1',
  ) AS
  SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
   avg(price) FROM sales GROUP BY order_time, country, sex
    
  CREATE DATAMAP agg_minute
  ON TABLE sales
  USING "timeseries"
  DMPROPERTIES (
  'event_time'='order_time',
  'minute_granualrity'='1',
  ) AS
  SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
   avg(price) FROM sales GROUP BY order_time, country, sex
  ```
  
  For Querying data and automatically roll-up to the desired aggregation level,Carbondata supports 
  UDF as
  ```
  timeseries(timeseries column name, 'aggregation level')
  ```
  ```
  Select timeseries(order_time, 'hour'), sum(quantity) from sales group by timeseries(order_time,
  'hour')
  ```
  
  It is **not necessary** to create pre-aggregate tables for each granularity unless required for 
  query. Carbondata can roll-up the data and fetch it.
   
  For Example: For main table **sales** , If pre-aggregate tables were created as  
  
  ```
  CREATE DATAMAP agg_day
    ON TABLE sales
    USING "timeseries"
    DMPROPERTIES (
    'event_time'='order_time',
    'day_granualrity'='1',
    ) AS
    SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
     avg(price) FROM sales GROUP BY order_time, country, sex
          
    CREATE DATAMAP agg_sales_hour
    ON TABLE sales
    USING "timeseries"
    DMPROPERTIES (
    'event_time'='order_time',
    'hour_granualrity'='1',
    ) AS
    SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
     avg(price) FROM sales GROUP BY order_time, country, sex
  ```
  
  Queries like below will be rolled-up and fetched from pre-aggregate tables
  ```
  Select timeseries(order_time, 'month'), sum(quantity) from sales group by timeseries(order_time,
    'month')
    
  Select timeseries(order_time, 'year'), sum(quantity) from sales group by timeseries(order_time,
    'year')
  ```
  
  NOTE (<b>RESTRICTION</b>):
  * Only value of 1 is supported for hierarchy levels. Other hierarchy levels are not supported. 
  Other hierarchy levels are not supported
  * pre-aggregate tables for the desired levels needs to be created one after the other
  * pre-aggregate tables created for each level needs to be dropped separately 
    
