<!--
    Licensed to the Apache Software Foundation (ASF) under one or more 
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership. 
    The ASF licenses this file to you under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with 
    the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software 
    distributed under the License is distributed on an "AS IS" BASIS, 
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and 
    limitations under the License.
-->

# CarbonData Streaming Ingestion

- [Streaming Table Management](#quick-example)
  - [Create table with streaming property](#create-table-with-streaming-property)
  - [Alter streaming property](#alter-streaming-property)
  - [Acquire streaming lock](#acquire-streaming-lock)
  - [Create streaming segment](#create-streaming-segment)
  - [Change Stream segment status](#change-segment-status)
  - [Handoff "streaming finish" segment to columnar segment](#handoff-streaming-finish-segment-to-columnar-segment)
  - [Auto handoff streaming segment](#auto-handoff-streaming-segment)
  - [Stream data parser](#stream-data-parser)
  - [Close streaming table](#close-streaming-table)
  - [Constraints](#constraint)
- [StreamSQL](#streamsql)
  - [Defining Streaming Table](#streaming-table)
  - [Streaming Job Management](#streaming-job-management)
    - [CREATE STREAM](#create-stream)
    - [DROP STREAM](#drop-stream)
    - [SHOW STREAMS](#show-streams)
    - [CLOSE STREAM](#close-stream)

## Quick example
Download and unzip spark-2.4.4-bin-hadoop2.7.tgz, and export $SPARK_HOME

Package carbon jar, and copy assembly/target/scala-2.11/carbondata_2.11-2.0.0-SNAPSHOT-shade-hadoop2.7.2.jar to $SPARK_HOME/jars
```shell
mvn clean package -DskipTests -Pspark-2.4
```

Start a socket data server in a terminal
```shell
nc -lk 9099
```
 type some CSV rows as following
```csv
1,col1
2,col2
3,col3
4,col4
5,col5
```

Start spark-shell in new terminal, type :paste, then copy and run the following code.
```scala
 import java.io.File
 import org.apache.spark.sql.{CarbonEnv, SparkSession}
 import org.apache.spark.sql.CarbonSession._
 import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}
 import org.apache.carbondata.core.util.path.CarbonTablePath
 import org.apache.carbondata.streaming.parser.CarbonStreamParser

 val warehouse = new File("./warehouse").getCanonicalPath
 val metastore = new File("./metastore").getCanonicalPath

 val spark = SparkSession
   .builder()
   .master("local")
   .appName("StreamExample")
   .config("spark.sql.warehouse.dir", warehouse)
   .getOrCreateCarbonSession(warehouse, metastore)

 spark.sparkContext.setLogLevel("ERROR")

 // drop table if exists previously
 spark.sql(s"DROP TABLE IF EXISTS carbon_table")
 // Create target carbon table and populate with initial data
 spark.sql(
   s"""
      | CREATE TABLE carbon_table (
      | col1 INT,
      | col2 STRING
      | )
      | STORED AS carbondata
      | TBLPROPERTIES('streaming'='true')""".stripMargin)

 val carbonTable = CarbonEnv.getCarbonTable(Some("default"), "carbon_table")(spark)
 val tablePath = carbonTable.getTablePath

 // batch load
 var qry: StreamingQuery = null
 val readSocketDF = spark.readStream
   .format("socket")
   .option("host", "localhost")
   .option("port", 9099)
   .load()

 // Write data from socket stream to carbondata file
 qry = readSocketDF.writeStream
   .format("carbondata")
   .trigger(ProcessingTime("5 seconds"))
   .option("checkpointLocation", CarbonTablePath.getStreamingCheckpointDir(tablePath))
   .option("dbName", "default")
   .option("tableName", "carbon_table")
   .option(CarbonStreamParser.CARBON_STREAM_PARSER,
     CarbonStreamParser.CARBON_STREAM_PARSER_CSV)
   .start()

 // start new thread to show data
 new Thread() {
   override def run(): Unit = {
     do {
       spark.sql("select * from carbon_table").show(false)
       Thread.sleep(10000)
     } while (true)
   }
 }.start()

 qry.awaitTermination()
```

Continue to type some rows into data server, and spark-shell will show the new data of the table.

## Create table with streaming property
Streaming table is just a normal carbon table with "streaming" table property, user can create
streaming table using following DDL.
```sql
CREATE TABLE streaming_table (
 col1 INT,
 col2 STRING
)
STORED AS carbondata
TBLPROPERTIES('streaming'='true')
```

 property name | default | description
 ---|---|--- 
 streaming | false |Whether to enable streaming ingest feature for this table <br /> Value range: true, false 

 "DESC FORMATTED" command will show streaming property.
 ```sql
 DESC FORMATTED streaming_table
 ```

## Alter streaming property
For an old table, use ALTER TABLE command to set the streaming property.
```sql
ALTER TABLE streaming_table SET TBLPROPERTIES('streaming'='true')
```

## Acquire streaming lock
At the begin of streaming ingestion, the system will try to acquire the table level lock of streaming.lock file. If the system isn't able to acquire the lock of this table, it will throw an InterruptedException.

## Create streaming segment
The streaming data will be ingested into a separate segment of carbondata table, this segment is termed as streaming segment. The status of this segment will be recorded as "streaming" in "tablestatus" file along with its data size. You can use "SHOW SEGMENTS FOR TABLE tableName" to check segment status. 

After the streaming segment reaches the max size, CarbonData will change the segment status to "streaming finish" from "streaming", and create new "streaming" segment to continue to ingest streaming data.

option | default | description
--- | --- | ---
carbon.streaming.segment.max.size | 1024000000 | Unit: byte <br />max size of streaming segment

segment status | description
--- | ---
streaming | The segment is running streaming ingestion
streaming finish | The segment already finished streaming ingestion, <br /> it will be handed off to a segment in the columnar format

## Change segment status
Use below command to change the status of "streaming" segment to "streaming finish" segment. If the streaming application is running, this command will be blocked.
```sql
ALTER TABLE streaming_table FINISH STREAMING
```

## Handoff "streaming finish" segment to columnar segment
Use below command to handoff "streaming finish" segment to columnar format segment manually.
```sql
ALTER TABLE streaming_table COMPACT 'streaming'

```

## Auto handoff streaming segment
Config the property "carbon.streaming.auto.handoff.enabled" to auto handoff streaming segment. If the value of this property is true, after the streaming segment reaches the max size, CarbonData will change this segment to "streaming finish" status and trigger to auto handoff this segment to columnar format segment in a new thread.

property name | default | description
--- | --- | ---
carbon.streaming.auto.handoff.enabled | true | whether to auto trigger handoff operation

## Stream data parser
Config the property "carbon.stream.parser" to define a stream parser to convert InternalRow to Object[] when write stream data.

property name | default | description
--- | --- | ---
carbon.stream.parser | org.apache.carbondata.streaming.parser.RowStreamParserImp | the class of the stream parser

Currently CarbonData support two parsers, as following:

**1. org.apache.carbondata.streaming.parser.CSVStreamParserImp**: This parser gets a line data(String type) from the first index of InternalRow and converts this String to Object[].

**2. org.apache.carbondata.streaming.parser.RowStreamParserImp**: This is the default stream parser, it will auto convert InternalRow to Object[] according to schema of this `DataSet`, for example:

```scala
 case class FileElement(school: Array[String], age: Int)
 case class StreamData(id: Int, name: String, city: String, salary: Float, file: FileElement)
 ...

 var qry: StreamingQuery = null
 val readSocketDF = spark.readStream
   .format("socket")
   .option("host", "localhost")
   .option("port", 9099)
   .load()
   .as[String]
   .map(_.split(","))
   .map { fields => {
     val tmp = fields(4).split("\\$")
     val file = FileElement(tmp(0).split(":"), tmp(1).toInt)
     StreamData(fields(0).toInt, fields(1), fields(2), fields(3).toFloat, file)
   } }

 // Write data from socket stream to carbondata file
 qry = readSocketDF.writeStream
   .format("carbondata")
   .trigger(ProcessingTime("5 seconds"))
   .option("checkpointLocation", tablePath.getStreamingCheckpointDir)
   .option("dbName", "default")
   .option("tableName", "carbon_table")
   .start()

 ...
```

### How to implement a customized stream parser
If user needs to implement a customized stream parser to convert a specific InternalRow to Object[], it needs to implement `initialize` method and `parserRow` method of interface `CarbonStreamParser`, for example:

```scala
 package org.XXX.XXX.streaming.parser
 
 import org.apache.hadoop.conf.Configuration
 import org.apache.spark.sql.catalyst.InternalRow
 import org.apache.spark.sql.types.StructType
 
 class XXXStreamParserImp extends CarbonStreamParser {
 
   override def initialize(configuration: Configuration, structType: StructType): Unit = {
     // user can get the properties from "configuration"
   }
   
   override def parserRow(value: InternalRow): Array[Object] = {
     // convert InternalRow to Object[](Array[Object] in Scala) 
   }
   
   override def close(): Unit = {
   }
 }
   
```

and then set the property "carbon.stream.parser" to "org.XXX.XXX.streaming.parser.XXXStreamParserImp".

## Close streaming table
Use below command to handoff all streaming segments to columnar format segments and modify the streaming property to false, this table becomes a normal table.
```sql
ALTER TABLE streaming_table COMPACT 'close_streaming'

```

## Constraint
1. reject set streaming property from true to false.
2. reject UPDATE/DELETE command on the streaming table.
3. reject create MV on the streaming table.
4. reject add the streaming property on the table with MV.
5. if the table has dictionary columns, it will not support concurrent data loading.
6. block delete "streaming" segment while the streaming ingestion is running.
7. block drop the streaming table while the streaming ingestion is running.



## StreamSQL



### Streaming Table

**Example**

Following example shows how to start a streaming ingest job

```
    sql(
      s"""
         |CREATE TABLE source(
         | id INT,
         | name STRING,
         | city STRING,
         | salary FLOAT,
         | tax DECIMAL(8,2),
         | percent double,
         | birthday DATE,
         | register TIMESTAMP,
         | updated TIMESTAMP
         |)
         |STORED AS carbondata
         |TBLPROPERTIES (
         | 'streaming'='source',
         | 'format'='csv',
         | 'path'='$csvDataDir'
         |)
      """.stripMargin)

    sql(
      s"""
         |CREATE TABLE sink(
         | id INT,
         | name STRING,
         | city STRING,
         | salary FLOAT,
         | tax DECIMAL(8,2),
         | percent double,
         | birthday DATE,
         | register TIMESTAMP,
         | updated TIMESTAMP
         |)
         |STORED AS carbondata
         |TBLPROPERTIES (
         |  'streaming'='true'
         |)
      """.stripMargin)

    sql(
      """
        |CREATE STREAM job123 ON TABLE sink
        |STMPROPERTIES(
        |  'trigger'='ProcessingTime',
        |  'interval'='1 seconds')
        |AS
        |  SELECT *
        |  FROM source
        |  WHERE id % 2 = 1
      """.stripMargin)

    sql("DROP STREAM job123")

    sql("SHOW STREAMS [ON TABLE tableName]")
```



In above example, two table is created: source and sink. The `source` table's format is `csv` and `sink` table format is `carbon`. Then a streaming job is created to stream data from source table to sink table.

These two tables are normal carbon tables, they can be queried independently.



### Streaming Job Management

As above example shown:

- `CREATE STREAM jobName ON TABLE tableName` is used to start a streaming ingest job. 
- `DROP STREAM jobName` is used to stop a streaming job by its name
- `SHOW STREAMS [ON TABLE tableName]` is used to print streaming job information



##### CREATE STREAM

When this is issued, carbon will start a structured streaming job to do the streaming ingestion. Before launching the job, system will validate:

- The format of table specified in CTAS FROM clause must be one of: csv, json, text, parquet, kafka, socket.  These are formats supported by spark 2.2.0 structured streaming

- User should pass the options of the streaming source table in its TBLPROPERTIES when creating it. StreamSQL will pass them transparently to spark when creating the streaming job. For example:

  ```SQL
  CREATE TABLE source(
    name STRING,
    age INT
  )
  STORED AS carbondata
  TBLPROPERTIES(
   'streaming'='source',
   'format'='socket',
   'host'='localhost',
   'port'='8888',
   'record_format'='csv', // can be csv or json, default is csv
   'delimiter'='|'
  )
  ```

  will translate to

  ```Scala
  spark.readStream
  	 .schema(tableSchema)
  	 .format("socket")
  	 .option("host", "localhost")
  	 .option("port", "8888")
  	 .option("delimiter", "|")
  ```



- The sink table should have a TBLPROPERTY `'streaming'` equal to `true`, indicating it is a streaming table.
- In the given STMPROPERTIES, user must specify `'trigger'`, its value must be `ProcessingTime` (In future, other value will be supported). User should also specify interval value for the streaming job.
- If the schema specified in sink table is different from CTAS, the streaming job will fail

For Kafka data source, create the source table by:
  ```SQL
  CREATE TABLE source(
    name STRING,
    age INT
  )
  STORED AS carbondata
  TBLPROPERTIES(
   'streaming'='source',
   'format'='kafka',
   'kafka.bootstrap.servers'='kafkaserver:9092',
   'subscribe'='test'
   'record_format'='csv', // can be csv or json, default is csv
   'delimiter'='|'
  )
  ```

- Then CREATE STREAM can be used to start the streaming ingest job from source table to sink table
```
CREATE STREAM job123 ON TABLE sink
STMPROPERTIES(
    'trigger'='ProcessingTime',
     'interval'='10 seconds'
) 
AS
   SELECT *
   FROM source
   WHERE id % 2 = 1
```

##### DROP STREAM

When `DROP STREAM` is issued, the streaming job will be stopped immediately. It will fail if the jobName specified is not exist.
```
DROP STREAM job123
```


##### SHOW STREAMS

`SHOW STREAMS ON TABLE tableName` command will print the streaming job information as following

| Job name | status  | Source | Sink | start time          | time elapsed |
| -------- | ------- | ------ | ---- | ------------------- | ------------ |
| job123   | Started | device | fact | 2018-02-03 14:32:42 | 10d2h32m     |

`SHOW STREAMS` command will show all stream jobs in the system.

##### ALTER TABLE CLOSE STREAM

When the streaming application is stopped, and user want to manually trigger data conversion from carbon streaming files to columnar files, one can use
`ALTER TABLE sink COMPACT 'CLOSE_STREAMING';`


