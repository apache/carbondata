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

## Quick example
Download and unzip spark-2.2.0-bin-hadoop2.7.tgz, and export $SPARK_HOME

Package carbon jar, and copy assembly/target/scala-2.11/carbondata_2.11-1.3.0-SNAPSHOT-shade-hadoop2.7.2.jar to $SPARK_HOME/jars
```shell
mvn clean package -DskipTests -Pspark-2.2
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
      | STORED BY 'carbondata'
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
 STORED BY 'carbondata'
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
The input data of streaming will be ingested into a segment of the CarbonData table, the status of this segment is streaming. CarbonData call it a streaming segment. The "tablestatus" file will record the segment status and data size. The user can use “SHOW SEGMENTS FOR TABLE tableName” to check segment status. 

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
3. reject create pre-aggregation DataMap on the streaming table.
4. reject add the streaming property on the table with pre-aggregation DataMap.
5. if the table has dictionary columns, it will not support concurrent data loading.
6. block delete "streaming" segment while the streaming ingestion is running.
7. block drop the streaming table while the streaming ingestion is running.
