<!--
    Licensed to the Apache Software Foundation (ASF) under one or more 
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership. 
    The ASF licenses this file to you under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with 
    the License.  You may obtain a copy of the License at

```
  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software 
distributed under the License is distributed on an "AS IS" BASIS, 
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and 
limitations under the License.
```

-->

# Carbon Flink Integration Guide

## Usage scenarios

  The CarbonData flink integration module is used to connect Flink and Carbon. The module provides 
  a set of Flink BulkWriter implementations (CarbonLocalWriter and CarbonS3Writer). The data is processed 
  by the Flink, and finally written into the stage directory of the target table by the CarbonXXXWriter. 

  By default, those data in table stage directory, can not be immediately queried, those data can be queried 
  after the `INSERT INTO $tableName STAGE` command is executed.

  Since the flink data written to carbon is endless, in order to ensure the visibility of data 
  and the controllable amount of data processed during the execution of each insert form stage command, 
  the user should execute the insert from stage command in a timely manner.

  The execution interval of the insert form stage command should take the data visibility requirements 
  of the actual business and the flink data traffic. When the data visibility requirements are high 
  or the data traffic is large, the execution interval should be appropriately shortened.
  
  A typical scenario is that the data is cleaned and preprocessed by Flink, and then written to Carbon, 
  for subsequent analysis and queries. 

## Usage description

### Writing process

  Typical flink stream: `Source -> Process -> Output(Carbon Writer Sink)`
  
  Pseudo code and description: 
  
  ```scala
    // Import dependencies.
    import java.util.Properties
    import org.apache.carbon.flink.CarbonWriterFactory
    import org.apache.carbon.flink.ProxyFileSystem
    import org.apache.carbondata.core.constants.CarbonCommonConstants
    import org.apache.flink.api.common.restartstrategy.RestartStrategies
    import org.apache.flink.core.fs.Path
    import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
    import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
   
    // Specify database name.
    val databaseName = "default"
   
    // Specify target table name.
    val tableName = "test"
    // Table path of the target table.
    val tablePath = "/data/warehouse/test"
    // Specify local temporary path.
    val dataTempPath = "/data/temp/"
   
    val tableProperties = new Properties
    // Set the table properties here.
   
    val writerProperties = new Properties
    // Set the writer properties here, such as temp path, commit threshold, access key, secret key, endpoint, etc.
   
    val carbonProperties = new Properties
    // Set the carbon properties here, such as date format, store location, etc.
     
    // Create carbon bulk writer factory. Two writer types are supported: 'Local' and 'S3'.
    val writerFactory = CarbonWriterFactory.builder("Local").build(
      databaseName,
      tableName,
      tablePath,
      tableProperties,
      writerProperties,
      carbonProperties
    )
     
    // Build a flink stream and run it.
    // 1. Create a new flink execution environment.
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    // Set flink environment configuration here, such as parallelism, checkpointing, restart strategy, etc.
   
    // 2. Create flink data source, may be a kafka source, custom source, or others.
    // The data type of source should be Array[AnyRef].
    // Array length should equals to table column count, and values order in array should matches table column order.
    val source = ...
    // 3. Create flink stream and set source.
    val stream = environment.addSource(source)
    // 4. Add other flink operators here.
    // ...
    // 5. Set flink stream target (write data to carbon with a write sink).
    stream.addSink(StreamingFileSink.forBulkFormat(new Path(ProxyFileSystem.DEFAULT_URI), writerFactory).build)
    // 6. Run flink stream.
    try {
      environment.execute
    } catch {
      case exception: Exception =>
        // Handle execute exception here.
    }
  ```

### Writer properties

#### Local Writer

  | Property                             | Name                                 | Description                                                                                             |
  |--------------------------------------|--------------------------------------|---------------------------------------------------------------------------------------------------------|
  | CarbonLocalProperty.DATA_TEMP_PATH   | carbon.writer.local.data.temp.path   | Usually is a local path, data will write to temp path first, and move to target data path finally.        |
  | CarbonLocalProperty.COMMIT_THRESHOLD | carbon.writer.local.commit.threshold | While written data count reach the threshold, data writer will flush and move data to target data path. | 

#### S3 Writer

  | Property                          | Name                              | Description                                                                                             |
  |-----------------------------------|-----------------------------------|---------------------------------------------------------------------------------------------------------|
  | CarbonS3Property.ACCESS_KEY       | carbon.writer.s3.access.key       | Access key of s3 file system                                                                            |
  | CarbonS3Property.SECRET_KEY       | carbon.writer.s3.secret.key       | Secret key of s3 file system                                                                            |
  | CarbonS3Property.ENDPOINT         | carbon.writer.s3.endpoint         | Endpoint of s3 file system                                                                              |
  | CarbonS3Property.DATA_TEMP_PATH   | carbon.writer.s3.data.temp.path   | Usually is a local path, data will write to temp path first, and move to target data path finally.        |
  | CarbonS3Property.COMMIT_THRESHOLD | carbon.writer.s3.commit.threshold | While written data count reach the threshold, data writer will flush and move data to target data path. |

### Insert from stage

  Refer [Grammar Description](./dml-of-carbondata.md#insert-data-into-carbondata-table-from-stage-input-files) for syntax.

## Usage Example Code

  Create target table.
  
  ```sql
    CREATE TABLE test (col1 string, col2 string, col3 string) STORED AS carbondata
  ```

  Writing flink data to local carbon table.

  ```scala
    import java.util.Properties
    import org.apache.carbon.flink.CarbonLocalProperty
    import org.apache.carbon.flink.CarbonWriterFactory
    import org.apache.carbon.flink.ProxyFileSystem
    import org.apache.carbondata.core.constants.CarbonCommonConstants
    import org.apache.flink.api.common.restartstrategy.RestartStrategies
    import org.apache.flink.core.fs.Path
    import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
    import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
    import org.apache.flink.streaming.api.functions.source.SourceFunction

    val databaseName = "default"
    val tableName = "test"
    val tablePath = "/data/warehouse/test"
    val dataTempPath = "/data/temp/"

    val tableProperties = new Properties

    val writerProperties = new Properties
    writerProperties.setProperty(CarbonLocalProperty.DATA_TEMP_PATH, dataTempPath)

    val carbonProperties = new Properties
    carbonProperties.setProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    carbonProperties.setProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    carbonProperties.setProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB, "1024")

    val writerFactory = CarbonWriterFactory.builder("Local").build(
      databaseName,
      tableName,
      tablePath,
      tableProperties,
      writerProperties,
      carbonProperties
    )

    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.enableCheckpointing(2000L)
    environment.setRestartStrategy(RestartStrategies.noRestart)

    // Define a custom source.
    val source = new SourceFunction[Array[AnyRef]]() {
      override
      def run(sourceContext: SourceFunction.SourceContext[Array[AnyRef]]): Unit = {
        // Array length should equals to table column count, and values order in array should matches table column order.
        val data = new Array[AnyRef](3)
        data(0) = "value1"
        data(1) = "value2"
        data(2) = "value3"
        sourceContext.collect(data)
      }

      override 
      def cancel(): Unit = {
        // do something.
      }
    }

    val stream = environment.addSource(source)
    val streamSink = StreamingFileSink.forBulkFormat(new Path(ProxyFileSystem.DEFAULT_URI), writerFactory).build

    stream.addSink(streamSink)

    try {
      environment.execute
    } catch {
      case exception: Exception =>
        // TODO
        throw new UnsupportedOperationException(exception)
    }
  ```

  Insert into table from stage directory.
  
  ```sql
    INSERT INTO test STAGE
  ```
