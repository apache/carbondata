# StreamSQL

Currently, user need to write Spark Streaming APP to use carbon streaming ingest feature, which is not so convenient for some users. To overcome this, CarbonData provides StreamSQL, using which user can manage the streaming job more easily.

- [Streaming Table](#streaming-table)
- [Streaming Job Management](#streaming-job-management)
  - [START STREAM](#start-stream)
  - [STOP STREAM](#stop-stream)
  - [SHOW STREAMS](#show-streams)



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
         |STORED BY carbondata
         |TBLPROPERTIES (
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
         |STORED BY carbondata
         |TBLPROPERTIES (
         |  'streaming'='true'
         |)
      """.stripMargin)

    sql(
      """
        |START STREAM job123 ON TABLE sink
        |STMPROPERTIES(
        |  'trigger'='ProcessingTime',
        |  'interval'='1 seconds')
        |AS
        |  SELECT *
        |  FROM source
        |  WHERE id % 2 = 1
      """.stripMargin)

    sql("STOP STREAM job123")

    sql("SHOW STREAMS [ON TABLE tableName]")
```



In above example, two table is created: source and sink. The `source` table's format is `csv` and `sink` table format is `carbon`. Then a streaming job is created to stream data from source table to sink table.

These two tables are normal carbon table, they can be queried independently.



### Streaming Job Management

As above example shown:

- `START STREAM jobName ON TABLE tableName` is used to start a streaming ingest job. 
- `STOP STREAM jobName` is used to stop a streaming job by its name
- `SHOW STREAMS [ON TABLE tableName]` is used to print streaming job information



##### START STREAM

When this is issued, carbon will start a structured streaming job to do the streaming ingestion. Before launching the job, system will validate:

- The format of table specified in CTAS FROM clause must be one of: csv, json, text, parquet, kafka, socket.  These are formats supported by spark 2.2.0 structured streaming

- User should pass the options of the streaming source table in its TBLPROPERTIES when creating it. StreamSQL will pass them transparently to spark when creating the streaming job. For example:

  ```SQL
  CREATE TABLE source(
    name STRING,
    age INT
  )
  STORED BY carbondata
  TBLPROPERTIES(
    'format'='socket',
    'host'='localhost',
    'port'='8888'
  )
  ```

  will translate to

  ```Scala
  spark.readStream
  	 .schema(tableSchema)
  	 .format("socket")
  	 .option("host", "localhost")
  	 .option("port", "8888")
  ```



- The sink table should have a TBLPROPERTY `'streaming'` equal to `true`, indicating it is a streaming table.
- In the given STMPROPERTIES, user must specify `'trigger'`, its value must be `ProcessingTime` (In future, other value will be supported). User should also specify interval value for the streaming job.
- If the schema specifid in sink table is different from CTAS, the streaming job will fail



##### STOP STREAM

When this is issued, the streaming job will be stopped immediately. It will fail if the jobName specified is not exist.



##### SHOW STREAMS

`SHOW STREAMS ON TABLE tableName` command will print the streaming job information as following

| Job name | status  | Source | Sink | start time          | time elapsed |
| -------- | ------- | ------ | ---- | ------------------- | ------------ |
| job123   | Started | device | fact | 2018-02-03 14:32:42 | 10d2h32m     |

`SHOW STREAMS` command will show all stream jobs in the system.



<script>
// Show selected style on nav item
$(function() { $('.b-nav__api').addClass('selected'); });
</script>