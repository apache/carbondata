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

# Upsert into a Carbon DataSet using Merge 

## SCD and CDC Scenarios
Change Data Capture (CDC), is to apply all data changes generated from an external data set 
into a target dataset. In other words, a set of changes (update/delete/insert) applied to an external 
table needs to be applied to a target table.

Slowly Changing Dimensions (SCD), are the dimensions in which the data changes slowly, rather 
than changing regularly on a time basis.

SCD and CDC data changes can be merged to a carbon dataset online using the data frame level `MERGE` API.

#### MERGE API

Below API merges the datasets online and applies the actions as per the conditions. 

```
  targetDS.merge(sourceDS, <condition>)
          .whenMatched(<condition>)
          .updateExpr(updateMap)
          .insertExpr(insertMap_u)
          .whenNotMatched(<condition>)
          .insertExpr(insertMap)
          .whenNotMatchedAndExistsOnlyOnTarget(<condition>)
          .delete()
          .insertHistoryTableExpr(insertMap_d, <table_name>)
          .execute()
```
**NOTE:** SQL syntax for merge is not yet supported.

##### Example code sample to implement ccd scenario

```scala
// Import dependencies.
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.CarbonSession._
import org.apache.spark.sql.test.SparkTestQueryExecutor

object TestCCD {
  def main(args: Array[String]): Unit = {

    // Create/Get sparkSession and sqlContext
    val spark: SparkSession = SparkTestQueryExecutor.spark
    val sqlContext: SQLContext = spark.sqlContext

    // create a target data frame
    val initDataFrame = sqlContext.sparkSession.createDataFrame(Seq(
      Row("a", "0"),
      Row("b", "1"),
      Row("c", "2"),
      Row("d", "3")
    ).asJava, StructType(Seq(StructField("key", StringType), StructField("value", StringType))))

    initDataFrame.write
      .format("carbondata")
      .option("tableName", "target")
      .mode(SaveMode.Overwrite)
      .save()
    val target = sqlContext.read.format("carbondata").option("tableName", "target").load()

    // create a cdc/scd scenario
    var cdc =
      sqlContext.sparkSession.createDataFrame(Seq(
        Row("a", "10", false, 0),
        Row("a", null, true, 1), // a was updated and then deleted
        Row("b", null, true, 2), // b was just deleted once
        Row("c", null, true, 3), // c was deleted and then updated twice
        Row("c", "20", false, 4),
        Row("c", "200", false, 5),
        Row("e", "100", false, 6) // new key
      ).asJava,
        StructType(Seq(StructField("key", StringType),
          StructField("newValue", StringType),
          StructField("deleted", BooleanType), StructField("time", IntegerType))))
    cdc.createOrReplaceTempView("changes")

    cdc = spark.sql(
      "SELECT key, latest.newValue as newValue, latest.deleted as deleted FROM " +
        "( SELECT key, max(struct(time, newValue, deleted)) as latest FROM changes GROUP BY key)")

    val updateMap = Map("key" -> "B.key", "value" -> "B.newValue").asInstanceOf[Map[Any, Any]]

    val insertMap = Map("key" -> "B.key", "value" -> "B.newValue").asInstanceOf[Map[Any, Any]]

    // merge cdc to target dataset
    target.as("A").merge(cdc.as("B"), "A.key=B.key").
      whenMatched("B.deleted=false").
      updateExpr(updateMap).
      whenNotMatched("B.deleted=false").
      insertExpr(insertMap).
      whenMatched("B.deleted=true").
      delete().execute()

    // run select query and check results
    spark.sql("select * from target").show()
  }
}
```
