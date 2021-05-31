/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.examples

import scala.collection.JavaConverters._

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.CarbonSession.DataSetMerge
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import org.apache.carbondata.examples.util.ExampleUtils

/**
 * Example for UPSERT APIs
 */
object DataUPSERTExample {

  def main(args: Array[String]): Unit = {
    val spark = ExampleUtils.createSparkSession("DataUPSERTExample")
    performUPSERT(spark)
  }

  def performUPSERT(spark: SparkSession): Unit = {
    spark.sql("drop table if exists target")
    val initframe = spark.createDataFrame(Seq(
      Row("a", "0"),
      Row("b", "1"),
      Row("c", "2"),
      Row("d", "3")
    ).asJava, StructType(Seq(StructField("key", StringType), StructField("value", StringType))))
    initframe.write
      .format("carbondata")
      .option("tableName", "target")
      .mode(SaveMode.Overwrite)
      .save()
    val target = spark.read.format("carbondata").option("tableName", "target").load()
    var cdc =
      spark.createDataFrame(Seq(
        Row("a", "7"),
        Row("b", null),
        Row("g", null),
        Row("e", "3")
      ).asJava,
        StructType(Seq(StructField("key", StringType),
          StructField("value", StringType))))
    spark.sql("select * from target").show(false)
    // upsert API updates a and b, inserts e and g
    target.as("A").upsert(cdc.as("B"), "key").execute()
    spark.sql("select * from target").show(false)

    cdc =
      spark.createDataFrame(Seq(
        Row("a", "7"),
        Row("e", "3")
      ).asJava,
        StructType(Seq(StructField("key", StringType),
          StructField("value", StringType))))
    // delete API, deletes a and e
    target.as("A").delete(cdc.as("B"), "key").execute()
    spark.sql("select * from target").show(false)

    cdc =
      spark.createDataFrame(Seq(
        Row("g", "56")
      ).asJava,
        StructType(Seq(StructField("key", StringType),
          StructField("value", StringType))))
    // update API, updates g
    target.as("A").update(cdc.as("B"), "key").execute()
    spark.sql("select * from target").show(false)

    cdc =
      spark.createDataFrame(Seq(
        Row("z", "234"),
        Row("x", "2")
      ).asJava,
        StructType(Seq(StructField("key", StringType),
          StructField("value", StringType))))
    // insert API, inserts z and x.
    target.as("A").insert(cdc.as("B"), "key"  ).execute()

    spark.sql("select * from target").show(false)
  }

}
