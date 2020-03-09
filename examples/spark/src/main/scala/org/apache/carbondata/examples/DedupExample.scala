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

import java.io.File
import java.time.LocalDateTime

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.max

import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.processing.util.Auditor

/**
 * Deduplicate Example to show how to use CarbonData to avoid loading duplicate record
 * by using Merge syntax
 *
 * The schema of target table:
 * (id int, value string, source_table string, mdt timestamp)
 *
 * The schema of change table:
 * (id int, value string, change_type string, mdt timestamp)
 *
 * This example will insert change table into target table without duplicated record
 */
// scalastyle:off println
object DedupExample {

  case class Target (id: Int, value: String, source_table: String, mdt: String)

  case class Change (id: Int, value: String, change_type: String, mdt: String)

  // number of records for target table before start CDC
  val numInitialRows = 10

  // number of records to insert for every batch
  val numInsertPerBatch = 3

  // number of duplicated records to update for every batch
  val numDuplicatePerBatch = 4

  // number of batch to simulate CDC
  val numBatch = 2

  // print result or not to console
  val printDetail = true

  val names = Seq("Amy", "Bob", "Lucy", "Roy", "Tony", "Mick", "Henry", "Michael", "Carly",
    "Emma", "Jade", "Josh", "Sue", "Ben", "Dale", "Chris", "Grace", "Emily")

  private def pickName = names(Random.nextInt(names.size))

  // IDs in the target table
  private val currentIds = new java.util.ArrayList[Int]()
  private def addId(id: Int) = currentIds.add(id)
  private def maxId: Int = currentIds.asScala.max

  private val INSERT = "I"

  // generate data to insert to the target table
  private def generateRowsForInsert(sparkSession: SparkSession) = {
    // make some duplicated id by maxId - 2
    val insertRows = (maxId - 2 to maxId + numInsertPerBatch).map { x =>
      addId(x)
      Change(x, pickName, INSERT, LocalDateTime.now().toString)
    }
    val insertData = sparkSession.createDataFrame(insertRows)

    // make more duplicated records
    val duplicatedData = insertData.union(insertData)
    duplicatedData.write
      .format("carbondata")
      .option("tableName", "change")
      .mode(SaveMode.Overwrite)
      .save()
  }

  // generate initial data for target table
  private def generateTarget(sparkSession: SparkSession) = {
    val insertRows = (1 to numInitialRows).map { x =>
      addId(x)
      Target(x, pickName, "table1", LocalDateTime.now().toString)
    }
    val targetData = sparkSession.createDataFrame(insertRows)
    targetData.write
      .format("carbondata")
      .option("tableName", "target")
      .mode(SaveMode.Overwrite)
      .save()
  }

  private def readTargetData(sparkSession: SparkSession): Dataset[Row] =
    sparkSession.read
      .format("carbondata")
      .option("tableName", "target")
      .load()

  private def readChangeData(sparkSession: SparkSession): Dataset[Row] =
    sparkSession.read
      .format("carbondata")
      .option("tableName", "change")
      .load()

  private def printChange(spark: SparkSession, i: Int) = {
    if (printDetail) {
      println(s"Insert batch$i")
      spark.sql("select * from change").show(100, false)
    }
  }

  private def printTarget(spark: SparkSession, i: Int) = {
    if (printDetail) {
      println(s"target table after insert batch$i")
      spark.sql("select * from target order by id").show(false)
    }
  }

  private def printTarget(spark: SparkSession) = {
    if (printDetail) {
      println("## target table")
      spark.sql("select * from target").show(100, false)
    }
  }

  private def createSession = {
    import org.apache.spark.sql.CarbonSession._
    val rootPath = new File(this.getClass.getResource("/").getPath + "../../../..").getCanonicalPath
    val spark = SparkSession
      .builder()
      .master("local[8]")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", s"$rootPath/examples/spark/target/warehouse")
      .getOrCreateCarbonSession()
    spark.sparkContext.setLogLevel("error")
    spark
  }

  def main(args: Array[String]): Unit = {
    CarbonProperties.setAuditEnabled(false);

    val spark: SparkSession = createSession

    spark.sql("drop table if exists target")
    spark.sql("drop table if exists change")

    // prepare target data
    generateTarget(spark)

    printTarget(spark)

    (1 to numBatch).foreach { i =>
      // prepare for change data
      generateRowsForInsert(spark)

      printChange(spark, i)

      // apply Change to history table by using MERGE
      dedupAndInsert(spark)

      printTarget(spark, i)
    }

    spark.close()
  }

  /**
   * Leveraging carbon's Merge syntax to perform data deduplication
   */
  private def dedupAndInsert(spark: SparkSession) = {
    import org.apache.spark.sql.CarbonSession._

    // find the latest value for each key
    val latestChangeForEachKey = readChangeData(spark)
      .selectExpr("id", "struct(mdt, value, change_type) as otherCols" )
      .groupBy("id")
      .agg(max("otherCols").as("latest"))
      .selectExpr("id", "latest.*")

    val target = readTargetData(spark)
    target.as("A")
      .merge(latestChangeForEachKey.as("B"), "A.id = B.id")
      .whenNotMatched()
      .insertExpr(
        Map("id" -> "B.id", "value" -> "B.value", "source_table" -> "'table1'", "mdt" -> "B.mdt"))
      .execute()
  }

}
// scalastyle:on println
