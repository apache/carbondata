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
import org.apache.spark.sql.functions._

import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.processing.util.Auditor

/**
 * CDC (Change Data Capture) example, it reads input CSV files as Change input file and merge
 * it into a target CarbonData table
 *
 * The schema of target table:
 * (id int, value string, remark string, mdt timestamp)
 *
 * The schema of change table:
 * (id int, value string, change_type string, mdt timestamp)
 *
 * change_type can be I/U/D
 *
 * This example generate N number batch of change data and merge them into target table
 */
// scalastyle:off println
object CDCExample {

  case class Target (id: Int, value: String, remark: String, mdt: String)

  case class Change (id: Int, value: String, change_type: String, mdt: String)

  // User can set it to "carbon" or "hive"
  // If "carbon" is set, will use CarbonData's MERGE to perform CDC
  // If "hive" is set, will use INSERT OVERWRITE to perform CDC
  private val solution = "carbon"

  // print result or not to console for debugging
  private val printDetail = false

  // number of records for target table before start CDC
  private val numInitialRows = 100000

  // number of records to insert for every batch
  private val numInsertPerBatch = 1000

  // number of records to update for every batch
  private val numUpdatePerBatch = 9000

  // number of records to delete for every batch
  private val numDeletePerBatch = 1000

  // number of batch to simulate CDC
  private val numBatch = 10

  private val random = new Random()

  // generate 100 random strings
  private val values =
    (1 to 100).map { x =>
      // to simulate a wide target table, make a relatively long string for value
      random.nextString(100)
    }

  // pick one value randomly
  private def pickValue = values(random.nextInt(values.size))

  // IDs in the target table
  private val currentIds = new java.util.ArrayList[Int](numInitialRows * 2)
  private def getId(index: Int) = currentIds.get(index)
  private def getAndRemoveId(index: Int) = currentIds.remove(index)
  private def addId(id: Int) = currentIds.add(id)
  private def removeId(index: Int) = currentIds.remove(index)
  private def numOfIds = currentIds.size
  private def maxId: Int = currentIds.asScala.max

  private val INSERT = "I"
  private val UPDATE = "U"
  private val DELETE = "D"

  // generate change data for insert
  private def generateRowsForInsert(sparkSession: SparkSession) = {
    // data for insert to the target table
    val insertRows = (maxId + 1 to maxId + numInsertPerBatch).map { x =>
      addId(x)
      Change(x, pickValue, INSERT, LocalDateTime.now().toString)
    }
    sparkSession.createDataFrame(insertRows)
  }

  // generate change data for delete
  private def generateRowsForDelete(sparkSession: SparkSession) = {
    val deletedRows = (1 to numDeletePerBatch).map { x =>
      val idIndex = random.nextInt(numOfIds)
      Change(getAndRemoveId(idIndex), "", DELETE, LocalDateTime.now().toString)
    }
    sparkSession.createDataFrame(deletedRows)
  }

  // generate change data for update
  private def generateRowsForUpdate(sparkSession: SparkSession) = {
    val updatedRows = (1 to numUpdatePerBatch).map { x =>
      val idIndex = random.nextInt(numOfIds)
      Change(getId(idIndex), pickValue, UPDATE, LocalDateTime.now().toString)
    }
    sparkSession.createDataFrame(updatedRows)
  }

  // generate initial data for target table
  private def generateTarget(sparkSession: SparkSession): Unit = {
    print("generating target table...")
    val time = timeIt { () =>
      val insertRows = (1 to numInitialRows).map { x =>
        addId(x)
        Target(x, pickValue, "origin", LocalDateTime.now().toString)
      }
      // here we insert duplicated rows to simulate non primary key table (which has repeated id)
      val duplicatedRow = insertRows.union(insertRows)
      val targetData = sparkSession.createDataFrame(duplicatedRow)
      targetData.repartition(8)
        .write
        .format("carbondata")
        .option("tableName", "target")
        .option("sort_scope", "global_sort")
        .option("sort_column", "id")
        .mode(SaveMode.Overwrite)
        .save()
    }
    println(s"done! ${timeFormatted(time)}")
  }

  // generate change data
  private def generateChange(sparkSession: SparkSession): Unit = {
    val update = generateRowsForUpdate(sparkSession)
    val delete = generateRowsForDelete(sparkSession)
    val insert = generateRowsForInsert(sparkSession)

    // union them so that the change contains IUD
    update
      .union(delete)
      .union(insert)
      .repartition(8)
      .write
      .format("carbondata")
      .option("tableName", "change")
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

  private def timeIt(func: () => Unit): Long = {
    val start = System.nanoTime()
    func()
    System.nanoTime() - start
  }

  private def timeFormatted(updateTime: Long) = {
    (updateTime.asInstanceOf[Double] / 1000 / 1000 / 1000).formatted("%.2f") + " s"
  }

  private def printTarget(spark: SparkSession, i: Int) = {
    if (printDetail) {
      println(s"target table after CDC batch$i")
      spark.sql("select * from target order by id").show(false)
    }
  }

  private def printChange(spark: SparkSession, i: Int) = {
    if (printDetail) {
      println(s"CDC batch$i")
      spark.sql("select * from change").show(100, false)
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
    spark
  }

  def main(args: Array[String]): Unit = {
    CarbonProperties.setAuditEnabled(false);
    val spark = createSession
    spark.sparkContext.setLogLevel("error")

    println(s"start CDC example using $solution solution")
    spark.sql("drop table if exists target")
    spark.sql("drop table if exists change")

    // prepare target table
    generateTarget(spark)

    if (printDetail) {
      println("## target table")
      spark.sql("select * from target").show(100, false)
    }

    var updateTime = 0L

    // Do CDC for N batch
    (1 to numBatch).foreach { i =>
      // prepare for change data
      generateChange(spark)

      printChange(spark, i)

      // apply change to target table
      val time = timeIt { () =>
        print(s"applying change batch$i...")
        if (solution.equals("carbon")) {
          carbonSolution(spark)
        } else {
          hiveSolution(spark)
        }
      }
      updateTime += time
      println(s"done! ${timeFormatted(time)}")
      printTarget(spark, i)
    }

    // do a query after all changes to compare query time
    val queryTime = timeIt {
      () => spark.sql("select * from target").collect()
    }

    // print update time
    println(s"total update takes ${timeFormatted(updateTime)}")

    // print query time
    println(s"total query takes ${timeFormatted(queryTime)}")

    spark.close()
  }

  /**
   * Solution leveraging carbon's Merge syntax to apply change data
   */
  private def carbonSolution(spark: SparkSession) = {
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
      .whenMatched("B.change_type = 'D'")
      .delete()
      .whenMatched("B.change_type = 'U'")
      .updateExpr(
        Map("id" -> "B.id", "value" -> "B.value", "remark" -> "'updated'", "mdt" -> "B.mdt"))
      .whenNotMatched("B.change_type = 'I'")
      .insertExpr(
        Map("id" -> "B.id", "value" -> "B.value", "remark" -> "'new'", "mdt" -> "B.mdt"))
      .execute()
  }

  /**
   * Typical solution when using hive
   * INSERT OVERWRITE to rewrite the whole table/partition for every CDC batch
   */
  private def hiveSolution(spark: SparkSession) = {
    val latestChangeForEachKey = readChangeData(spark)
      .selectExpr("id", "struct(mdt, value, change_type) as otherCols" )
      .groupBy("id")
      .agg(max("otherCols").as("latest"))
      .selectExpr("id", "latest.*")
    latestChangeForEachKey.createOrReplaceTempView("latest_change")
    spark.sql(
      """
        | insert overwrite table target
        | select * from
        | (
        |   select A.id, B.value, 'updated', B.mdt
        |     from target A
        |     right join latest_change B
        |     on A.id = B.id
        |     where B.change_type = 'U'
        |   union all
        |     select B.id, B.value, 'new', B.mdt
        |     from latest_change B
        |     where B.change_type = 'I'
        |   union all
        |     select A.id, A.value, A.remark, A.mdt
        |     from target A
        |     left join latest_change B
        |     on A.id = B.id
        |     where B.id is null
        | ) T
      """.stripMargin)
  }
}
// scalastyle:on println
