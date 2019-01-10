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

package org.apache.carbondata.benchmark

import java.io.File
import java.sql.Date

import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * Benchmark for Change Data Capture scenario.
 * This test simulates updates to history table using CDC table.
 *
 * The benchmark shows performance of two update methods:
 * 1. hive_solution, which uses INSERT OVERWRITE. This is a popular method for hive warehouse.
 * 2. carbon_solution, which uses CarbonData's update syntax to update the history table directly.
 *
 * When running in a 8-cores laptop, the benchmark shows:
 *
 * 1. test one
 * History table 1M records, update 10K records everyday and insert 10K records everyday,
 * simulated 3 days.
 * hive_solution: total process time takes 13,516 ms
 * carbon_solution: total process time takes 7,521 ms
 *
 *
 * 2. test two
 * History table 10M records, update 10K records everyday and insert 10K records everyday,
 * simulated 3 days.
 * hive_solution: total process time takes 104,250 ms
 * carbon_solution: total process time takes 17,384 ms
 *
 */
object CDCBenchmark {

  // Schema for history table
  // Table name: dw_order
  // +-------------+-----------+-------------+
  // | Column name | Data type | Cardinality |
  // +-------------+-----------+-------------+
  // | order_id    | string    | 10,000,000  |
  // +-------------+-----------+-------------+
  // | customer_id | string    | 10,000,000  |
  // +-------------+-----------+-------------+
  // | start_date  | date      | NA          |
  // +-------------+-----------+-------------+
  // | end_date    | date      | NA          |
  // +-------------+-----------+-------------+
  // | state       | int       | 4           |
  // +-------------+-----------+-------------+
  case class Order (order_id: String, customer_id: String, start_date: Date, end_date: Date,
      state: Int)

  // Schema for CDC data which is used for update to history table every day
  // Table name: ods_order
  // +-------------+-----------+-------------+
  // | Column name | Data type | Cardinality |
  // +-------------+-----------+-------------+
  // | order_id    | string    | 10,000,000  |
  // +-------------+-----------+-------------+
  // | customer_id | string    | 10,000,000  |
  // +-------------+-----------+-------------+
  // | update_date | date      | NA          |
  // +-------------+-----------+-------------+
  // | state       | int       | 4           |
  // +-------------+-----------+-------------+
  case class CDC (order_id: String, customer_id: String, update_date: Date, state: Int)

  // number of records for first day
  val numOrders = 10000000

  // number of records to update every day
  val numUpdateOrdersDaily = 10000

  // number of new records to insert every day
  val newNewOrdersDaily = 10000

  // number of days to simulate
  val numDays = 3

  // print eveyday result or not to console
  val printDetail = false

  def generateDataForDay0(
      sparkSession: SparkSession,
      numOrders: Int = 1000000,
      startDate: Date = Date.valueOf("2018-05-01")): DataFrame = {
    import sparkSession.implicits._
    sparkSession.sparkContext.parallelize(1 to numOrders, 4)
      .map { x => Order(s"order$x", s"customer$x", startDate, Date.valueOf("9999-01-01"), 1)
      }.toDS().toDF()
  }

  def generateDailyCDC(
      sparkSession: SparkSession,
      numUpdatedOrders: Int,
      startDate: Date,
      updateDate: Date,
      newState: Int,
      numNewOrders: Int
      ): DataFrame = {
    import sparkSession.implicits._
    val ds1 = sparkSession.sparkContext.parallelize(1 to numUpdatedOrders, 4)
      .map {x => CDC(s"order$x", s"customer$x", updateDate, newState)
      }.toDS().toDF()
    val ds2 = sparkSession.sparkContext.parallelize(1 to numNewOrders, 4)
      .map {x => CDC(s"newOrder${System.currentTimeMillis()}", s"customer$x", updateDate, 1)
      }.toDS().toDF()
    ds1.union(ds2)
  }

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.CarbonSession._
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val master = Option(System.getProperty("spark.master"))
      .orElse(sys.env.get("MASTER"))
      .orElse(Option("local[8]"))

    val spark = SparkSession
      .builder()
      .master(master.get)
      .enableHiveSupport()
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreateCarbonSession(storeLocation)
    spark.sparkContext.setLogLevel("warn")

    spark.sql("drop table if exists dw_order")
    spark.sql("drop table if exists ods_order")

    // prepare base data for first day
    val df = generateDataForDay0(
      sparkSession = spark,
      numOrders = numOrders,
      startDate = Date.valueOf("2018-05-01"))

    spark.sql(s"drop table if exists dw_order")
    df.write
      .format("carbondata")
      .option("tableName", "dw_order")
      .mode(SaveMode.Overwrite)
      .save()

    var startDate = Date.valueOf("2018-05-01")
    var state = 2
    var updateTime = 0L

    if (printDetail) {
      println("## day0")
      spark.sql("select * from dw_order").show(100, false)
    }

    for (i <- 1 to numDays) {
      // prepare for incremental update data for day-i
      val newDate = new Date(DateUtils.addDays(startDate, 1).getTime)
      val cdc = generateDailyCDC(
        sparkSession = spark,
        numUpdatedOrders = numUpdateOrdersDaily,
        startDate = startDate,
        updateDate = newDate,
        newState = state,
        numNewOrders = newNewOrdersDaily)
      cdc.write
        .format("carbondata")
        .option("tableName", "ods_order")
        .mode(SaveMode.Overwrite)
        .save()

      if (printDetail) {
        println(s"day$i CDC")
        spark.sql("select * from ods_order").show(100, false)
      }

      // update dw table using CDC data
      val start = System.nanoTime()
      hive_solution(spark)
      // carbon_solution(spark)
      val end = System.nanoTime()
      updateTime += end - start

      if (printDetail) {
        println(s"day$i result")
        spark.sql("select * from dw_order").show(100, false)
      }

      startDate = newDate
      state = state + 1
    }

    println(s"simulated $numDays days, total process time takes ${updateTime / 1000 / 1000} ms")
    spark.close()
  }

  /**
   * Typical solution when using hive
   * This solution uses INSERT OVERWRITE to rewrite the whole table every day
   */
  private def hive_solution(spark: SparkSession) = {
    spark.sql(
      """
        | insert overwrite table dw_order
        | select * from
        | (
        |   select A.order_id, A.customer_id, A.start_date,
        |     case when A.end_date > B.update_date then B.update_date
        |     else A.end_date
        |     end as end_date,
        |   A.state
        |   from dw_order A
        |   left join ods_order B
        |   on A.order_id = B.order_id
        |   union all
        |   select B.order_id, B.customer_id, B.update_date, date("9999-01-01"),
        |     B.state
        |   from ods_order B
        | ) T
      """.stripMargin)
  }

  /**
   * Solution leveraging carbon's update syntax
   */
  private def carbon_solution(spark: SparkSession) = {
    spark.sql(
      """
        | update dw_order A
        | set (A.end_date) =
        |   (select B.update_date
        |   from ods_order B
        |   where A.order_id = B.order_id and A.end_date > B.update_date)
      """.stripMargin).show
    spark.sql(
      """
        | insert into dw_order
        | select B.order_id, B.customer_id, B.update_date, date('9999-12-30'), B.state
        | from ods_order B
      """.stripMargin)
  }
}
