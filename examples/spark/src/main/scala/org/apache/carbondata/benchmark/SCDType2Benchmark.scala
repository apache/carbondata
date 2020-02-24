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
 * Benchmark for SCD (Slowly Change Dimension) Type 2 performance.
 * See [[https://en.wikipedia.org/wiki/Slowly_changing_dimension]]
 *
 * The benchmark shows performance of two update methods:
 * 1. overwrite_solution, which uses INSERT OVERWRITE. This is a popular method for hive warehouse.
 * 2. update_solution, which uses CarbonData's update syntax to update the history table directly.
 *
 * When running in a 8-cores laptop, the benchmark shows:
 *
 * 1. test one
 * History table 1M records, update 10K records everyday and insert 10K records everyday,
 * simulated 3 days.
 * hive_solution: total process time takes 13s
 * carbon_solution: total process time takes 6s
 *
 *
 * 2. test two
 * History table 10M records, update 10K records everyday and insert 10K records everyday,
 * simulated 3 days.
 * hive_solution: total process time takes 115s
 * carbon_solution: total process time takes 15s
 *
 */
// scalastyle:off println
object SCDType2Benchmark {

  // Schema for history table
  // Table name: dw_order_solution1 (using overwrite), dw_order_solution2 (using update)
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

  // Schema for Change table which is used for update to history table every day
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
  case class Change (order_id: String, customer_id: String, update_date: Date, state: Int)

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

  def generateDailyChange(
      sparkSession: SparkSession,
      numUpdatedOrders: Int,
      startDate: Date,
      updateDate: Date,
      newState: Int,
      numNewOrders: Int
      ): DataFrame = {
    import sparkSession.implicits._
    // data for update to the history table
    val ds1 = sparkSession.sparkContext.parallelize(1 to numUpdatedOrders, 4)
      .map {x => Change(s"order$x", s"customer$x", updateDate, newState)
      }.toDS().toDF()
    // date for insert to the history table
    val ds2 = sparkSession.sparkContext.parallelize(1 to numNewOrders, 4)
      .map {x => Change(s"newOrder${System.currentTimeMillis()}", s"customer$x", updateDate, 1)
      }.toDS().toDF()
    // union them so that the Change table contains both data for update and insert
    ds1.union(ds2)
  }

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.CarbonSession._
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath

    val spark = SparkSession
      .builder()
      .master("local[8]")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", s"$rootPath/examples/spark/target/warehouse")
      .getOrCreateCarbonSession()
    spark.sparkContext.setLogLevel("error")

    // prepare base data for first day
    spark.sql("drop table if exists dw_order_solution1")
    spark.sql("drop table if exists dw_order_solution2")
    spark.sql("drop table if exists change")

    val baseData = generateDataForDay0(
        sparkSession = spark,
        numOrders = numOrders,
        startDate = Date.valueOf("2018-05-01"))

    baseData.write
      .format("carbondata")
      .option("tableName", "dw_order_solution1")
      .mode(SaveMode.Overwrite)
      .save()

    baseData.write
      .format("carbondata")
      .option("tableName", "dw_order_solution2")
      .option("sort_columns", "order_id")
      .mode(SaveMode.Overwrite)
      .save()

    var startDate = Date.valueOf("2018-05-01")
    var state = 2
    var solution1UpdateTime = 0L
    var solution2UpdateTime = 0L

    if (printDetail) {
      println("## day0")
      spark.sql("select * from dw_order").show(100, false)
    }

    def timeIt(func: (SparkSession) => Unit): Long = {
      val start = System.nanoTime()
      func(spark)
      System.nanoTime() - start
    }

    for (i <- 1 to numDays) {
      // prepare for incremental update data for day-i
      val newDate = new Date(DateUtils.addDays(startDate, 1).getTime)
      val changeData = generateDailyChange(
        sparkSession = spark,
        numUpdatedOrders = numUpdateOrdersDaily,
        startDate = startDate,
        updateDate = newDate,
        newState = state,
        numNewOrders = newNewOrdersDaily)
      changeData.write
        .format("carbondata")
        .option("tableName", "change")
        .mode(SaveMode.Overwrite)
        .save()

      if (printDetail) {
        println(s"day$i Change")
        spark.sql("select * from change").show(100, false)
      }

      // apply Change to history table by using INSERT OVERWRITE
      solution1UpdateTime += timeIt(solution1)

      // apply Change to history table by using UPDATE
      solution2UpdateTime += timeIt(solution2)

      if (printDetail) {
        println(s"day$i result")
        spark.sql("select * from dw_order_solution1").show(false)
        spark.sql("select * from dw_order_solution2").show(false)
      }

      startDate = newDate
      state = state + 1
    }

    // do a query after apply SCD Type2 update
    val solution1QueryTime = timeIt(
      spark => spark.sql(
      s"""
         | select sum(state) as sum, customer_id
         | from dw_order_solution1
         | group by customer_id
         | order by sum
         | limit 10
         |""".stripMargin).collect()
    )

    val solution2QueryTime = timeIt(
      spark => spark.sql(
        s"""
           | select sum(state) as sum, customer_id
           | from dw_order_solution2
           | group by customer_id
           | order by sum
           | limit 10
           |""".stripMargin).collect()
    )

    // print update time
    println(s"overwrite solution update takes ${solution1UpdateTime / 1000 / 1000 / 1000} s")
    println(s"update solution update takes ${solution2UpdateTime / 1000 / 1000 / 1000} s")

    // print query time
    println(s"overwrite solution query takes ${solution1QueryTime / 1000 / 1000 / 1000} s")
    println(s"update solution query takes ${solution2QueryTime / 1000 / 1000 / 1000} s")

    spark.close()
  }

  /**
   * Typical solution when using hive
   * This solution uses INSERT OVERWRITE to rewrite the whole table every day
   */
  private def solution1(spark: SparkSession) = {
    spark.sql(
      """
        | insert overwrite table dw_order_solution1
        | select * from
        | (
        |   select A.order_id, A.customer_id, A.start_date,
        |     case when A.end_date > B.update_date then B.update_date
        |     else A.end_date
        |     end as end_date,
        |   A.state
        |   from dw_order_solution1 A
        |   left join change B
        |   on A.order_id = B.order_id
        |   union all
        |     select B.order_id, B.customer_id, B.update_date, date("9999-01-01"), B.state
        |     from change B
        | ) T
      """.stripMargin)
  }

  /**
   * Solution leveraging carbon's update syntax
   */
  private def solution2(spark: SparkSession) = {
    spark.sql(
      """
        | update dw_order_solution2 A
        | set (A.end_date) =
        |   (select B.update_date
        |   from change B
        |   where A.order_id = B.order_id and A.end_date > B.update_date)
      """.stripMargin).show
    spark.sql(
      """
        | insert into dw_order_solution2
        | select B.order_id, B.customer_id, B.update_date, date('9999-12-30'), B.state
        | from change B
      """.stripMargin)
  }
}
// scalastyle:on println
