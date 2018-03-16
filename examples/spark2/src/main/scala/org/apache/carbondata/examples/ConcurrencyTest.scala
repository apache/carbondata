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
import java.util
import java.util.concurrent.{Callable, Executors, Future, TimeUnit}

import scala.util.Random

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}

// scalastyle:off println
object ConcurrencyTest {

  var totalNum = 100 * 1000 * 1000
  var ThreadNum = 16
  var TaskNum = 100
  var ResultIsEmpty = true
  val cardinalityId = 10000 * 10000
  val cardinalityCity = 6

  def parquetTableName: String = "comparetest_parquet"

  def orcTableName: String = "comparetest_orc"

  def carbonTableName(version: String): String = s"comparetest_carbonV$version"

  // Table schema:
  // +-------------+-----------+-------------+-------------+------------+
  // | id          | string    | 100,000,000 | dimension   | no         |
  // +-------------+-----------+-------------+-------------+------------+
  // | Column name | Data type | Cardinality | Column type | Dictionary |
  // +-------------+-----------+-------------+-------------+------------+
  // | city        | string    | 6           | dimension   | yes        |
  // +-------------+-----------+-------------+-------------+------------+
  // | country     | string    | 6           | dimension   | yes        |
  // +-------------+-----------+-------------+-------------+------------+
  // | planet      | string    | 100,007     | dimension   | yes        |
  // +-------------+-----------+-------------+-------------+------------+
  // | m1          | short     | NA          | measure     | no         |
  // +-------------+-----------+-------------+-------------+------------+
  // | m2          | int       | NA          | measure     | no         |
  // +-------------+-----------+-------------+-------------+------------+
  // | m3          | big int   | NA          | measure     | no         |
  // +-------------+-----------+-------------+-------------+------------+
  // | m4          | double    | NA          | measure     | no         |
  // +-------------+-----------+-------------+-------------+------------+
  // | m5          | decimal   | NA          | measure     | no         |
  // +-------------+-----------+-------------+-------------+------------+

  private def generateDataFrame(spark: SparkSession): DataFrame = {
    val rdd = spark.sparkContext
      .parallelize(1 to totalNum, 4)
      .map { x =>
        ((x % 100000000).toString, "city" + x % 6, "country" + x % 6, "planet" + x % 10007,
          (x % 16).toShort, x / 2, (x << 1).toLong, x.toDouble / 13,
          BigDecimal.valueOf(x.toDouble / 11))
      }.map { x =>
      Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9)
    }

    val schema = StructType(
      Seq(
        StructField("id", StringType, nullable = false),
        StructField("city", StringType, nullable = false),
        StructField("country", StringType, nullable = false),
        StructField("planet", StringType, nullable = false),
        StructField("m1", ShortType, nullable = false),
        StructField("m2", IntegerType, nullable = false),
        StructField("m3", LongType, nullable = false),
        StructField("m4", DoubleType, nullable = false),
        StructField("m5", DecimalType(30, 10), nullable = false)
      )
    )

    spark.createDataFrame(rdd, schema)
  }

  // performance test queries, they are designed to test various data access type
  val r = new Random()
  val tmpId = r.nextInt(cardinalityId) % totalNum
  val tmpCity = "city" + (r.nextInt(cardinalityCity) % totalNum)
  val queries: Array[Query] = Array(
    Query(
      "select * from $table" + s" where id = '$tmpId' ",
      "filter scan",
      "filter on high card dimension"
    ),

    Query(
      "select id from $table" + s" where id = '$tmpId' ",
      "filter scan",
      "filter on high card dimension"
    ),

    Query(
      "select * from $table" + s" where city = '$tmpCity' ",
      "filter scan",
      "filter on high card dimension"
    ),

    Query(
      "select city from $table" + s" where city = '$tmpCity' ",
      "filter scan",
      "filter on high card dimension"
    ),

    Query(
      "select country, sum(m1) from $table group by country",
      "aggregate",
      "group by on big data, on medium card column, medium result set,"
    ),

    Query(
      "select country, sum(m1) from $table" +
        s" where id = '$tmpId' group by country",
      "aggregate",
      "group by on big data, on medium card column, medium result set,"
    ),

    Query(
      "select t1.country, sum(t1.m1) from $table t1 join $table t2"
        + s" on t1.id = t2.id where t1.id = '$tmpId' group by t1.country",
      "aggregate",
      "group by on big data, on medium card column, medium result set,"
    )
    ,
    Query(
      "select t2.country, sum(t2.m1) " +
        "from $table t1 join $table t2 join $table t3 " +
        "join $table t4 join $table t5 join $table t6 join $table t7 " +
        s"on t1.id=t2.id and t1.id=t3.id and t1.id=t4.id " +
        s"and t1.id=t5.id and t1.id=t6.id and " +
        s"t1.id=t7.id " +
        s" where t2.id = '$tmpId' " +
        s" group by t2.country",
      "aggregate",
      "group by on big data, on medium card column, medium result set,"
    )
  )

  private def loadParquetTable(spark: SparkSession, input: DataFrame, table: String)
  : Double = time {
    // partitioned by last 1 digit of id column
    val dfWithPartition = input.withColumn("partitionCol", input.col("id").%(10))
    dfWithPartition.write
      .partitionBy("partitionCol")
      .mode(SaveMode.Overwrite)
      .parquet(table)
    spark.read.parquet(table).createOrReplaceTempView(table)
  }

  private def loadOrcTable(spark: SparkSession, input: DataFrame, table: String): Double = time {
    // partitioned by last 1 digit of id column
    input.write
      .mode(SaveMode.Overwrite)
      .orc(table)
    spark.read.orc(table).createOrReplaceTempView(table)
  }

  private def loadCarbonTable(spark: SparkSession, input: DataFrame, tableName: String): Double = {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATA_FILE_VERSION,
      "3"
    )
    spark.sql(s"drop table if exists $tableName")
    time {
      input.write
        .format("carbondata")
        .option("tableName", tableName)
        .option("single_pass", "true")
        .option("dictionary_exclude", "id") // id is high cardinality column
        .option("table_blocksize", "32")
        .mode(SaveMode.Overwrite)
        .save()
    }
  }

  // load data into parquet, carbonV2, carbonV3
  def prepareTable(spark: SparkSession, table1: String, table2: String): Unit = {
    val df = generateDataFrame(spark).cache
    println(s"generating ${df.count} records, schema: ${df.schema}")
    val table1Time = if (table1.endsWith("parquet")) {
      loadParquetTable(spark, df, table1)
    } else if (table1.endsWith("orc")) {
      loadOrcTable(spark, df, table1)
    } else {
      sys.error("invalid table: " + table1)
    }
    val table2Time = loadCarbonTable(spark, df, table2)
    println(s"load completed, time: $table1Time, $table2Time")
    df.unpersist()
  }

  // Run all queries for the specified table
  private def runQueries(spark: SparkSession, tableName: String): Unit = {
    println(s"start running queries for $tableName...")
    val start = System.currentTimeMillis()
    println("90% time: xx.xx sec\t99% time: xx.xx sec\tlast time: xx.xx sec\t " +
      "running query sql\taverage time: xx.xx sec\t result: show it when ResultIsEmpty is false")
    queries.zipWithIndex.map { case (query, index) =>
      val sqlText = query.sqlText.replace("$table", tableName)

      val executorService = Executors.newFixedThreadPool(ThreadNum)
      val tasks = new util.ArrayList[Callable[Results]]()

      for (num <- 1 to TaskNum) {
        tasks.add(new QueryTask(spark, sqlText))
      }
      val results = executorService.invokeAll(tasks)

      val sql = s"query ${index + 1}: $sqlText "
      printResult(results, sql)
      executorService.shutdown()
      executorService.awaitTermination(600, TimeUnit.SECONDS)

      val taskTime = (System.currentTimeMillis() - start).toDouble / 1000
      println("task time: " + taskTime.formatted("%.3f") + " s")
    }
  }

  def printResult(results: util.List[Future[Results]], sql: String = "") {
    val timeArray = new Array[Double](results.size())
    val sqlResult = results.get(0).get().sqlResult
    for (i <- 0 until results.size()) {
      results.get(i).get()
    }
    for (i <- 0 until results.size()) {
      timeArray(i) = results.get(i).get().time
    }
    val sortTimeArray = timeArray.sorted

    // the time of 90 percent sql are finished
    val time90 = ((sortTimeArray.length) * 0.9).toInt - 1
    // the time of 99 percent sql are finished
    val time99 = ((sortTimeArray.length) * 0.99).toInt - 1
    print("90%:" + sortTimeArray(time90).formatted("%.3f") + " s," +
      "\t99%:" + sortTimeArray(time99).formatted("%.3f") + " s," +
      "\tlast:" + sortTimeArray.last.formatted("%.3f") + " s," +
      "\t" + sql +
      "\taverage:" + (timeArray.sum / timeArray.length).formatted("%.3f") + " s," +
      "\t" + sqlResult.mkString(",") + "\t")
  }

  case class Results(time: Double, sqlResult: Array[Row])


  class QueryTask(spark: SparkSession, query: String)
    extends Callable[Results] with Serializable {
    override def call(): Results = {
      var result: Array[Row] = null
      val rt = time {
        result = spark.sql(query).head(1)
      }
      if (ResultIsEmpty) {
        Results(rt, Array.empty[Row])
      } else {
        Results(rt, result)
      }
    }
  }

  // run testcases and print comparison result
  def runTest(spark: SparkSession, table1: String, table2: String): Unit = {
    // run queries on parquet and carbon
    runQueries(spark, table1)
    // do GC and sleep for some time before running next table
    System.gc()
    Thread.sleep(1000)
    System.gc()
    Thread.sleep(1000)
    runQueries(spark, table2)
  }

  def time(code: => Unit): Double = {
    val start = System.currentTimeMillis()
    code
    // return time in second
    (System.currentTimeMillis() - start).toDouble / 1000
  }

  def initParameters(arr: Array[String]): Unit = {
    if (arr.length > 0) {
      totalNum = arr(0).toInt
    }
    if (arr.length > 1) {
      ThreadNum = arr(1).toInt
    }
    if (arr.length > 2) {
      TaskNum = arr(2).toInt
    }
    if (arr.length > 3) {
      ResultIsEmpty = if (arr(3).equalsIgnoreCase("true")) {
        true
      } else if (arr(3).equalsIgnoreCase("false")) {
        true
      } else {
        throw new Exception("error parameter, should be true or false")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    CarbonProperties.getInstance()
      .addProperty("carbon.enable.vector.reader", "true")
      .addProperty("enable.unsafe.sort", "true")
      .addProperty("carbon.blockletgroup.size.in.mb", "32")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
    import org.apache.spark.sql.CarbonSession._
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"

    val spark = SparkSession
      .builder()
      .master("local[8]")
      .enableHiveSupport()
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreateCarbonSession(storeLocation)
    spark.sparkContext.setLogLevel("warn")

    initParameters(args)

    val table1 = parquetTableName
    val table2 = carbonTableName("3")
    prepareTable(spark, table1, table2)
    println("totalNum:" + totalNum + "\tThreadNum:" + ThreadNum +
      "\tTaskNum:" + TaskNum + "\tResultIsEmpty:" + ResultIsEmpty)
    runTest(spark, table1, table2)

    CarbonUtil.deleteFoldersAndFiles(new File(table1))
    spark.sql(s"drop table $table2")
    spark.close()
  }
}

// scalastyle:on println
