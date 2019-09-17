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
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent.{Callable, Executors, Future, TimeUnit}

import scala.util.Random

import org.apache.spark.sql.{CarbonEnv, DataFrame, Row, SaveMode, SparkSession}

import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonVersionConstants}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.spark.util.DataGenerator

// scalastyle:off println
/**
 * Test concurrent query performance of CarbonData
 *
 * This benchmark will print out some information:
 * 1.Environment information
 * 2.Parameters information
 * 3.concurrent query performance result using parquet format
 * 4.concurrent query performance result using CarbonData format
 *
 * This benchmark default run in local model,
 * user can change 'runInLocal' to false if want to run in cluster,
 * user can change variables like:
 *
 * spark-submit \
 * --class org.apache.carbondata.benchmark.ConcurrentQueryBenchmark \
 * --master  yarn \
 * --deploy-mode client \
 * --driver-memory 16g \
 * --executor-cores 4g \
 * --executor-memory 24g \
 * --num-executors 3  \
 * concurrencyTest.jar \
 * totalNum threadNum taskNum resultIsEmpty runInLocal generateFile
 * deleteFile openSearchMode storeLocation
 * details in initParameters method of this benchmark
 */
object ConcurrentQueryBenchmark {

  // generate number of data
  var totalNum = 10 * 1000 * 1000
  // the number of thread pool
  var threadNum = 16
  // task number of spark sql query
  var taskNum = 100
  // whether is result empty, if true then result is empty
  var resultIsEmpty = true
  // the store path of task details
  var path: String = "/tmp/carbondata"
  // whether run in local or cluster
  var runInLocal = true
  // whether generate new file
  var generateFile = true
  // whether delete file
  var deleteFile = true
  // carbon store location
  var storeLocation = "/tmp"

  val cardinalityId = 100 * 1000 * 1000
  val cardinalityCity = 6

  def parquetTableName: String = "Num" + totalNum + "_" + "comparetest_parquet"

  def orcTableName: String = "Num" + totalNum + "_" + "comparetest_orc"

  def carbonTableName(version: String): String =
    "Num" + totalNum + "_" + s"comparetest_carbonV$version"

  // performance test queries, they are designed to test various data access type
  val r = new Random()
  lazy val tmpId = r.nextInt(cardinalityId) % totalNum
  lazy val tmpCity = "city" + (r.nextInt(cardinalityCity) % totalNum)
  // different query SQL
  lazy val queries: Array[Query] = Array(
    Query(
      "select * from $table" + s" where id = '$tmpId' ",
      "filter scan",
      "filter on high card dimension"
    )
    , Query(
      "select id from $table" + s" where id = '$tmpId' ",
      "filter scan",
      "filter on high card dimension"
    ),
    Query(
      "select city from $table" + s" where id = '$tmpId' ",
      "filter scan",
      "filter on high card dimension"
    ),
    Query(
      "select * from $table" + s" where city = '$tmpCity' limit 100",
      "filter scan",
      "filter on low card dimension, medium result set, fetch all columns"
    ),

    Query(
      "select city from $table" + s" where city = '$tmpCity' limit 100",
      "filter scan",
      "filter on low card dimension"
    ),

    Query(
      "select id from $table" + s" where city = '$tmpCity'  limit 100",
      "filter scan",
      "filter on low card dimension"
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
    ),

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

  /**
   * generate parquet format table
   *
   * @param spark SparkSession
   * @param input DataFrame
   * @param table table name
   * @return the time of generating parquet format table
   */
  private def generateParquetTable(spark: SparkSession, input: DataFrame, table: String)
  : Double = time {
    // partitioned by last 1 digit of id column
    val dfWithPartition = input.withColumn("partitionCol", input.col("id").%(10))
    dfWithPartition.write
      .partitionBy("partitionCol")
      .mode(SaveMode.Overwrite)
      .parquet(table)
  }

  /**
   * generate ORC format table
   *
   * @param spark SparkSession
   * @param input DataFrame
   * @param table table name
   * @return the time of generating ORC format table
   */
  private def generateOrcTable(spark: SparkSession, input: DataFrame, table: String): Double =
    time {
      // partitioned by last 1 digit of id column
      input.write
        .mode(SaveMode.Overwrite)
        .orc(table)
    }

  /**
   * generate carbon format table
   *
   * @param spark     SparkSession
   * @param input     DataFrame
   * @param tableName table name
   * @return the time of generating carbon format table
   */
  private def generateCarbonTable(spark: SparkSession, input: DataFrame, tableName: String)
  : Double = {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATA_FILE_VERSION,
      "3"
    )
    spark.sql(s"drop table if exists $tableName")
    time {
      input.write
        .format("carbondata")
        .option("tableName", tableName)
        .option("tempCSV", "false")
        .option("single_pass", "true")
        .option("dictionary_exclude", "id") // id is high cardinality column
        .option("table_blocksize", "32")
        .mode(SaveMode.Overwrite)
        .save()
    }
  }

  /**
   * load data into parquet, carbonV2, carbonV3
   *
   * @param spark  SparkSession
   * @param table1 table1 name
   * @param table2 table2 name
   */
  def prepareTable(spark: SparkSession, table1: String, table2: String): Unit = {
    val df = if (generateFile) {
      DataGenerator.generateDataFrame(spark, totalNum).cache
    } else {
      null
    }
    val table1Time = time {
      if (table1.endsWith("parquet")) {
        if (generateFile) {
          generateParquetTable(spark, df, storeLocation + "/" + table1)
        }
        spark.read.parquet(storeLocation + "/" + table1).createOrReplaceTempView(table1)
      } else if (table1.endsWith("orc")) {
        if (generateFile) {
          generateOrcTable(spark, df, table1)
          spark.read.orc(table1).createOrReplaceTempView(table1)
        }
      } else {
        sys.error("invalid table: " + table1)
      }
    }
    println(s"$table1 completed, time: $table1Time sec")

    val table2Time: Double = if (generateFile) {
      generateCarbonTable(spark, df, table2)
    } else {
      0.0
    }
    println(s"$table2 completed, time: $table2Time sec")
    if (null != df) {
      df.unpersist()
    }
  }

  /**
   * Run all queries for the specified table
   *
   * @param spark     SparkSession
   * @param tableName table name
   */
  private def runQueries(spark: SparkSession, tableName: String): Unit = {
    println()
    println(s"Start running queries for $tableName...")
    println(
      "Min: min time" +
        "\tMax: max time" +
        "\t90%: 90% time" +
        "\t99%: 99% time" +
        "\tAvg: average time" +
        "\tCount: number of result" +
        "\tQuery X: running different query sql" +
        "\tResult: show it when ResultIsEmpty is false" +
        "\tTotal execute time: total runtime")
    queries.zipWithIndex.map { case (query, index) =>
      val sqlText = query.sqlText.replace("$table", tableName)

      val executorService = Executors.newFixedThreadPool(threadNum)
      val tasks = new java.util.ArrayList[Callable[Results]]()
      val tasksStartTime = System.nanoTime()
      for (num <- 1 to taskNum) {
        tasks.add(new QueryTask(spark, sqlText))
      }
      val results = executorService.invokeAll(tasks)

      executorService.shutdown()
      executorService.awaitTermination(600, TimeUnit.SECONDS)

      val tasksEndTime = System.nanoTime()
      val sql = s"Query ${index + 1}: $sqlText "
      printResults(results, sql, tasksStartTime)
      val taskTime = (tasksEndTime - tasksStartTime).toDouble / (1000 * 1000 * 1000)
      println("Total execute time: " + taskTime.formatted("%.3f") + " s")

      val timeString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
      writeResults(spark, results, sql, tasksStartTime,
        path + s"/${tableName}_query${index + 1}_$timeString")
    }
  }

  /**
   * save the result for subsequent  analysis
   *
   * @param spark    SparkSession
   * @param results  Results
   * @param sql      query sql
   * @param start    tasks start time
   * @param filePath write file path
   */
  def writeResults(
      spark: SparkSession,
      results: java.util.List[Future[Results]],
      sql: String = "",
      start: Long,
      filePath: String): Unit = {
    val timeArray = new Array[(Double, Double, Double)](results.size())
    for (i <- 0 until results.size()) {
      timeArray(i) =
        ((results.get(i).get().startTime - start) / (1000.0 * 1000),
          (results.get(i).get().endTime - start) / (1000.0 * 1000),
          (results.get(i).get().endTime - results.get(i).get().startTime) / (1000.0 * 1000))
    }
    val timeArraySorted = timeArray.sortBy(x => x._1)
    val timeArrayString = timeArraySorted.map { e =>
      e._1.formatted("%.3f") + ",\t" + e._2.formatted("%.3f") + ",\t" + e._3.formatted("%.3f")
    }
    val saveArray = Array(sql, "startTime, endTime, runtime, measure time by the microsecond",
      s"${timeArrayString.length}")
      .union(timeArrayString)
    val rdd = spark.sparkContext.parallelize(saveArray, 1)
    rdd.saveAsTextFile(filePath)
  }

  /**
   * print out results
   *
   * @param results        Results
   * @param sql            query sql
   * @param tasksStartTime tasks start time
   */
  def printResults(results: util.List[Future[Results]], sql: String = "", tasksStartTime: Long) {
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
    print(
      "Min: " + sortTimeArray.head.formatted("%.3f") + " s," +
        "\tMax: " + sortTimeArray.last.formatted("%.3f") + " s," +
        "\t90%: " + sortTimeArray(time90).formatted("%.3f") + " s," +
        "\t99%: " + sortTimeArray(time99).formatted("%.3f") + " s," +
        "\tAvg: " + (timeArray.sum / timeArray.length).formatted("%.3f") + " s," +
        "\t\tCount: " + results.get(0).get.count +
        "\t\t\t\t" + sql +
        "\t" + sqlResult.mkString(",") + "\t")
  }

  /**
   * save result after finishing each task/thread
   *
   * @param time      each task time of executing query sql  and with millis time
   * @param sqlResult query sql result
   * @param count     result count
   * @param startTime task start time with nano time
   * @param endTime   task end time with nano time
   */
  case class Results(
      time: Double,
      sqlResult: Array[Row],
      count: Int,
      startTime: Long,
      endTime: Long)


  class QueryTask(spark: SparkSession, query: String)
    extends Callable[Results] with Serializable {
    override def call(): Results = {
      var result: Array[Row] = null
      val startTime = System.nanoTime()
      val rt = time {
        result = spark.sql(query).collect()
      }
      val endTime = System.nanoTime()
      if (resultIsEmpty) {
        Results(rt, Array.empty[Row], count = result.length, startTime, endTime)
      } else {
        Results(rt, result, count = result.length, startTime, endTime)
      }
    }
  }

  /**
   * run testcases and print comparison result
   *
   * @param spark  SparkSession
   * @param table1 table1 name
   * @param table2 table2 name
   */
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

  /**
   * the time of running code
   *
   * @param code the code
   * @return the run time
   */
  def time(code: => Unit): Double = {
    val start = System.currentTimeMillis()
    code
    // return time in second
    (System.currentTimeMillis() - start).toDouble / 1000
  }

  /**
   * init parameters
   *
   * @param arr parameters
   */
  def initParameters(arr: Array[String]): Unit = {
    if (arr.length > 0) {
      totalNum = arr(0).toInt
    }
    if (arr.length > 1) {
      threadNum = arr(1).toInt
    }
    if (arr.length > 2) {
      taskNum = arr(2).toInt
    }
    if (arr.length > 3) {
      resultIsEmpty = if (arr(3).equalsIgnoreCase("true")) {
        true
      } else if (arr(3).equalsIgnoreCase("false")) {
        false
      } else {
        throw new Exception("error parameter, should be true or false")
      }
    }
    if (arr.length > 4) {
      path = arr(4)
    }
    if (arr.length > 5) {
      runInLocal = if (arr(5).equalsIgnoreCase("true")) {
        val rootPath = new File(this.getClass.getResource("/").getPath
          + "../../../..").getCanonicalPath
        storeLocation = s"$rootPath/examples/spark2/target/store"
        true
      } else if (arr(5).equalsIgnoreCase("false")) {
        false
      } else {
        throw new Exception("error parameter, should be true or false")
      }
    }
    if (arr.length > 6) {
      generateFile = if (arr(6).equalsIgnoreCase("true")) {
        true
      } else if (arr(6).equalsIgnoreCase("false")) {
        false
      } else {
        throw new Exception("error parameter, should be true or false")
      }
    }
    if (arr.length > 7) {
      deleteFile = if (arr(7).equalsIgnoreCase("true")) {
        true
      } else if (arr(7).equalsIgnoreCase("false")) {
        false
      } else {
        throw new Exception("error parameter, should be true or false")
      }
    }
    if (arr.length > 8) {
      storeLocation = arr(8)
    }
  }

  /**
   * main method of this benchmark
   *
   * @param args parameters
   */
  def main(args: Array[String]): Unit = {
    CarbonProperties.getInstance()
      .addProperty("carbon.enable.vector.reader", "true")
      .addProperty("enable.unsafe.sort", "true")
      .addProperty("carbon.blockletgroup.size.in.mb", "32")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "false")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION, "false")
    import org.apache.spark.sql.CarbonUtils._

    // 1. initParameters
    initParameters(args)
    val table1 = parquetTableName
    val table2 = carbonTableName("3")
    val parameters = "totalNum: " + totalNum +
      "\tthreadNum: " + threadNum +
      "\ttaskNum: " + taskNum +
      "\tresultIsEmpty: " + resultIsEmpty +
      "\tfile path: " + path +
      "\trunInLocal: " + runInLocal +
      "\tgenerateFile: " + generateFile +
      "\tdeleteFile: " + deleteFile +
      "\tstoreLocation: " + storeLocation

    val spark = if (runInLocal) {
      SparkSession
        .builder()
        .appName(parameters)
        .master("local[8]")
        .enableHiveSupport()
        .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
        .getOrCreate()
    } else {
      SparkSession
        .builder()
        .appName(parameters)
        .enableHiveSupport()
        .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
        .getOrCreate()
    }
    CarbonEnv.getInstance(spark)
    spark.sparkContext.setLogLevel("ERROR")
    println("\nEnvironment information:")
    val env = Array(
      "spark.master",
      "spark.driver.cores",
      "spark.driver.memory",
      "spark.executor.cores",
      "spark.executor.memory",
      "spark.executor.instances")
    env.foreach { each =>
      println(each + ":\t" + spark.conf.get(each, "default value") + "\t")
    }
    println("SPARK_VERSION:" + spark.version + "\t")
    println("CARBONDATA_VERSION:" + CarbonVersionConstants.CARBONDATA_VERSION + "\t")
    println("\nParameters information:")
    println(parameters)

    // 2. prepareTable
    prepareTable(spark, table1, table2)

    // 3. runTest
    runTest(spark, table1, table2)

    if (deleteFile) {
      CarbonUtil.deleteFoldersAndFiles(new File(table1))
      spark.sql(s"drop table $table2")
    }
    spark.close()
  }
}

// scalastyle:on println
