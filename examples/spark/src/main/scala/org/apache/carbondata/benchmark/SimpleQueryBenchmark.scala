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
import java.util.Date

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.spark.util.DataGenerator



// scalastyle:off println
object SimpleQueryBenchmark {

  def parquetTableName: String = "comparetest_parquet"
  def orcTableName: String = "comparetest_orc"
  def carbonTableName(version: String): String = s"comparetest_carbonV$version"

  // performance test queries, they are designed to test various data access type
  val queries: Array[Query] = Array(
    // ===========================================================================
    // ==                     FULL SCAN AGGREGATION                             ==
    // ===========================================================================
    Query(
      "select sum(m1) from $table",
      "full scan",
      "full scan query, 1 aggregate"
    ),
    Query(
      "select sum(m1), sum(m2) from $table",
      "full scan",
      "full scan query, 2 aggregate"
    ),
    Query(
      "select sum(m1), sum(m2), sum(m3) from $table",
      "full scan",
      "full scan query, 3 aggregate"
    ),
    Query(
      "select sum(m1), sum(m2), sum(m3), sum(m4) from $table",
      "full scan",
      "full scan query, 4 aggregate"
    ),
    Query(
      "select sum(m1), sum(m2), sum(m3), sum(m4), avg(m5) from $table",
      "full scan",
      "full scan query, 5 aggregate"
    ),
    Query(
      "select count(distinct id) from $table",
      "full scan",
      "full scan and count distinct of high card column"
    ),
    Query(
      "select count(distinct country) from $table",
      "full scan",
      "full scan and count distinct of medium card column"
    ),
    Query(
      "select count(distinct city) from $table",
      "full scan",
      "full scan and count distinct of low card column"
    ),
    // ===========================================================================
    // ==                      FULL SCAN GROUP BY AGGREGATE                     ==
    // ===========================================================================
    Query(
      "select country, sum(m1) as metric from $table group by country order by metric",
      "aggregate",
      "group by on big data, on medium card column, medium result set,"
    ),
    Query(
      "select city, sum(m1) as metric from $table group by city order by metric",
      "aggregate",
      "group by on big data, on low card column, small result set,"
    ),
    Query(
      "select id, sum(m1) as metric from $table group by id order by metric desc limit 100",
      "topN",
      "top N on high card column"
    ),
    Query(
      "select country,sum(m1) as metric from $table group by country order by metric desc limit 10",
      "topN",
      "top N on medium card column"
    ),
    Query(
      "select city,sum(m1) as metric from $table group by city order by metric desc limit 10",
      "topN",
      "top N on low card column"
    ),
    // ===========================================================================
    // ==                  FILTER SCAN GROUP BY AGGREGATION                     ==
    // ===========================================================================
    Query(
      "select country, sum(m1) as metric from $table where city='city8' group by country " +
          "order by metric",
      "filter scan and aggregate",
      "group by on large data, small result set"
    ),
    Query(
      "select id, sum(m1) as metric from $table where planet='planet10' group by id " +
          "order by metric",
      "filter scan and aggregate",
      "group by on medium data, large result set"
    ),
    Query(
      "select city, sum(m1) as metric from $table where country='country12' group by city " +
          "order by metric",
      "filter scan and aggregate",
      "group by on medium data, small result set"
    ),
    // ===========================================================================
    // ==                             FILTER SCAN                               ==
    // ===========================================================================
    Query(
      "select * from $table where city = 'city3' limit 10000",
      "filter scan",
      "filter on low card dimension, limit, medium result set, fetch all columns"
    ),
    Query(
      "select * from $table where country = 'country9' ",
      "filter scan",
      "filter on low card dimension, medium result set, fetch all columns"
    ),
    Query(
      "select * from $table where planet = 'planet101' ",
      "filter scan",
      "filter on medium card dimension, small result set, fetch all columns"
    ),
    Query(
      "select * from $table where id = '408938' ",
      "filter scan",
      "filter on high card dimension"
    ),
    Query(
      "select * from $table where country='country10000'  ",
      "filter scan",
      "filter on low card dimension, not exist"
    ),
    Query(
      "select * from $table where country='country2' and city ='city8' ",
      "filter scan",
      "filter on 2 dimensions, small result set, fetch all columns"
    ),
    Query(
      "select * from $table where city='city1' and country='country2' and planet ='planet3' ",
      "filter scan",
      "filter on 3 dimensions, small result set, fetch all columns"
    ),
    Query(
      "select * from $table where m1 < 3",
      "filter scan",
      "filter on measure, small result set, fetch all columns"
    ),
    Query(
      "select * from $table where id like '1%' ",
      "fuzzy filter scan",
      "like filter, big result set"
    ),
    Query(
      "select * from $table where id like '%111'",
      "fuzzy filter scan",
      "like filter, medium result set"
    ),
    Query(
      "select * from $table where id like 'xyz%' ",
      "fuzzy filter scan",
      "like filter, full scan but not exist"
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
      "V3"
    )
    spark.sql(s"drop table if exists $tableName")
    time {
      input.write
          .format("carbondata")
          .option("tableName", tableName)
          .option("table_blocksize", "32")
          .mode(SaveMode.Overwrite)
          .save()
    }
  }

  // load data into parquet, carbonV2, carbonV3
  private def prepareTable(spark: SparkSession, table1: String, table2: String): Unit = {
    val df = DataGenerator.generateDataFrame(spark, totalNum = 10 * 10 * 1000).cache
    println(s"loading ${df.count} records, schema: ${df.schema}")
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
  private def runQueries(spark: SparkSession, tableName: String): Array[(Double, Array[Row])] = {
    println(s"start running queries for $tableName...")
    var result: Array[Row] = null
    queries.zipWithIndex.map { case (query, index) =>
      val sqlText = query.sqlText.replace("$table", tableName)
      print(s"running query ${index + 1}: $sqlText ")
      val rt = time {
        result = spark.sql(sqlText).collect()
      }
      println(s"=> $rt sec")
      (rt, result)
    }
  }

  private def printErrorIfNotMatch(index: Int, table1: String, result1: Array[Row],
      table2: String, result2: Array[Row]): Unit = {
    // check result size instead of result value, because some test case include
    // aggregation on double column which will give different result since carbon
    // records are sorted
    if (result1.length != result2.length) {
      val num = index + 1
      println(s"$table1 result for query $num: ")
      println(s"""${result1.mkString(",")}""")
      println(s"$table2 result for query $num: ")
      println(s"""${result2.mkString(",")}""")
      sys.error(s"result not matching for query $num (${queries(index).desc})")
    }
  }

  // run test cases and print comparison result
  private def runTest(spark: SparkSession, table1: String, table2: String): Unit = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = new Date
    // run queries on parquet and carbon
    val table1Result: Array[(Double, Array[Row])] = runQueries(spark, table1)
    // do GC and sleep for some time before running next table
    System.gc()
    Thread.sleep(1000)
    System.gc()
    Thread.sleep(1000)
    val table2Result: Array[(Double, Array[Row])] = runQueries(spark, table2)
    // check result by comparing output from parquet and carbon
    table1Result.zipWithIndex.foreach { case (result, index) =>
      printErrorIfNotMatch(index, table1, result._2, table2, table2Result(index)._2)
    }
    // print all response time in JSON format, so that it can be analyzed later
    queries.zipWithIndex.foreach { case (query, index) =>
      println("{" +
          s""""query":"${index + 1}", """ +
          s""""$table1 time":${table1Result(index)._1}, """ +
          s""""$table2 time":${table2Result(index)._1}, """ +
          s""""fetched":${table1Result(index)._2.length}, """ +
          s""""type":"${query.queryType}", """ +
          s""""desc":"${query.desc}",  """ +
          s""""date": "${formatter.format(date)}" """ +
          "}"
      )
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
    val storeLocation = s"$rootPath/examples/spark/target/store"
    val master = Option(System.getProperty("spark.master"))
      .orElse(sys.env.get("MASTER"))
      .orElse(Option("local[8]"))

    val spark = SparkSession
        .builder()
        .master(master.get)
        .enableHiveSupport()
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
        .getOrCreate()
    spark.sparkContext.setLogLevel("warn")

    val table1 = parquetTableName
    val table2 = carbonTableName("3")
    prepareTable(spark, table1, table2)
    runTest(spark, table1, table2)

    CarbonUtil.deleteFoldersAndFiles(new File(table1))
    spark.sql(s"drop table if exists $table2")
    spark.close()
  }

  def time(code: => Unit): Double = {
    val start = System.currentTimeMillis()
    code
    // return time in second
    (System.currentTimeMillis() - start).toDouble / 1000
  }
}
// scalastyle:on println
