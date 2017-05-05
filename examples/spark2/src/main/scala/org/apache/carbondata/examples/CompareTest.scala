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
import java.text.SimpleDateFormat
import java.util.Date

import scala.util.Random

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * A query test case
 * @param sqlText SQL statement
 * @param queryType type of query: scan, filter, aggregate, topN
 * @param desc description of the goal of this test case
 */
case class Query(sqlText: String, queryType: String, desc: String)

// scalastyle:off println
object CompareTest {

  def parquetTableName: String = "comparetest_parquet"
  def carbonTableName(version: String): String = s"comparetest_carbonV$version"

  // Table schema:
  // +-------------+-----------+-------------+-------------+------------+
  // | Column name | Data type | Cardinality | Column type | Dictionary |
  // +-------------+-----------+-------------+-------------+------------+
  // | city        | string    | 8           | dimension   | yes        |
  // +-------------+-----------+-------------+-------------+------------+
  // | country     | string    | 1103        | dimension   | yes        |
  // +-------------+-----------+-------------+-------------+------------+
  // | planet      | string    | 100,007     | dimension   | yes        |
  // +-------------+-----------+-------------+-------------+------------+
  // | id          | string    | 10,000,000  | dimension   | no         |
  // +-------------+-----------+-------------+-------------+------------+
  // | m1          | short     | NA          | measure     | no         |
  // +-------------+-----------+-------------+-------------+------------+
  // | m2          | int       | NA          | measure     | no         |
  // +-------------+-----------+-------------+-------------+------------+
  // | m3          | big int   | NA          | measure     | no         |
  // +-------------+-----------+-------------+-------------+------------+
  // | m4          | double    | NA          | measure     | no         |
  // +-------------+-----------+-------------+-------------+------------+
  // | m5          | double    | NA          | measure     | no         |
  // +-------------+-----------+-------------+-------------+------------+
  private def generateDataFrame(spark: SparkSession): DataFrame = {
    val r = new Random()
    val rdd = spark.sparkContext
        .parallelize(1 to 10 * 1000 * 1000, 4)
        .map { x =>
          ("city" + x % 8, "country" + x % 1103, "planet" + x % 10007, "IDENTIFIER" + x.toString,
          (x % 16).toShort, x / 2, (x << 1).toLong, x.toDouble / 13, x.toDouble / 11)
        }.map { x =>
          Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9)
        }

    val schema = StructType(
      Seq(
        StructField("city", StringType, nullable = false),
        StructField("country", StringType, nullable = false),
        StructField("planet", StringType, nullable = false),
        StructField("id", StringType, nullable = false),
        StructField("m1", ShortType, nullable = false),
        StructField("m2", IntegerType, nullable = false),
        StructField("m3", LongType, nullable = false),
        StructField("m4", DoubleType, nullable = false),
        StructField("m5", DoubleType, nullable = false)
      )
    )

    spark.createDataFrame(rdd, schema)
  }

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
      "select country, sum(m1) from $table group by country",
      "aggregate",
      "group by on big data, on medium card column, medium result set,"
    ),
    Query(
      "select city, sum(m1) from $table group by city",
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
      "select country, sum(m1) from $table where city='city8' group by country ",
      "filter scan and aggregate",
      "group by on large data, small result set"
    ),
    Query(
      "select id, sum(m1) from $table where planet='planet10' group by id",
      "filter scan and aggregate",
      "group by on medium data, large result set"
    ),
    Query(
      "select city, sum(m1) from $table where country='country12' group by city ",
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

  private def loadParquetTable(spark: SparkSession, input: DataFrame): Double = time {
    // partitioned by last 1 digit of id column
    val dfWithPartition = input.withColumn("partitionCol", input.col("id").%(10))
    dfWithPartition.write
        .partitionBy("partitionCol")
        .mode(SaveMode.Overwrite)
        .parquet(parquetTableName)
  }

  private def loadCarbonTable(spark: SparkSession, input: DataFrame, version: String): Double = {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATA_FILE_VERSION,
      version
    )
    spark.sql(s"drop table if exists ${carbonTableName(version)}")
    time {
      input.write
          .format("carbondata")
          .option("tableName", carbonTableName(version))
          .option("tempCSV", "false")
          .option("single_pass", "true")
          .option("dictionary_exclude", "id") // id is high cardinality column
          .option("table_blocksize", "32")
          .mode(SaveMode.Overwrite)
          .save()
    }
  }

  // load data into parquet, carbonV2, carbonV3
  private def prepareTable(spark: SparkSession): Unit = {
    val df = generateDataFrame(spark).cache
    println(s"loading ${df.count} records, schema: ${df.schema}")
    val loadParquetTime = loadParquetTable(spark, df)
    val loadCarbonV3Time = loadCarbonTable(spark, df, version = "3")
    println(s"load completed, time: $loadParquetTime, $loadCarbonV3Time")
    df.unpersist()
    spark.read.parquet(parquetTableName).registerTempTable(parquetTableName)
  }

  // Run all queries for the specified table
  private def runQueries(spark: SparkSession, tableName: String): Array[(Double, Int)] = {
    println(s"start running queries for $tableName...")
    var result: Array[Row] = null
    queries.zipWithIndex.map { case (query, index) =>
      val sqlText = query.sqlText.replace("$table", tableName)
      print(s"running query ${index + 1}: $sqlText ")
      val rt = time {
        result = spark.sql(sqlText).collect()
      }
      println(s"=> $rt sec")
      (rt, result.length)
    }
  }

  // run testcases and print comparison result
  private def runTest(spark: SparkSession): Unit = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = new Date
    val timestamp = date.getTime
    // run queries on parquet and carbon
    val parquetResult: Array[(Double, Int)] = runQueries(spark, parquetTableName)
    // do GC and sleep for some time before running next table
    System.gc()
    Thread.sleep(1000)
    System.gc()
    Thread.sleep(1000)
    val carbonResult: Array[(Double, Int)] = runQueries(spark, carbonTableName("3"))
    // check result by comparing output from parquet and carbon
    parquetResult.zipWithIndex.foreach { case (result, index) =>
      if (result._2 != carbonResult(index)._2) {
        sys.error(s"result not matching for query ${index + 1}: " +
            s"${result._2} and ${carbonResult(index)._2}")
      }
    }
    // print all response time in JSON format, so that it can be analyzed later
    queries.zipWithIndex.foreach { case (query, index) =>
      println("{" +
          s""""query":"${index + 1}", """ +
          s""""parquetTime":${parquetResult(index)._1}, """ +
          s""""carbonTime":${carbonResult(index)._1}, """ +
          s""""fetched":${parquetResult(index)._2}, """ +
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
    import org.apache.spark.sql.CarbonSession._
    val rootPath = new File(this.getClass.getResource("/").getPath
        + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val spark = SparkSession
        .builder()
        .master("local")
        .enableHiveSupport()
        .getOrCreateCarbonSession(storeLocation)
    spark.sparkContext.setLogLevel("warn")

    prepareTable(spark)
    runTest(spark)

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
