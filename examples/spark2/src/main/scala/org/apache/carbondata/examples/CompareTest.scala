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

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}

/**
 * A query test case
 * @param sqlText SQL statement
 * @param queryType type of query: scan, filter, aggregate, topN
 * @param desc description of the goal of this test case
 */
case class Query(sqlText: String, queryType: String, desc: String)

// scalastyle:off println
object CompareTest {

  def parquetTableName: String = "parquet"
  def orcTableName: String = "orc"
  def carbonTableName(version: String): String = s"carbonV$version"

  // Table schema:
  // +-------------+-----------+-------------+-------------+------------+
  // | Column name | Data type | Cardinality | Column type | Dictionary |
  // +-------------+-----------+-------------+-------------+------------+
  // | city        | string    | 8           | dimension   | yes        |
  // +-------------+-----------+-------------+-------------+------------+
  // | country     | string    | 1103        | dimension   | yes        |
  // +-------------+-----------+-------------+-------------+------------+
  // | planet      | string    | 10,007      | dimension   | yes        |
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
  // | m5          | decimal   | NA          | measure     | no         |
  // +-------------+-----------+-------------+-------------+------------+
  private def generateDataFrame(spark: SparkSession): DataFrame = {
    val rdd = spark.sparkContext
        .parallelize(1 to 10 * 1000 * 1000, 4)
        .map { x =>
          ("city" + x % 8,
            "country" + x % 1103,
            "planet" + x % 10007,
            "IDENTIFIER" + x.toString,
            (x % 16).toShort,
            x / 2,
            (x << 1).toLong,
            x.toDouble / 13,
            BigDecimal.valueOf(x.toDouble / 11))
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
        StructField("m5", DecimalType(30, 10), nullable = false)
      )
    )

    spark.createDataFrame(rdd, schema)
  }

  // performance test queries, they are designed to test various data access type
  val queries: Array[Query] = Array(
    // ===========================================================================
    // ==                     SINGLE COLUMN FULL SCAN                           ==
    // ===========================================================================
    Query(
      "select max(city) from $table",
      "full scan city",
      "full scan city column"
    ),
    Query(
      "select max(country) from $table",
      "full scan country",
      "full scan country column"
    ),
    Query(
      "select max(planet) from $table",
      "full scan planet",
      "full scan planet column"
    ),
    Query(
      "select max(id) from $table",
      "full scan id",
      "full scan id column"
    ),
    Query(
      "select max(m1) from $table",
      "full scan m1",
      "full scan short column"
    ),
    Query(
      "select max(m2) from $table",
      "full scan m2",
      "full scan int column"
    ),
    Query(
      "select max(m3) from $table",
      "full scan m3",
      "full scan long column"
    ),
    Query(
      "select max(m4) from $table",
      "full scan m4",
      "full scan double column"
    ),
    Query(
      "select max(m5) from $table",
      "full scan m5",
      "full scan decimal column"
    ),
    // ===========================================================================
    // ==                      FULL SCAN GROUP BY AGGREGATE                     ==
    // ===========================================================================
    Query(
      "select country, sum(m1) as metric from $table group by country order by metric",
      "sum(m1) groupby country",
      "group by on big data, on medium card column, medium result set,"
    ),
    Query(
      "select city, sum(m1) as metric from $table group by city order by metric",
      "sum(m1) groupby city",
      "group by on big data, on low card column, small result set,"
    ),
    Query(
      "select id, sum(m1) as metric from $table group by id order by metric desc limit 100",
      "topN sum(m1) groupby id",
      "top N on high card column"
    ),
    Query(
      "select country,sum(m1) as metric from $table group by country order by metric desc limit 10",
      "topN sum(m1) groupby country",
      "top N on medium card column"
    ),
    Query(
      "select city,sum(m1) as metric from $table group by city order by metric desc limit 10",
      "topN sum(m1) groupby city",
      "top N on low card column"
    ),
    Query(
      "select count(distinct id) from $table",
      "count distinct id",
      "count distinct of high card column"
    ),
    Query(
      "select count(distinct country) from $table",
      "count distinct country",
      "count distinct of medium card column"
    ),
    Query(
      "select count(distinct city) from $table",
      "count distinct city",
      "count distinct of low card column"
    ),
    // ===========================================================================
    // ==                  FILTER SCAN GROUP BY AGGREGATION                     ==
    // ===========================================================================
    Query(
      "select country, sum(m1) as metric from $table where city='city8' group by country " +
          "order by metric",
      "filter city groupby country",
      "group by on large data, small result set"
    ),
    Query(
      "select id, sum(m1) as metric from $table where planet='planet10' group by id " +
          "order by metric",
      "filter planet groupby id",
      "group by on medium data, large result set"
    ),
    Query(
      "select city, sum(m1) as metric from $table where country='country12' group by city " +
          "order by metric",
      "filter country groupby city and topN",
      "group by on medium data, small result set"
    ),
    // ===========================================================================
    // ==                             FILTER SCAN                               ==
    // ===========================================================================
    Query(
      "select count(*) from $table where city = 'city3' ",
      "filter city",
      "filter on low card dimension, limit, large result set"
    ),
    Query(
      "select count(*) from $table where city = 'city3' limit 10000",
      "filter city",
      "filter on low card dimension, limit, medium result set"
    ),
    Query(
      "select count(*) from $table where country = 'country9' ",
      "filter country",
      "filter on low card dimension, medium result set"
    ),
    Query(
      "select count(*) from $table where planet = 'planet101' ",
      "filter planet",
      "filter on medium card dimension, small result set"
    ),
    Query(
      "select count(*) from $table where id = 'IDENTIFIER408938' ",
      "filter id",
      "filter on high card dimension, small result set"
    ),
    Query(
      "select count(*) from $table where country='country10000'  ",
      "filter country",
      "filter on low card dimension, not exist"
    ),
    Query(
      "select count(*) from $table where country='country2' and city ='city8' ",
      "filter 2 columns",
      "filter on 2 dimensions, small result set"
    ),
    Query(
      "select count(*) from $table where city='city1' and country='country2' and planet ='planet3' ",
      "filter 3 columns",
      "filter on 3 dimensions, small result set"
    ),
    Query(
      "select count(*) from $table where m1 < 3",
      "filter short measure",
      "filter on measure, small result set"
    ),
    Query(
      "select count(*) from $table where m5 < 100.0",
      "filter decimal measure",
      "filter on measure, small result set"
    ),
    // ===========================================================================
    // ==                    FUZZY MATCH FILTER SCAN                            ==
    // ===========================================================================
    Query(
      "select count(*) from $table where id like 'IDENTIFIER1%' ",
      "fuzzy filter id, IDENTIFIER1%",
      "like filter, big result set"
    ),
    Query(
      "select count(*) from $table where id like '%111'",
      "fuzzy filter id %111",
      "like filter, medium result set"
    ),
    Query(
      "select count(*) from $table where id like 'xyz%' ",
      "fuzzy filter id, xyz%",
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
          .option("single_pass", "true")
          .option("table_blocksize", "128")
          .mode(SaveMode.Overwrite)
          .save()
    }
  }

  // load data into parquet, carbonV2, carbonV3
  private def prepareTable(spark: SparkSession, table1: String, table2: String): Unit = {
    val df = generateDataFrame(spark).cache
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
      println(s"=> $rt sec, ${result.size} rows")
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

  // run testcases and print comparison result
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
      println(
          s"${index + 1}:" +
          s"""$table1:${table1Result(index)._1.formatted("%.2f")}, """ +
          s"""$table2:${table2Result(index)._1.formatted("%.2f")}, """ +
          s"""rs:${table1Result(index)._2.length}, """ +
          s"""q:"${query.queryType}" """
      )
    }
  }

  def main(args: Array[String]): Unit = {
    CarbonProperties.getInstance()
        .addProperty("carbon.enable.vector.reader", "true")
        .addProperty("enable.unsafe.sort", "true")
        .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_LOADING, "true")
    import org.apache.spark.sql.CarbonSession._
    val rootPath = new File(this.getClass.getResource("/").getPath
        + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val spark = SparkSession
        .builder()
        .master("local")
        .enableHiveSupport()
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreateCarbonSession(storeLocation)
    spark.sparkContext.setLogLevel("warn")

    val table1 = parquetTableName
    val table2 = carbonTableName("3")

    // load data
    prepareTable(spark, table1, table2)

    // uncomment it if want to skip loading
    // spark.read.parquet(table1).createOrReplaceTempView(table1)

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
