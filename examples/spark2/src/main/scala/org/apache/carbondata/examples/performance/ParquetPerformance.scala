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

package org.apache.carbondata.examples.performance

import java.io.File

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.CompareTest._

// This is used to capture the performance of Parquet on Spark.

object ParquetPerformance {

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

  private def loadParquetTable(spark: SparkSession, input: DataFrame): Double = time {
    // partitioned by last 1 digit of id column
    val dfWithPartition = input.withColumn("partitionCol", input.col("id").%(10))
    dfWithPartition.write
      .partitionBy("partitionCol")
      .mode(SaveMode.Overwrite)
      .parquet(parquetTableName)
  }

  // load data into parquet
  private def prepareTable(spark: SparkSession): Unit = {
    val df = generateDataFrame(spark).cache
    // scalastyle:off
    println(s"loading ${df.count} records, schema: ${df.schema}")
    val loadParquetTime = loadParquetTable(spark, df)
    val loadCarbonV3Time = loadCarbonTable(spark, df, version = "3")
    println(s"load completed, time: $loadParquetTime, $loadCarbonV3Time")
    // scalastyle:on
    df.unpersist()
    spark.read.parquet(parquetTableName).registerTempTable(parquetTableName)
  }

  // Run all queries for the specified table
  private def runQueries(spark: SparkSession, tableName: String): Array[(Double, Int)] = {
    // scalastyle:off
    println(s"start running queries for $tableName...")
    var result: Array[Row] = null
    queries.zipWithIndex.map { case (query, index) =>
      val sqlText = query.sqlText.replace("$table", tableName)
      print(s"running query ${index + 1}: $sqlText ")
      val rt = time {
        result = spark.sql(sqlText).collect()
      }
      println(s"=> $rt sec")
      // scalastyle:on
      (rt, result.length)
    }
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

  private def runParquetQueries(spark: SparkSession): Array[(Double, Int)] = {
    // run queries on parquet
    val parquetResult: Array[(Double, Int)] = runQueries(spark, parquetTableName)
    // do GC and sleep for some time before running next table
    System.gc()
    Thread.sleep(1000)
    parquetResult
  }

  private def writeResults(content: String, file: String) = {
    scala.tools.nsc.io.File(file).appendAll(content)
  }

  def main(args: Array[String]): Unit = {
    CarbonProperties.getInstance()
      .addProperty("carbon.enable.vector.reader", "true")
      .addProperty("enable.unsafe.sort", "true")
      .addProperty("carbon.blockletgroup.size.in.mb", "32")
    import org.apache.spark.sql.CarbonSession._
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/integration/presto/data/parquet/store"
    val warehouse = s"$rootPath/integration/presto/data/parquet/warehouse"
    val metaStoreDb = s"$rootPath/integration/presto/data/parquet"
    val filePath = s"$rootPath/integration/presto/data/ParquetBenchmarkingResults.txt"

    val spark = SparkSession
      .builder()
      .master("local")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(storeLocation, metaStoreDb)

    spark.sparkContext.setLogLevel("warn")

    prepareTable(spark)
    val res: Array[(Double, Int)] = runParquetQueries(spark)
    res.foreach { parquetTime =>
      writeResults("" + parquetTime._1 + "\n", filePath)
    }
    spark.close()
    System.exit(0)
  }
}
