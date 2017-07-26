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
import java.sql.DriverManager

import scala.util.Try

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.datanucleus.store.rdbms.connectionpool.DatastoreDriverNotFoundException

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.CompareTest
import org.apache.carbondata.hive.server.HiveEmbeddedServer2

object OrcPerformance {

  private def writeResults(content: String, file: String) = {
    scala.tools.nsc.io.File(file).appendAll(content)
  }

  private def loadCarbonTable(input: DataFrame, carbonTableName: String): Unit = {
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
    input.write
      .format("carbondata")
      .option("tableName", carbonTableName)
      .option("tempCSV", "false")
      .option("single_pass", "true")
      .option("dictionary_exclude", "id") // id is high cardinality column
      .option("table_blocksize", "32")
      .mode(SaveMode.Overwrite)
      .save()
  }

  private def generateDataFrame(spark: SparkSession): DataFrame = {
    val rdd = spark.sparkContext
      .parallelize(1 to 10 * 1000 * 1000, 4)
      .map { value =>
        ("city" + value % 8, "country" + value % 1103, "planet" + value % 10007, "IDENTIFIER" +
          value.toString,
          (value % 16).toShort, value / 2, (value << 1).toLong, value.toDouble / 13,
          value.toDouble / 11)
      }.map { value =>
      Row(value._1, value._2, value._3, value._4, value._5, value._6, value._7, value._8, value._9)
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

    val dataFrame = spark.createDataFrame(rdd, schema)
    dataFrame.printSchema()
    dataFrame
  }

  def main(args: Array[String]): Unit = {
    val logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val filePath = s"$rootPath/integration/presto/data/OrcBenchmarkingResults.txt"
    val hiveCarbonTableName = "comparetest_hive_carbon"
    val hiveOrcTableName = "hive_orctable"
    val carbonTableName = "comparetest_carbonv3"
    val storeLocation = "hdfs://localhost:54310/user/hive/warehouse/carbon.store"
    val warehouse = s"$rootPath/integration/presto/data/warehouse"
    val metaStoreDb = s"$rootPath/integration/presto/data/metastore_db"

    CarbonProperties.getInstance()
      .addProperty("carbon.enable.vector.reader", "true")
      .addProperty("enable.unsafe.sort", "true")
      .addProperty("carbon.blockletgroup.size.in.mb", "32")

    import org.apache.spark.sql.CarbonSession._

    val carbon = SparkSession
      .builder()
      .master("local")
      .appName("CompareTestExample")
      .config("carbon.sql.warehouse.dir", warehouse).enableHiveSupport()
      .getOrCreateCarbonSession(
        storeLocation, metaStoreDb)

    loadCarbonTable(generateDataFrame(carbon), carbonTableName)

    val hiveEmbeddedServer2 = new HiveEmbeddedServer2
    hiveEmbeddedServer2.start()
    val port = hiveEmbeddedServer2.getFreePort

    Try(Class.forName("org.apache.hive.jdbc.HiveDriver")).getOrElse(
      throw new DatastoreDriverNotFoundException("driver not found "))

    val conn = DriverManager
      .getConnection(s"jdbc:hive2://localhost:$port/default", "anonymous", "anonymous")
    val stmt = conn.createStatement

    try {
      logger.info(s"============HIVE CLI STARTED ON PORT $port ==============")

      logger.info("Creating Hive Orc Table")

      stmt.execute(s"DROP TABLE IF EXISTS $hiveOrcTableName")
      stmt.execute(s"DROP TABLE IF EXISTS $hiveCarbonTableName")

      stmt
        .execute(
          s"CREATE TABLE IF NOT EXISTS $hiveOrcTableName(city string,country " +
          s"string,planet string ,id string,m1 smallint,m2 int,m3 bigint,m4 " +
          s"double,m5 double)stored as orc")

      stmt
        .execute(
          s"CREATE TABLE IF NOT EXISTS $hiveCarbonTableName(city string,country string,planet " +
            s"string,id string,m1 smallint,m2 int,m3 bigint,m4 double,m5 double)")
      stmt
        .execute(
          "ALTER TABLE comparetest_hive_carbon SET FILEFORMAT INPUTFORMAT " +
          "\"org.apache.carbondata.hive.MapredCarbonInputFormat\"OUTPUTFORMAT " +
          "\"org.apache.carbondata.hive.MapredCarbonOutputFormat\"SERDE " +
          "\"org.apache.carbondata.hive.CarbonHiveSerDe\" ")

      stmt
        .execute(s"ALTER TABLE $hiveCarbonTableName SET LOCATION " +
          s"'$storeLocation/default/$carbonTableName' ".stripMargin)

      stmt
        .execute(s"INSERT INTO $hiveOrcTableName SELECT * FROM $hiveCarbonTableName")

      val orcResults: Array[Double] = CompareTest.queries.map { queries =>
        val query = queries.sqlText.replace("$table", hiveOrcTableName)
        CompareTest.time(stmt.execute(query))
      }

      stmt.execute(s"DROP TABLE IF EXISTS $hiveOrcTableName")

      orcResults.foreach { orcTime =>
        writeResults("" + orcTime + "\n", filePath)
      }
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage)
    } finally {
      stmt.close()
      conn.close()
      hiveEmbeddedServer2.stop()
      System.exit(0)
    }
  }
}
