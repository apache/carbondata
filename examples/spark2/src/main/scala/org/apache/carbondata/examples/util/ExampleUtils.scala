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

package org.apache.carbondata.examples.util

import java.io.File

import org.apache.spark.sql.{CarbonEnv, SaveMode, SparkSession}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object ExampleUtils {

  def currentPath: String = new File(this.getClass.getResource("/").getPath + "../../")
      .getCanonicalPath
  val storeLocation: String = currentPath + "/target/store"

  def createCarbonSession (appName: String, workThreadNum: Int = 1,
      storePath: String = null): SparkSession = {
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath

    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metaStoreDB = s"$rootPath/examples/spark2/target"

    val storeLocation = if (null != storePath) {
      storePath
    } else {
      s"$rootPath/examples/spark2/target/store"
    }

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
      .addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC, "")

    val masterUrl = if (workThreadNum <= 1) {
      "local"
    } else {
      "local[" + workThreadNum.toString() + "]"
    }

    val spark = SparkSession
      .builder()
      .master(masterUrl)
      .appName(appName)
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.driver.host", "localhost")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
      .enableHiveSupport()
      .getOrCreate()

    CarbonEnv.getInstance(spark)

    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  /**
   * This func will write a sample CarbonData file containing following schema:
   * c1: String, c2: String, c3: Double
   * Returns table path
   */
  def writeSampleCarbonFile(spark: SparkSession, tableName: String, numRows: Int = 1000): String = {
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    writeDataframe(spark, tableName, numRows, SaveMode.Overwrite)
    s"$storeLocation/default/$tableName"
  }

  /**
   * This func will append data to the CarbonData file
   * Returns table path
   */
  def appendSampleCarbonFile(
      spark: SparkSession, tableName: String, numRows: Int = 1000): String = {
    writeDataframe(spark, tableName, numRows, SaveMode.Append)
    s"$storeLocation/default/$tableName"
  }

  /**
   * create a new dataframe and write to CarbonData file, based on save mode
   */
  private def writeDataframe(
      spark: SparkSession, tableName: String, numRows: Int, mode: SaveMode): Unit = {
    // use CarbonContext to write CarbonData files
    import spark.implicits._
    val sc = spark.sparkContext
    val df = sc.parallelize(1 to numRows, 2)
        .map(x => ("a", "b", x))
        .toDF("c1", "c2", "c3")

    // save dataframe directl to carbon file without tempCSV
    df.write
      .format("carbondata")
      .option("tableName", tableName)
      .option("compress", "true")
      .option("tempCSV", "false")
      .mode(mode)
      .save()
  }

  def cleanSampleCarbonFile(spark: SparkSession, tableName: String): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
  }
}
