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

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.s3a.Constants
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager

/**
 * This example show DataFrame How to read/Write SDK data from/to S3
 */
object DataFrameSDKS3Example {

  val rootPath = new File(this.getClass.getResource("/").getPath
    + "../../../..").getCanonicalPath

  def main(args: Array[String]): Unit = {
    if (args.length > 4 || args.length == 2) {
      val LOGGER = LogServiceFactory.getLogService(classOf[DataMapStoreManager].getName)
      LOGGER.error("If you want to use S3, Please input parameters:" +
        " <access-key> <secret-key> <s3-endpoint> [table-path-on-s3];" +
        "If you want to run in local, please use default to input local path")
      System.exit(0)
    }
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("SparkSessionExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    exampleBody(sparkSession, args)

    sparkSession.stop()
  }

  def exampleBody(sparkSession: SparkSession, args: Array[String] = Array.empty): Unit = {

    try {
      val df = sparkSession.emptyDataFrame

      var path = s"$rootPath/examples/spark2/target/carbon"
      if (args.length == 1) {
        path = args(0)
      }
      if (args.length == 3) {
        path = "s3a://carbon/sdk/DFTest"
      }
      if (args.length > 3) {
        path = args(3)
      }

      write(df, path, args);
      read(df, path, args);
    } catch {
      case e: Exception => assert(false)
    }
  }

  /**
   * inherit DataFrame from other place,
   * it need create CarbonSession for read CarbonData from S3
   *
   * @param df   DataFrame, including SparkConf
   * @param path read path
   * @param args argument, including ak, sk, endpoint
   */
  def read(df: DataFrame, path: String, args: Array[String]): Unit = {
    val carbonSession = DataFrameToCarbonSession(df, path, args, 3);

    val result = carbonSession
      .read
      .format("carbon")
      .load(path)
    result.show()
    result.foreach { each =>
      assert(each.get(0).toString.contains("city"))
    }
    carbonSession.stop()
  }

  /**
   * inherit DataFrame from other place,
   * it need create CarbonSession for write CarbonData to S3
   *
   * @param df   DataFrame, including SparkConf
   * @param path write path
   * @param args argument, including ak, sk, endpoint
   */
  def write(df: DataFrame, path: String, args: Array[String]): Unit = {
    val carbonSession = DataFrameToCarbonSession(df, path, args, 4);

    val rdd = carbonSession.sqlContext.sparkContext
      .parallelize(1 to 1200, 4)
      .map { x =>
        ("city" + x % 8, "country" + x % 1103, "planet" + x % 10007, x.toString,
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

    carbonSession.createDataFrame(rdd, schema)
      .write
      .format("carbon")
      .mode(SaveMode.Append)
      .save(path)

    carbonSession.stop()
  }

  def DataFrameToCarbonSession(df: DataFrame,
      path: String, args: Array[String], num: Int): SparkSession = {
    import org.apache.spark.sql.CarbonSession._
    val storeLocation = s"$rootPath/examples/spark2/target/store" + num
    val metaStoreDB = s"$rootPath/examples/spark2/target/metaStore_db" + num

    if (true) {
      val clean = (path: String) => FileUtils.deleteDirectory(new File(path))
      clean(storeLocation)
      clean(metaStoreDB)
    }

    val builder = SparkSession
      .builder()
      .master("local")
      .appName("carbon")
      .config(df.sparkSession.sparkContext.getConf)
      .config("spark.sql.crossJoin.enabled", "true")

    if (path.startsWith(CarbonCommonConstants.S3A_PREFIX)
      || path.startsWith(CarbonCommonConstants.S3N_PREFIX) ||
      path.startsWith(CarbonCommonConstants.S3_PREFIX)) {
      builder.config(Constants.ACCESS_KEY, args(0))
        .config(Constants.SECRET_KEY, args(1))
        .config(Constants.ENDPOINT, args(2))
        .config("javax.jdo.option.ConnectionURL",
          s"jdbc:derby:;databaseName=$metaStoreDB;create=true")
        .getOrCreateCarbonSession(storeLocation, metaStoreDB)
    } else {
      builder.config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=$metaStoreDB;create=true")
        .getOrCreateCarbonSession(storeLocation, metaStoreDB)
    }
  }
}
