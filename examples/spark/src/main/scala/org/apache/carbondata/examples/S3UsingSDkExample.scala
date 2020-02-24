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

import org.apache.hadoop.fs.s3a.Constants.{ACCESS_KEY, ENDPOINT, SECRET_KEY}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.sdk.file.{CarbonWriter, Field, Schema}
import org.apache.carbondata.spark.util.CarbonSparkUtil


/**
 * Generate data and write data to S3
 * User can generate different numbers of data by specifying the number-of-rows in parameters
 */
object S3UsingSdkExample {

  // prepare SDK writer output
  def buildTestData(
                     args: Array[String],
                     path: String,
                     num: Int = 3): Any = {

    // getCanonicalPath gives path with \, but the code expects /.
    val writerPath = path.replace("\\", "/")

    val fields: Array[Field] = new Array[Field](3)
    fields(0) = new Field("name", DataTypes.STRING)
    fields(1) = new Field("age", DataTypes.INT)
    fields(2) = new Field("height", DataTypes.DOUBLE)

    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(writerPath)
          .uniqueIdentifier(System.currentTimeMillis)
          .withBlockSize(2)
          .writtenBy("S3UsingSdkExample")
          .withHadoopConf(ACCESS_KEY, args(0))
          .withHadoopConf(SECRET_KEY, args(1))
          .withHadoopConf(ENDPOINT, CarbonSparkUtil.getS3EndPoint(args))
          .withCsvInput(new Schema(fields)).build()
      var i = 0
      val row = num
      while (i < row) {
        writer.write(Array[String]("robot" + i, String.valueOf(i), String.valueOf(i.toDouble / 2)))
        i += 1
      }
      writer.close()
    } catch {
      case e: Exception => throw e
    }
  }

  /**
   * This example demonstrate usage of
   * 1. create carbon table with storage location on object based storage
   * like AWS S3, Huawei OBS, etc
   * 2. load data into carbon table, the generated file will be stored on object based storage
   * query the table.
   *
   * @param args require three parameters "Access-key" "Secret-key"
   *             "table-path on s3" "s3-endpoint" "spark-master"
   */
  def main(args: Array[String]) {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)

    if (args.length < 2 || args.length > 6) {
      logger.error("Usage: java CarbonS3Example <access-key> <secret-key>" +
        "[table-path-on-s3] [s3-endpoint] [number-of-rows] [spark-master]")
      System.exit(0)
    }

    val (accessKey, secretKey, endpoint) = CarbonSparkUtil.getKeyOnPrefix(args(2))
    val spark = SparkSession
      .builder()
      .master(getSparkMaster(args))
      .appName("S3UsingSDKExample")
      .config("spark.driver.host", "localhost")
      .config(accessKey, args(0))
      .config(secretKey, args(1))
      .config(endpoint, CarbonSparkUtil.getS3EndPoint(args))
      .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val path = if (args.length < 3) {
      "s3a://sdk/WriterOutput2 "
    } else {
      args(2)
    }
    val num = if (args.length > 4) {
      Integer.parseInt(args(4))
    } else {
      3
    }
    buildTestData(args, path, num)

    spark.sql("DROP TABLE IF EXISTS s3_sdk_table")
    spark.sql(s"CREATE EXTERNAL TABLE s3_sdk_table STORED AS carbondata" +
      s" LOCATION '$path'")
    spark.sql("SELECT * FROM s3_sdk_table LIMIT 10").show()
    spark.stop()
  }

  def getSparkMaster(args: Array[String]): String = {
    if (args.length == 6) args(5)
    else "local"
  }

}
