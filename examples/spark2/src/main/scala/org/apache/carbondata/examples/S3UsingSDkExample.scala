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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.sdk.file.{CarbonWriter, Field, Schema}

/**
 * Generate data and write data to S3
 * User can generate different numbers of data by specifying the number-of-rows in parameters
 */
object S3UsingSDKExample {

  // prepare SDK writer output
  def buildTestData(
      path: String,
      num: Int = 3,
      persistSchema: Boolean = false): Any = {

    // getCanonicalPath gives path with \, but the code expects /.
    val writerPath = path.replace("\\", "/");

    val fields: Array[Field] = new Array[Field](3)
    fields(0) = new Field("name", DataTypes.STRING)
    fields(1) = new Field("age", DataTypes.INT)
    fields(2) = new Field("height", DataTypes.DOUBLE)

    try {
      val builder = CarbonWriter.builder()
      val writer =
        if (persistSchema) {
          builder.persistSchemaFile(true)
          builder.outputPath(writerPath).isTransactionalTable(true)
            .uniqueIdentifier(
              System.currentTimeMillis)
            .buildWriterForCSVInput(new Schema(fields))
        } else {
          builder.outputPath(writerPath).isTransactionalTable(true)
            .uniqueIdentifier(
              System.currentTimeMillis).withBlockSize(2)
            .buildWriterForCSVInput(new Schema(fields))
        }
      var i = 0
      var row = num
      while (i < row) {
        writer.write(Array[String]("robot" + i, String.valueOf(i), String.valueOf(i.toDouble / 2)))
        i += 1
      }
      writer.close()
    } catch {
      case ex: Exception => None
      case e => None
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

    import org.apache.spark.sql.CarbonSession._
    if (args.length < 2 || args.length > 6) {
      logger.error("Usage: java CarbonS3Example <access-key> <secret-key>" +
                   "[table-path-on-s3] [s3-endpoint] [number-of-rows] [spark-master]")
      System.exit(0)
    }

    val (accessKey, secretKey, endpoint) = getKeyOnPrefix(args(2))
    val spark = SparkSession
      .builder()
      .master(getSparkMaster(args))
      .appName("S3UsingSDKExample")
      .config("spark.driver.host", "localhost")
      .config(accessKey, args(0))
      .config(secretKey, args(1))
      .config(endpoint, getS3EndPoint(args))
      .getOrCreateCarbonSession()

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
    buildTestData(path, num)

    spark.sql("DROP TABLE IF EXISTS s3_sdk_table")
    spark.sql(s"CREATE EXTERNAL TABLE s3_sdk_table STORED BY 'carbondata'" +
      s" LOCATION '$path/Fact/Part0/Segment_null'")
    spark.sql("SELECT * FROM s3_sdk_table LIMIT 10").show()
    spark.stop()
  }

  def getKeyOnPrefix(path: String): (String, String, String) = {
    val endPoint = "spark.hadoop." + ENDPOINT
    if (path.startsWith(CarbonCommonConstants.S3A_PREFIX)) {
      ("spark.hadoop." + ACCESS_KEY, "spark.hadoop." + SECRET_KEY, endPoint)
    } else if (path.startsWith(CarbonCommonConstants.S3N_PREFIX)) {
      ("spark.hadoop." + CarbonCommonConstants.S3N_ACCESS_KEY,
        "spark.hadoop." + CarbonCommonConstants.S3N_SECRET_KEY, endPoint)
    } else if (path.startsWith(CarbonCommonConstants.S3_PREFIX)) {
      ("spark.hadoop." + CarbonCommonConstants.S3_ACCESS_KEY,
        "spark.hadoop." + CarbonCommonConstants.S3_SECRET_KEY, endPoint)
    } else {
      throw new Exception("Incorrect Store Path")
    }
  }

  def getS3EndPoint(args: Array[String]): String = {
    if (args.length >= 4 && args(3).contains(".com")) args(3)
    else ""
  }

  def getSparkMaster(args: Array[String]): String = {
    if (args.length == 6) args(5)
    else if (args(3).contains("spark:") || args(3).contains("mesos:")) args(3)
    else "local"
  }
}
