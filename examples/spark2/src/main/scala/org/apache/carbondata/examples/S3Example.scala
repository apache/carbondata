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

import org.apache.hadoop.fs.s3a.Constants.{ACCESS_KEY, ENDPOINT, SECRET_KEY}
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import org.apache.carbondata.core.constants.CarbonCommonConstants

object S3Example {

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
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val path = s"$rootPath/examples/spark2/src/main/resources/data1.csv"
    val logger: Logger = LoggerFactory.getLogger(this.getClass)

    import org.apache.spark.sql.CarbonSession._
    if (args.length < 3 || args.length > 5) {
      logger.error("Usage: java CarbonS3Example <access-key> <secret-key>" +
                   "<table-path-on-s3> [s3-endpoint] [spark-master]")
      System.exit(0)
    }

    val (accessKey, secretKey, endpoint) = getKeyOnPrefix(args(2))
    val spark = SparkSession
      .builder()
      .master(getSparkMaster(args))
      .appName("S3Example")
      .config("spark.driver.host", "localhost")
      .config(accessKey, args(0))
      .config(secretKey, args(1))
      .config(endpoint, getS3EndPoint(args))
      .getOrCreateCarbonSession()

    spark.sparkContext.setLogLevel("WARN")

    spark.sql("Drop table if exists carbon_table")

    spark.sql(
      s"""
         | CREATE TABLE if not exists carbon_table(
         | shortField SHORT,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT
         | )
         | STORED BY 'carbondata'
         | LOCATION '${ args(2) }'
         | TBLPROPERTIES('SORT_COLUMNS'='', 'DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon_table
         | OPTIONS('HEADER'='true')
       """.stripMargin)

    spark.sql(
      s"""
         | SELECT *
         | FROM carbon_table
      """.stripMargin).show()

    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon_table
         | OPTIONS('HEADER'='true')
       """.stripMargin)

    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon_table
         | OPTIONS('HEADER'='true')
       """.stripMargin)

    val countSegment: Array[Row] =
      spark.sql(
        s"""
           | SHOW SEGMENTS FOR TABLE carbon_table
       """.stripMargin).collect()

    while (countSegment.length != 3) {
      this.wait(2000)
    }

    // Use compaction command to merge segments or small files in object based storage,
    // this can be done periodically.
    spark.sql("ALTER table carbon_table compact 'MAJOR'")
    spark.sql("show segments for table carbon_table").show()

    spark.sql(
      s"""
         | SELECT *
         | FROM carbon_table
      """.stripMargin).show()

    spark.sql("Drop table if exists carbon_table")

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
    if (args.length == 5) args(4)
    else if (args(3).contains("spark:") || args(3).contains("mesos:")) args(3)
    else "local"
  }
}
