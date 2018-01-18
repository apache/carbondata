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

import org.apache.hadoop.fs.s3a.Constants.{ACCESS_KEY, SECRET_KEY}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object S3Example {

  /**
   * This example demonstrate usage of s3 as a store.
   *
   * @param args require three parameters "Access-key" "Secret-key"
   *             "s3 bucket path"
   */

  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val path = s"$rootPath/examples/spark2/src/main/resources/data1.csv"
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_LOADING, "true")
      .addProperty(CarbonCommonConstants.DEFAULT_CARBON_MAJOR_COMPACTION_SIZE, "0.02")

    import org.apache.spark.sql.CarbonSession._
    if (args.length != 3) {
      logger.error("Usage: java CarbonS3Example <access-key> <secret-key>" +
                   "<carbon store location>")
      System.exit(0)
    }

    val (accessKey, secretKey) = getKeyOnPrefix(args(2))
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonSessionExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.driver.host", "localhost")
      .config(accessKey, args(0))
      .config(secretKey, args(1))
      .getOrCreateCarbonSession(args(2), warehouse)

    spark.sparkContext.setLogLevel("INFO")

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

    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon_table
         | OPTIONS('HEADER'='true')
       """.stripMargin)

    spark.sql("ALTER table carbon_table compact 'MINOR'")
    spark.sql("show segments for table carbon_table").show()

    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon_table
         | OPTIONS('HEADER'='true')
       """.stripMargin)

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

  def getKeyOnPrefix(path: String): (String, String) = {
    if (path.startsWith(CarbonCommonConstants.S3A_PREFIX)) {
      ("spark.hadoop." + ACCESS_KEY, "spark.hadoop." + SECRET_KEY)
    } else if (path.startsWith(CarbonCommonConstants.S3N_PREFIX)) {
      ("spark.hadoop." + CarbonCommonConstants.S3N_ACCESS_KEY,
        "spark.hadoop." + CarbonCommonConstants.S3N_SECRET_KEY)
    } else if (path.startsWith(CarbonCommonConstants.S3_PREFIX)) {
      ("spark.hadoop." + CarbonCommonConstants.S3_ACCESS_KEY,
        "spark.hadoop." + CarbonCommonConstants.S3_SECRET_KEY)
    } else {
      throw new Exception("Incorrect Store Path")
    }
  }
}
