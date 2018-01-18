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

object MultiStoreExample {

  /** This example demonstrate the usage of multiple filesystem(s3 and local) on one carbon session
   *
   * @param args represents "fs.s3a.access.key" "fs.s3a.secret.key" "bucket-name"
   */

  def main(args: Array[String]) {

    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target"
    val path = s"$rootPath/examples/spark2/src/main/resources/data.csv"
    val logger: Logger = LoggerFactory.getLogger(this.getClass)

    if (args.length != 3) {
      logger.error("Usage: java CarbonS3Example <fs.s3a.access.key> <fs.s3a.secret" +
              ".key> <bucket-name>")
      System.exit(0)
    }

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_LOADING, "true")

    import org.apache.spark.sql.CarbonSession._
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonSessionExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.driver.host", "localhost")
      .config("spark.hadoop." + ACCESS_KEY, args(0))
      .config("spark.hadoop." + SECRET_KEY, args(1))
      .getOrCreateCarbonSession(storeLocation, warehouse)

    spark.sparkContext.setLogLevel("INFO")

    val s3Db = "s3Db"
    val s3Table = "s3table"
    val localDb = "localdatabase"
    val localTable = "localtable"

    spark.sql(s"CREATE DATABASE if not exists $s3Db LOCATION 's3a://${ args(2) }/$s3Db'")
    spark.sql(
      s"""
         | CREATE TABLE if not exists $s3Db.$s3Table(
         | shortField SHORT,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('SORT_COLUMNS'='', 'DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    // scalastyle:off
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE $s3Db.$s3Table
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)
    // scalastyle:on

    spark.sql(
      s"""
         | SELECT *
         | FROM $s3Db.$s3Table
         |
      """.stripMargin).show()
    spark.sql(s"Drop table if exists $s3Db.$s3Table")
    spark.sql(s"Drop database if exists $s3Db")

    spark.sql(s"CREATE DATABASE if not exists $localDb")
    spark.sql(
      s"""
         | CREATE TABLE if not exists $localDb.$localTable(
         | shortField SHORT,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('SORT_COLUMNS'='', 'DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    // scalastyle:off
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE $localDb.$localTable
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)
    // scalastyle:on

    spark.sql(
      s"""
         | SELECT *
         | FROM $localDb.$localTable
         |
      """.stripMargin).show()

    spark.sql(s"Drop table if exists $localDb.$localTable")
    spark.sql(s"Drop database if exists $localDb")

    spark.stop()
  }
}
