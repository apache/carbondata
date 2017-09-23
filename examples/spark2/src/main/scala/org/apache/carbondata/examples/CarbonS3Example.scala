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

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 *
 */
object CarbonS3Example {

  def main(args: Array[String]) {
    if (args.length < 5) {
      // scalastyle:off println
      System.err.println("Require 5 parameters")
      System.err.println("Usage: java CarbonS3Example <fs.s3a.endpoint> <fs.s3a.access.key>" +
                       " <fs.s3a.secret.key> <fs.s3a.impl> <carbon store location>")
      // scalastyle:on println
      return
    }

    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val storeLocation = args(4)
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_LOADING, "true")
      .addProperty(CarbonCommonConstants.LOCK_TYPE, "HDFSLOCK")

    import org.apache.spark.sql.CarbonSession._
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonSessionExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.driver.host", "localhost")
      .config("fs.s3a.endpoint", args(0))
      .config("fs.s3a.access.key", args(1))
      .config("fs.s3a.secret.key", args(2))
      .config("fs.s3a.impl", args(3))
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    spark.sparkContext.setLogLevel("INFO")
    // Create table
    spark.sql(
      s"""
         | CREATE TABLE if not exists carbondata_table(
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
         | TBLPROPERTIES('SORT_COLUMNS'='intField')
       """.stripMargin)

    val path = s"$rootPath/examples/spark2/src/main/resources/data.csv"

    // scalastyle:off
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbondata_table
         | OPTIONS('HEADER'='true')
       """.stripMargin)

    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbondata_table
         | OPTIONS('HEADER'='true')
       """.stripMargin)
    // scalastyle:on

    spark.sql(
      s"""
        | SELECT *
        | FROM carbondata_table
      """.stripMargin).show()

    spark.sql(
      s"""
         | SELECT count(*)
         | FROM carbondata_table
      """.stripMargin).show()

    spark.stop()
  }

}
