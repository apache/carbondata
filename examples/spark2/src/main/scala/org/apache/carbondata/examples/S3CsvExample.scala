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

object S3CsvExample {

  /**
   * This example demonstrate to create local store having csv on s3.
   *
   * @param args require three parameters "Access-key" "Secret-key"
   *             "s3 bucket path"
   */

  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val logger: Logger = LoggerFactory.getLogger(this.getClass)

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_LOADING, "true")

    import org.apache.spark.sql.CarbonSession._
    if (args.length != 3) {
      logger.error("Usage: java CarbonS3Example <access-key> <secret-key>" +
                   "<s3.csv.location>")
      System.exit(0)
    }

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
         | floatField FLOAT,
         | complexData ARRAY<STRING>
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('SORT_COLUMNS'='', 'DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    // scalastyle:off
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '${ args(2) }'
         | INTO TABLE carbon_table
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)

    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '${ args(2) }'
         | INTO TABLE carbon_table
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)
    // scalastyle:on

    spark.sql(
      s"""
         | SELECT *
         | FROM carbon_table
      """.stripMargin).show()

    spark.sql("Drop table if exists carbon_table")

    spark.stop()
  }
}
