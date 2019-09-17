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
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object S3CsvExample {

  /**
   * This example demonstrate to create local store and load data from CSV files on S3
   *
   * @param args require three parameters "Access-key" "Secret-key"
   *             "s3 path to csv" "spark-master"
   */
  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val logger: Logger = LoggerFactory.getLogger(this.getClass)

    import org.apache.spark.sql.CarbonUtils._
    if (args.length != 4) {
      logger.error("Usage: java CarbonS3Example <access-key> <secret-key>" +
                   "<s3.csv.location> <spark-master>")
      System.exit(0)
    }

    val spark =
      SparkSession
      .builder()
      .master(args(3))
      .appName("S3CsvExample")
      .config("spark.driver.host", "localhost")
      .config("spark.hadoop." + ACCESS_KEY, args(0))
      .config("spark.hadoop." + SECRET_KEY, args(1))
      .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
      .getOrCreate()

    CarbonEnv.getInstance(spark)

    spark.sparkContext.setLogLevel("INFO")

    spark.sql(
      s"""
         | CREATE TABLE if not exists carbon_table1(
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
         | LOCATION '$rootPath/examples/spark2/target/store'
         | TBLPROPERTIES('SORT_COLUMNS'='', 'DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '${ args(2) }'
         | INTO TABLE carbon_table1
         | OPTIONS('HEADER'='true')
       """.stripMargin)

    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '${ args(2) }'
         | INTO TABLE carbon_table1
         | OPTIONS('HEADER'='true')
       """.stripMargin)

    spark.sql(
      s"""
         | SELECT *
         | FROM carbon_table1
      """.stripMargin).show()

    spark.sql("Drop table if exists carbon_table1")

    spark.stop()
  }
}
