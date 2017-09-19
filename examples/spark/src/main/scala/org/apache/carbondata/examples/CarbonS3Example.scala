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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.CarbonContext

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
/**
 *
 */
object CarbonS3Example {

  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Require 5 parameters")
      System.err.println("Usage: java CarbonS3Example <fs.s3a.endpoint> <fs.s3a.access.key>" +
                       " <fs.s3a.secret.key> <fs.s3a.impl> <carbon store location>")
      return
    }

    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath

    val sc = new SparkContext(new SparkConf()
      .setAppName("CarbonS3Example")
      .setMaster("local[2]"))

    sc.hadoopConfiguration.set("fs.s3a.endpoint", args(0))
    sc.hadoopConfiguration.set("fs.s3a.access.key", args(1))
    sc.hadoopConfiguration.set("fs.s3a.secret.key", args(2))
    sc.hadoopConfiguration.set("fs.s3a.impl", args(3))

    val storeLocation = args(4)
    val metastoredb = s"$rootPath/examples/spark/target"
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_LOADING, "true")
      .addProperty(CarbonCommonConstants.LOCK_TYPE, "HDFSLOCK")
      .addProperty("carbon.storelocation", storeLocation)

    val spark = new CarbonContext(sc, storeLocation, metastoredb)
    spark.sql("drop table if exists t3")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE IF NOT EXISTS t3
         | (ID Int, date Date, country String,
         |  name String, phonetype String, serialname char(10), salary Int)
         | STORED BY 'carbondata'
       """.stripMargin)

    val path = s"$rootPath/examples/spark/src/main/resources/data.csv"

    // scalastyle:off
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE t3
         | OPTIONS('HEADER'='true')
       """.stripMargin)

    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE t3
         | OPTIONS('HEADER'='true')
       """.stripMargin)
    // scalastyle:on

    spark.sql(
      s"""
        | SELECT *
        | FROM t3
      """.stripMargin).show()

    spark.sql(
      s"""
         | SELECT *
         | FROM t3
         | where id = 1
      """.stripMargin).show()

    spark.sql(
      s"""
         | SELECT count(*)
         | FROM t3
      """.stripMargin).show()
  }

}
