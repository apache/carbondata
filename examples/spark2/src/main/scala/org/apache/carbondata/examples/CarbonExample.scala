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

package org.apache.spark.sql.examples

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object CarbonExample {

  def main(args: Array[String]): Unit = {
    val rootPath = new File(this.getClass.getResource("/").getPath
        + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target/metastore_db"

    // clean data folder
    if (true) {
      val clean = (path: String) => FileUtils.deleteDirectory(new File(path))
      clean(storeLocation)
      clean(warehouse)
      clean(metastoredb)
    }

    val spark = SparkSession
        .builder()
        .master("local")
        .appName("CarbonExample")
        .enableHiveSupport()
        .config("carbon.kettle.home",
          s"$rootPath/processing/carbonplugins")
        .config("carbon.storelocation", storeLocation)
        .config("spark.sql.warehouse.dir", warehouse)
        .config("javax.jdo.option.ConnectionURL",
          s"jdbc:derby:;databaseName=$metastoredb;create=true")
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE carbon_table(
         |    shortField short,
         |    intField int,
         |    bigintField long,
         |    doubleField double,
         |    stringField string,
         |    timestampField timestamp
         | )
         | USING org.apache.spark.sql.CarbonSource
       """.stripMargin)

    // val prop = s"$rootPath/conf/dataload.properties.template"
    // val tableName = "carbon_table"
    val path = s"$rootPath/examples/spark2/src/main/resources/data.csv"
    // TableLoader.main(Array[String](prop, tableName, path))

    spark.sql(
      s"""
         | CREATE TABLE csv_table
         | (  shortField short,
         |    intField int,
         |    bigintField long,
         |    doubleField double,
         |    stringField string,
         |    timestampField string)
         |    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       """.stripMargin)

    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE csv_table
       """.stripMargin)

    spark.sql("""
             SELECT *
             FROM csv_table
              """).show

    spark.sql(
      s"""
         | INSERT INTO TABLE carbon_table
         | SELECT shortField, intField, bigintField, doubleField, stringField,
         | from_unixtime(unix_timestamp(timestampField,'yyyy/M/dd')) timestampField
         | FROM csv_table
       """.stripMargin)

    spark.sql("""
             SELECT *
             FROM carbon_table
              """).show

    spark.sql("""
             SELECT *
             FROM carbon_table where length(stringField) = 5
              """).show

    spark.sql("""
           SELECT sum(intField), stringField
           FROM carbon_table
           GROUP BY stringField
           """).show

    spark.sql(
      """
        |select t1.*, t2.*
        |from carbon_table t1, carbon_table t2
        |where t1.stringField = t2.stringField
      """.stripMargin).show

    // Drop table
    spark.sql("DROP TABLE IF EXISTS carbon_table")
    spark.sql("DROP TABLE IF EXISTS csv_table")
  }
}
