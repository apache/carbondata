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

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object SparkSessionExample {

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
      .appName("SparkSessionExample")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouse)
      .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=$metastoredb;create=true")
      .getOrCreate()

    CarbonProperties.getInstance()
      .addProperty("carbon.storelocation", storeLocation)

    spark.sparkContext.setLogLevel("WARN")

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE carbon_table(
         | shortField SHORT,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5)
         | )
         | USING org.apache.spark.sql.CarbonSource
         | OPTIONS('DICTIONARY_INCLUDE'='dateField, charField',
         | 'dbName'='default', 'tableName'='carbon_table')
       """.stripMargin)

    val path = s"$rootPath/examples/spark2/src/main/resources/data.csv"

    spark.sql(
      s"""
         | CREATE TABLE csv_table(
         | shortField SHORT,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField STRING,
         | decimalField DECIMAL(18,2),
         | dateField STRING,
         | charField CHAR(5))
         | ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       """.stripMargin)

    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE csv_table
       """.stripMargin)

    spark.sql("SELECT * FROM csv_table").show()

    spark.sql(
      s"""
         | INSERT INTO TABLE carbon_table
         | SELECT shortField, intField, bigintField, doubleField, stringField,
         | from_unixtime(unix_timestamp(timestampField,'yyyy/MM/dd HH:mm:ss')) timestampField,
         | decimalField,from_unixtime(unix_timestamp(dateField,'yyyy/MM/dd')), charField
         | FROM csv_table
       """.stripMargin)

    spark.sql(
      s"""
         | SELECT *
         | FROM carbon_table
         | WHERE stringfield = 'spark' AND decimalField > 40
      """.stripMargin).show()

    // Shows with raw data's timestamp format
    spark.sql(
      s"""
         | SELECT
         | stringField, date_format(timestampField, "yyyy/MM/dd HH:mm:ss") AS
         | timestampField
         | FROM carbon_table WHERE length(stringField) = 5
       """.stripMargin).show()

    spark.sql(
      s"""
         | SELECT *
         | FROM carbon_table where date_format(dateField, "yyyy-MM-dd") = "2015-07-23"
       """.stripMargin).show()

    spark.sql("SELECT count(stringField) FROM carbon_table").show()

    spark.sql(
      s"""
         | SELECT sum(intField), stringField
         | FROM carbon_table
         | GROUP BY stringField
       """.stripMargin).show()

    spark.sql(
      s"""
         | SELECT t1.*, t2.*
         | FROM carbon_table t1, carbon_table t2
         | WHERE t1.stringField = t2.stringField
      """.stripMargin).show()

    spark.sql(
      s"""
         | WITH t1 AS (
         | SELECT * FROM carbon_table
         | UNION ALL
         | SELECT * FROM carbon_table
         | )
         | SELECT t1.*, t2.*
         | FROM t1, carbon_table t2
         | WHERE t1.stringField = t2.stringField
      """.stripMargin).show()

    // Drop table
    spark.sql("DROP TABLE IF EXISTS carbon_table")
    spark.sql("DROP TABLE IF EXISTS csv_table")

    spark.stop()
  }
}
