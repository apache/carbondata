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

/**
 * This example doesn't create carbonsession, but use CarbonSource when creating table
 */

object SparkSessionExample {

  def main(args: Array[String]): Unit = {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metaStoreDB = s"$rootPath/examples/spark2/target/metastore_db"

    // clean data folder
    if (true) {
      val clean = (path: String) => FileUtils.deleteDirectory(new File(path))
      clean(storeLocation)
      clean(warehouse)
      clean(metaStoreDB)
    }

    val sparksession = SparkSession
      .builder()
      .master("local")
      .appName("SparkSessionExample")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouse)
      .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=$metaStoreDB;create=true")
      .getOrCreate()

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
      .addProperty("carbon.storelocation", storeLocation)

    sparksession.sparkContext.setLogLevel("ERROR")

    // Create table
    sparksession.sql("DROP TABLE IF EXISTS sparksession_table")
    sparksession.sql(
      s"""
         | CREATE TABLE sparksession_table(
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
         | USING carbondata
         | OPTIONS('DICTIONARY_INCLUDE'='dateField, charField',
         | 'dbName'='default', 'tableName'='sparksession_table')
       """.stripMargin)

    val path = s"$rootPath/examples/spark2/src/main/resources/data.csv"

    sparksession.sql("DROP TABLE IF EXISTS csv_table")
    sparksession.sql(
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

    sparksession.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE csv_table
       """.stripMargin)

    sparksession.sql("SELECT * FROM csv_table").show()

    sparksession.sql(
      s"""
         | INSERT INTO TABLE sparksession_table
         | SELECT shortField, intField, bigintField, doubleField, stringField,
         | from_unixtime(unix_timestamp(timestampField,'yyyy/MM/dd HH:mm:ss')) timestampField,
         | decimalField,from_unixtime(unix_timestamp(dateField,'yyyy/MM/dd')), charField
         | FROM csv_table
       """.stripMargin)

    sparksession.sql("SELECT * FROM sparksession_table").show()

    sparksession.sql(
      s"""
         | SELECT *
         | FROM sparksession_table
         | WHERE stringfield = 'spark' AND decimalField > 40
      """.stripMargin).show()

    // Shows with raw data's timestamp format
    sparksession.sql(
      s"""
         | SELECT
         | stringField, date_format(timestampField, "yyyy/MM/dd HH:mm:ss") AS
         | timestampField
         | FROM sparksession_table WHERE length(stringField) = 5
       """.stripMargin).show()

    sparksession.sql(
      s"""
         | SELECT *
         | FROM sparksession_table where date_format(dateField, "yyyy-MM-dd") = "2015-07-23"
       """.stripMargin).show()

    sparksession.sql("SELECT count(stringField) FROM sparksession_table").show()

    sparksession.sql(
      s"""
         | SELECT sum(intField), stringField
         | FROM sparksession_table
         | GROUP BY stringField
       """.stripMargin).show()

    sparksession.sql(
      s"""
         | SELECT t1.*, t2.*
         | FROM sparksession_table t1, sparksession_table t2
         | WHERE t1.stringField = t2.stringField
      """.stripMargin).show()

    sparksession.sql(
      s"""
         | WITH t1 AS (
         | SELECT * FROM sparksession_table
         | UNION ALL
         | SELECT * FROM sparksession_table
         | )
         | SELECT t1.*, t2.*
         | FROM t1, sparksession_table t2
         | WHERE t1.stringField = t2.stringField
      """.stripMargin).show()

    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

    // Drop table
    sparksession.sql("DROP TABLE IF EXISTS sparksession_table")
    sparksession.sql("DROP TABLE IF EXISTS csv_table")

    sparksession.stop()
  }
}
