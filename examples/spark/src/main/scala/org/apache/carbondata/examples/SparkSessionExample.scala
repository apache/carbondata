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
import org.apache.carbondata.examples.util.ExampleUtils

/**
 * This example doesn't create carbonsession, but use CarbonSource when creating table
 */

object SparkSessionExample {
  val rootPath = new File(this.getClass.getResource("/").getPath
                          + "../../../..").getCanonicalPath
  def main(args: Array[String]): Unit = {
    val sparkSession = ExampleUtils.createSparkSession("SparkSessionExample")
    val path = s"$rootPath/examples/spark/src/main/resources/data.csv"
    sparkSession.sql("DROP TABLE IF EXISTS csv_table")
    sparkSession.sql(
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

    sparkSession.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE csv_table
       """.stripMargin)

    sparkSession.sql("SELECT * FROM csv_table").show()

    sparkTableExample(sparkSession)
    hiveTableExample(sparkSession)

    // Drop table
    sparkSession.sql("DROP TABLE IF EXISTS csv_table")
    sparkSession.stop()
  }

  def sparkTableExample(sparkSession: SparkSession): Unit = {
    // Create table
    sparkSession.sql("DROP TABLE IF EXISTS sparksession_table")
    sparkSession.sql(
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
       """.stripMargin)

    validateTable(sparkSession, "sparksession_table")

    sparkSession.sql("DROP TABLE IF EXISTS sparksession_table")
  }

  def hiveTableExample(sparkSession: SparkSession): Unit = {
    // Create table
    sparkSession.sql("DROP TABLE IF EXISTS sparksession_hive_table")
    sparkSession.sql(
      s"""
         | CREATE TABLE sparksession_hive_table(
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
         | STORED AS carbondata
       """.stripMargin)

    validateTable(sparkSession, "sparksession_hive_table")
  }

  def validateTable(sparkSession: SparkSession, tableName: String): Unit = {
    sparkSession.sql(
      s"""
         | INSERT INTO TABLE $tableName
         | SELECT shortField, intField, bigintField, doubleField, stringField,
         | from_unixtime(unix_timestamp(timestampField,'yyyy/MM/dd HH:mm:ss')) timestampField,
         | decimalField,from_unixtime(unix_timestamp(dateField,'yyyy/MM/dd')), charField
         | FROM csv_table
       """.stripMargin)

    sparkSession.sql(s"SELECT * FROM $tableName").show()

    sparkSession.sql(
      s"""
         | SELECT *
         | FROM $tableName
         | WHERE stringfield = 'spark' AND decimalField > 40
      """.stripMargin).show()

    // Shows with raw data's timestamp format
    sparkSession.sql(
      s"""
         | SELECT
         | stringField, date_format(timestampField, "yyyy/MM/dd HH:mm:ss") AS
         | timestampField
         | FROM $tableName WHERE length(stringField) = 5
       """.stripMargin).show()

    sparkSession.sql(
      s"""
         | SELECT *
         | FROM $tableName where date_format(dateField, "yyyy-MM-dd") = "2015-07-23"
       """.stripMargin).show()

    sparkSession.sql(s"SELECT count(stringField) FROM $tableName").show()

    sparkSession.sql(
      s"""
         | SELECT sum(intField), stringField
         | FROM $tableName
         | GROUP BY stringField
       """.stripMargin).show()

    sparkSession.sql(
      s"""
         | SELECT t1.*, t2.*
         | FROM $tableName t1, $tableName t2
         | WHERE t1.stringField = t2.stringField
      """.stripMargin).show()

    sparkSession.sql(
      s"""
         | WITH t1 AS (
         | SELECT * FROM $tableName
         | UNION ALL
         | SELECT * FROM $tableName
         | )
         | SELECT t1.*, t2.*
         | FROM t1, $tableName t2
         | WHERE t1.stringField = t2.stringField
      """.stripMargin).show()
  }
}
