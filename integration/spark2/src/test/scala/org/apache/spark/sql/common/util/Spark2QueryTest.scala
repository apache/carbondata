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

package org.apache.spark.sql.common.util

import org.apache.spark.sql.hive.CarbonSessionState
import org.apache.spark.sql.test.util.QueryTest


class Spark2QueryTest extends QueryTest {

  val hiveClient = sqlContext.sparkSession.sessionState.asInstanceOf[CarbonSessionState]
    .metadataHive

  protected def createAndLoadInputTable(inputTableName: String, inputPath: String): Unit = {
    sql(
      s"""
         | CREATE TABLE $inputTableName
         | (  shortField short,
         |    intField int,
         |    bigintField long,
         |    doubleField double,
         |    stringField string,
         |    timestampField string,
         |    decimalField decimal(18,2),
         |    dateField string,
         |    charField char(5)
         | )
         | ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$inputPath'
         | INTO TABLE $inputTableName
       """.stripMargin)
  }

  protected def createAndLoadTestTable(tableName: String, inputTableName: String): Unit = {
    sql(
      s"""
         | CREATE TABLE $tableName(
         |    shortField short,
         |    intField int,
         |    bigintField long,
         |    doubleField double,
         |    stringField string,
         |    timestampField timestamp,
         |    decimalField decimal(18,2),
         |    dateField date,
         |    charField char(5)
         | )
         | USING org.apache.spark.sql.CarbonSource
         | OPTIONS ('tableName' '$tableName')
       """.stripMargin)
    sql(
      s"""
         | INSERT INTO TABLE $tableName
         | SELECT shortField, intField, bigintField, doubleField, stringField,
         | from_unixtime(unix_timestamp(timestampField,'yyyy/M/dd')) timestampField, decimalField,
         | cast(to_date(from_unixtime(unix_timestamp(dateField,'yyyy/M/dd'))) as date), charField
         | FROM $inputTableName
       """.stripMargin)
  }

  protected def dropTable(tableName: String): Unit ={
    sql(s"DROP TABLE IF EXISTS $tableName")
  }
}