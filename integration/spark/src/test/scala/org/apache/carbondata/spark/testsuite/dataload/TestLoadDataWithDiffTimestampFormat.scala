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

package org.apache.carbondata.spark.testsuite.dataload

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException

class TestLoadDataWithDiffTimestampFormat extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, LoggerAction.FORCE.name())

    sql("DROP TABLE IF EXISTS t3")
    sql("""
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date date, starttime Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED AS carbondata
        """)
  }

  test("test load data with different timestamp format") {
      sql(s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData1.csv' into table t3
           OPTIONS('dateformat' = 'yyyy/MM/dd','timestampformat'='yyyy-MM-dd HH:mm:ss')
           """)
      sql(s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData2.csv' into table t3
           OPTIONS('dateformat' = 'yyyy-MM-dd','timestampformat'='yyyy/MM/dd HH:mm:ss')
           """)
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
      checkAnswer(
        sql("SELECT date FROM t3 WHERE ID = 1"),
        Seq(Row(new Date(sdf.parse("2015-07-23").getTime)))
      )
      checkAnswer(
        sql("SELECT starttime FROM t3 WHERE ID = 1"),
        Seq(Row(Timestamp.valueOf("2016-07-23 01:01:30.0")))
      )
      checkAnswer(
        sql("SELECT date FROM t3 WHERE ID = 18"),
        Seq(Row(new Date(sdf.parse("2015-07-25").getTime)))
      )
      checkAnswer(
        sql("SELECT starttime FROM t3 WHERE ID = 18"),
        Seq(Row(Timestamp.valueOf("2016-07-25 02:32:02.0")))
      )
  }

  test("test load data with different timestamp format with wrong setting") {

    val ex = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData1.csv' into table t3
           OPTIONS('dateformat' = 'date')
           """)
    }
    assertResult(ex.getMessage)("Error: Wrong option: date is provided for option DateFormat")

    val ex0 = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData1.csv' into table t3
           OPTIONS('timestampformat' = 'timestamp')
           """)
    }
    assertResult(ex0.getMessage)("Error: Wrong option: timestamp is provided for option TimestampFormat")

    val ex1 = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData1.csv' into table t3
           OPTIONS('dateformat' = 'date:  ')
           """)
    }
    assertResult(ex1.getMessage)("Error: Wrong option: date:   is provided for option DateFormat")

    val ex2 = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData1.csv' into table t3
           OPTIONS('dateformat' = 'date  ')
           """)
    }
    assertResult(ex2.getMessage)("Error: Wrong option: date   is provided for option DateFormat")

    val ex3 = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData1.csv' into table t3
           OPTIONS('dateformat' = 'fasfdas:yyyy/MM/dd')
           """)
    }
    assertResult(ex3.getMessage)("Error: Wrong option: fasfdas:yyyy/MM/dd is provided for option DateFormat")

  }

  test("test load data with date/timestamp format set at table level") {
    sql("DROP TABLE IF EXISTS t3")
    sql(
      """
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date date, starttime Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED AS carbondata TBLPROPERTIES('dateformat'='yyyy/MM/dd',
           'timestampformat'='yyyy-MM-dd HH:mm')
        """)
    sql(
      s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData1.csv' into table t3
           """)
    sql(
      s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData2.csv' into table t3
           OPTIONS('dateformat' = 'yyyy-MM-dd','timestampformat'='yyyy/MM/dd HH:mm:ss')
           """)
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    checkAnswer(
      sql("SELECT starttime FROM t3 WHERE ID = 1"),
      Seq(Row(Timestamp.valueOf("2016-07-23 01:01:00")))
    )
    checkAnswer(
      sql("SELECT starttime FROM t3 WHERE ID = 18"),
      Seq(Row(Timestamp.valueOf("2016-07-25 02:32:02")))
    )
    checkAnswer(
      sql("SELECT date FROM t3 WHERE ID = 1"),
      Seq(Row(new Date(sdf.parse("2015-07-23").getTime)))
    )
    checkAnswer(
      sql("SELECT date FROM t3 WHERE ID = 18"),
      Seq(Row(new Date(sdf.parse("2015-07-25").getTime)))
    )
  }

  test("test load data with date/timestamp format set at different levels") {
    CarbonProperties.getInstance().addProperty(
      CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT, "yyyy/MM/dd")
    CarbonProperties.getInstance().addProperty(
      CarbonLoadOptionConstants.CARBON_OPTIONS_TIMESTAMPFORMAT, "yyyy-MM-dd HH:mm")
    sql("DROP TABLE IF EXISTS t3")
    sql(
      """
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date date, starttime Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED AS carbondata TBLPROPERTIES('dateformat'='yyyy/MM/dd',
           'timestampformat'='yyyy-MM-dd')
        """)
    sql(
      s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData1.csv' into table t3
           """)
    sql(
      s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData2.csv' into table t3
           OPTIONS('dateformat' = 'yyyy-MM-dd','timestampformat'='yyyy/MM/dd HH:mm:ss')
           """)
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    checkAnswer(
      sql("SELECT starttime FROM t3 WHERE ID = 1"),
      Seq(Row(Timestamp.valueOf("2016-07-23 01:01:00")))
    )
    checkAnswer(
      sql("SELECT starttime FROM t3 WHERE ID = 18"),
      Seq(Row(Timestamp.valueOf("2016-07-25 02:32:02")))
    )
    checkAnswer(
      sql("SELECT date FROM t3 WHERE ID = 1"),
      Seq(Row(new Date(sdf.parse("2015-07-23").getTime)))
    )
    checkAnswer(
      sql("SELECT date FROM t3 WHERE ID = 18"),
      Seq(Row(new Date(sdf.parse("2015-07-25").getTime)))
    )
    CarbonProperties.getInstance()
      .removeProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT)
    CarbonProperties.getInstance()
      .removeProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_TIMESTAMPFORMAT)
  }

  test("test insert data with date/timestamp format set at table level") {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "true")
    sql("DROP TABLE IF EXISTS t3")
    sql(
      """
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date date, starttime Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED AS carbondata TBLPROPERTIES('dateformat'='yyyy-MM-dd',
           'timestampformat'='yyyy-MM-dd HH:mm')
        """)
    sql(
      s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData1.csv' into table t3
           OPTIONS('dateformat' = 'yyyy/MM/dd')
           """)
    sql(s"insert into t3 select 11,'2015-7-23','2016-7-23 01:01:30','china','aaa1','phone197'," +
        s"'ASD69643',15000")
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    checkAnswer(
      sql("SELECT starttime FROM t3 WHERE ID = 1"),
      Seq(Row(Timestamp.valueOf("2016-07-23 01:01:00")))
    )
    checkAnswer(
      sql("SELECT starttime FROM t3 WHERE ID = 11"),
      Seq(Row(Timestamp.valueOf("2016-07-23 01:01:00")))
    )
    checkAnswer(
      sql("SELECT date FROM t3 WHERE ID = 1"),
      Seq(Row(new Date(sdf.parse("2015-07-23").getTime)))
    )
    checkAnswer(
      sql("SELECT date FROM t3 WHERE ID = 11"),
      Seq(Row(new Date(sdf.parse("2015-07-23").getTime)))
    )
  }

  test("test alter table set and unset date,timestamp from properties") {
    CarbonProperties.getInstance
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd hh:mm:ss")
    sql("DROP TABLE IF EXISTS t3")
    sql(
      """
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date date, starttime Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED AS carbondata
        """)
    sql("alter table t3 set tblproperties('dateformat'='yyyy/MM/dd'," +
        "'timestampformat'='yyyy-MM-dd HH:mm')")
    sql(
      s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData1.csv' into table t3
           """)
    sql("alter table t3 unset tblproperties('dateformat','timestampformat')")
    sql(
      s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData2.csv' into table t3
           """)
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    checkAnswer(
      sql("SELECT starttime FROM t3 WHERE ID = 1"),
      Seq(Row(Timestamp.valueOf("2016-07-23 01:01:00")))
    )
    checkAnswer(
      sql("SELECT starttime FROM t3 WHERE ID = 18"),
      Seq(Row(Timestamp.valueOf("2016-07-25 02:32:02")))
    )
    checkAnswer(
      sql("SELECT date FROM t3 WHERE ID = 1"),
      Seq(Row(new Date(sdf.parse("2015-07-23").getTime)))
    )
    checkAnswer(
      sql("SELECT date FROM t3 WHERE ID = 18"),
      Seq(Row(new Date(sdf.parse("2015-07-25").getTime)))
    )
  }

  test("test create table with date/timestamp format and check describe formatted") {
    sql("DROP TABLE IF EXISTS t3")
    sql(
      """
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date date, starttime Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED AS carbondata TBLPROPERTIES('dateformat'='yyyy/MM/dd',
           'timestampformat'='yyyy-MM-dd HH:mm')
        """)
    val descTable = sql(s"describe formatted t3").collect
    descTable.find(_.get(0).toString.contains("Date Format")) match {
      case Some(row) => assert(row.get(1).toString.contains("yyyy/MM/dd"))
      case None => assert(false)
    }
    descTable.find(_.get(0).toString.contains("Timestamp Format")) match {
      case Some(row) => assert(row.get(1).toString.contains("yyyy-MM-dd HH:mm"))
      case None => assert(false)
    }
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS t3")
  }
}
