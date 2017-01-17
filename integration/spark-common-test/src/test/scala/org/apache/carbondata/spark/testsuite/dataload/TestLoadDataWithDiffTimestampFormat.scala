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

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException

class TestLoadDataWithDiffTimestampFormat extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("DROP TABLE IF EXISTS t3")
    sql("""
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, starttime Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'carbondata'
        """)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
  }

  test("test load data with different timestamp format") {
      sql(s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData1.csv' into table t3
           OPTIONS('dateformat' = 'starttime:yyyy-MM-dd HH:mm:ss')
           """)
      sql(s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData2.csv' into table t3
           OPTIONS('dateformat' = ' date : yyyy-MM-dd , StartTime : yyyy/MM/dd HH:mm:ss')
           """)
      checkAnswer(
        sql("SELECT date FROM t3 WHERE ID = 1"),
        Seq(Row(Timestamp.valueOf("2015-07-23 00:00:00.0")))
      )
      checkAnswer(
        sql("SELECT starttime FROM t3 WHERE ID = 1"),
        Seq(Row(Timestamp.valueOf("2016-07-23 01:01:30.0")))
      )
      checkAnswer(
        sql("SELECT date FROM t3 WHERE ID = 18"),
        Seq(Row(Timestamp.valueOf("2015-07-25 00:00:00.0")))
      )
      checkAnswer(
        sql("SELECT starttime FROM t3 WHERE ID = 18"),
        Seq(Row(Timestamp.valueOf("2016-07-25 02:32:02.0")))
      )
  }

  test("test load data with different timestamp format with wrong setting") {
    try {
      sql(s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData1.csv' into table t3
           OPTIONS('dateformat' = '')
           """)
      assert(false)
    } catch {
      case ex: MalformedCarbonCommandException =>
        assertResult(ex.getMessage)("Error: Option DateFormat is set an empty string.")
      case _: Throwable=> assert(false)
    }

    try {
      sql(s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData1.csv' into table t3
           OPTIONS('dateformat' = 'fasfdas:yyyy/MM/dd')
           """)
      assert(false)
    } catch {
      case ex: MalformedCarbonCommandException =>
        assertResult(ex.getMessage)("Error: Wrong Column Name fasfdas is provided in Option DateFormat.")
      case _: Throwable => assert(false)
    }

    try {
      sql(s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData1.csv' into table t3
           OPTIONS('dateformat' = 'date:  ')
           """)
      assert(false)
    } catch {
      case ex: MalformedCarbonCommandException =>
        assertResult(ex.getMessage)("Error: Option DateFormat is not provided for Column date.")
      case _: Throwable => assert(false)
    }

    try {
      sql(s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData1.csv' into table t3
           OPTIONS('dateformat' = 'date  ')
           """)
      assert(false)
    } catch {
      case ex: MalformedCarbonCommandException =>
        assertResult(ex.getMessage)("Error: Option DateFormat is not provided for Column date  .")
      case _: Throwable => assert(false)
    }

    try {
      sql(s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/timeStampFormatData1.csv' into table t3
           OPTIONS('dateformat' = ':yyyy/MM/dd  ')
           """)
      assert(false)
    } catch {
      case ex: MalformedCarbonCommandException =>
        assertResult(ex.getMessage)("Error: Wrong Column Name  is provided in Option DateFormat.")
      case _: Throwable => assert(false)
    }

  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS t3")
  }
}
