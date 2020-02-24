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

package org.apache.carbondata.spark.testsuite.directdictionary

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.TimeStampGranularityConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

/**
  * Test Class for detailed query on timestamp datatypes
  */
class TimestampDataTypeDirectDictionaryWithNoDictTestCase extends QueryTest with BeforeAndAfterAll {
  var hiveContext: HiveContext = _

  override def beforeAll {
    CarbonProperties.getInstance()
      .addProperty(TimeStampGranularityConstants.CARBON_CUTOFF_TIMESTAMP, "2000-12-13 02:10.00.0")
    CarbonProperties.getInstance()
      .addProperty(TimeStampGranularityConstants.CARBON_TIME_GRANULARITY,
        TimeStampGranularityConstants.TIME_GRAN_SEC.toString
      )
    CarbonProperties.getInstance().addProperty("carbon.direct.dictionary", "true")
    sql(
      """
         CREATE TABLE IF NOT EXISTS directDictionaryTable
        (empno String, doj Timestamp, salary Int)
         STORED AS carbondata"""
    )
    val csvFilePath = s"$resourcesPath/datasample.csv"
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE directDictionaryTable OPTIONS"
        + "('DELIMITER'= ',', 'QUOTECHAR'= '\"')")
  }

  test("select doj from directDictionaryTable") {
    checkAnswer(
      sql("select doj from directDictionaryTable"),
      Seq(Row(Timestamp.valueOf("2016-03-14 15:00:09.0")),
        Row(Timestamp.valueOf("2016-04-14 15:00:09.0")),
        Row(null)
      )
    )
  }


  test("select doj from directDictionaryTable with equals filter") {
    checkAnswer(
      sql("select doj from directDictionaryTable where doj='2016-03-14 15:00:09'"),
      Seq(Row(Timestamp.valueOf("2016-03-14 15:00:09")))
    )

  }

  test("select doj from directDictionaryTable with greater than filter") {
    checkAnswer(
      sql("select doj from directDictionaryTable where doj>'2016-03-14 15:00:09'"),
      Seq(Row(Timestamp.valueOf("2016-04-14 15:00:09")))
    )

  }


  override def afterAll {
    sql("drop table directDictionaryTable")
    CarbonProperties.getInstance().addProperty("carbon.direct.dictionary", "false")
  }
}