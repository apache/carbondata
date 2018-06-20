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

import java.sql.Date

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
  * Test Class for detailed query on Date datatypes
  *
  *
  */
class DateDataTypeDifferentFormatTest extends QueryTest with BeforeAndAfterAll {

  def createAndLoadWithFormat(csvFilePath:String, dateFormat: String) = {
    sql("drop table if exists dateFormatTable1 ")
    sql(
      "CREATE TABLE if not exists dateFormatTable1 (empno int,doj date, salary int) " +
      "STORED BY 'org.apache.carbondata.format'"
    )

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, dateFormat)

    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE dateFormatTable1 OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')" )
  }

  test("test table data for date format 'yyyy-mm-dd' ") {
    val csvFilePath = s"$resourcesPath/datasamplefordate.csv"
    createAndLoadWithFormat(csvFilePath, "yyyy-mm-dd")
    checkAnswer(
      sql("select doj from dateFormatTable1 where doj is not null"),
      Seq(Row(Date.valueOf("2016-03-14")),
        Row(Date.valueOf("2016-04-14"))
      )
    )
  }

  test("test table data for date format 'YYYY-MM-DD' ") {
    val csvFilePath = s"$resourcesPath/datasamplefordate.csv"
    createAndLoadWithFormat(csvFilePath, "YYYY-MM-DD")
    checkAnswer(
      sql("select doj from dateFormatTable1 where doj is not null"),
      Seq(Row(Date.valueOf("2016-03-14")),
        Row(Date.valueOf("2016-04-14"))
      )
    )
  }

  test("test table data for date format 'YYYY-mm-DD' ") {
    val csvFilePath = s"$resourcesPath/datasamplefordate.csv"
    createAndLoadWithFormat(csvFilePath, "YYYY-mm-DD")

    checkAnswer(
      sql("select doj from dateFormatTable1 where doj is not null"),
      Seq(Row(Date.valueOf("2016-03-14")),
        Row(Date.valueOf("2016-04-14"))
      )
    )
  }

  test("test table data for date format 'yyyy/mm/dd' ") {
    val csvFilePath = s"$resourcesPath/datasamplefordate1.csv"
    createAndLoadWithFormat(csvFilePath, "yyyy/mm/dd")

    checkAnswer(
      sql("select doj from dateFormatTable1 where doj is not null"),
      Seq(Row(Date.valueOf("2016-03-14")), Row(Date.valueOf("2016-04-14"))
      )
    )
  }

  test("test table data for date format 'YYYY/mm/DD' ") {
    val csvFilePath = s"$resourcesPath/datasamplefordate1.csv"
    createAndLoadWithFormat(csvFilePath, "YYYY/mm/DD")

    checkAnswer(
      sql("select doj from dateFormatTable1 where doj is not null"),
      Seq(Row(Date.valueOf("2016-03-14")), Row(Date.valueOf("2016-04-14"))
      )
    )
  }

  override def afterAll {
    sql("drop table dateFormatTable1")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }
}