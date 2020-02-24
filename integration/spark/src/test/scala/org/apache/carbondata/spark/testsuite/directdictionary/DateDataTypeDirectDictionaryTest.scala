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
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.common.constants.LoggerAction

/**
  * Test Class for detailed query on timestamp datatypes
  *
  *
  */
class DateDataTypeDirectDictionaryTest extends QueryTest with BeforeAndAfterAll {
  var hiveContext: HiveContext = _

  override def beforeAll {
    try {
      CarbonProperties.getInstance().addProperty("carbon.direct.dictionary", "true")
      CarbonProperties.getInstance().addProperty(
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, LoggerAction.FORCE.name())
      sql("drop table if exists directDictionaryTable ")
      sql(
        "CREATE TABLE if not exists directDictionaryTable (empno int,doj date, " +
          "salary int) " +
          "STORED AS carbondata"
      )

      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
      val csvFilePath = s"$resourcesPath/datasamplefordate.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE directDictionaryTable OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')" )
    } catch {
      case x: Throwable =>
        x.printStackTrace()
        CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
          CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    }
  }

  test("test direct dictionary for not null condition") {
    checkAnswer(
      sql("select doj from directDictionaryTable where doj is not null"),
      Seq(Row(Date.valueOf("2016-03-14")),
        Row(Date.valueOf("2016-04-14"))
      )
    )
  }

  test("test direct dictionary for getting all the values") {
    checkAnswer(
      sql("select doj from directDictionaryTable"),
      Seq(Row(Date.valueOf("2016-03-14")),
        Row(Date.valueOf("2016-04-14")),
        Row(null)
      )
    )
  }

  test("test direct dictionary for not equals condition") {
    checkAnswer(
      sql("select doj from directDictionaryTable where doj != '2016-04-14'"),
      Seq(Row(Date.valueOf("2016-03-14"))
      )
    )
  }

  test("test direct dictionary for null condition") {
    checkAnswer(
      sql("select doj from directDictionaryTable where doj is null"),
      Seq(Row(null)
      )
    )
  }

  test("select doj from directDictionaryTable with equals filter") {
    checkAnswer(
      sql("select doj from directDictionaryTable where doj = '2016-03-14'"),
      Seq(Row(Date.valueOf("2016-03-14")))
    )

  }

  test("select doj from directDictionaryTable with regexp_replace equals filter") {
    checkAnswer(
      sql("select doj from directDictionaryTable where regexp_replace(doj, '-', '/') = '2016/03/14'"),
      Seq(Row(Date.valueOf("2016-03-14")))
    )
  }

  test("select doj from directDictionaryTable with regexp_replace NOT IN filter") {
    checkAnswer(
      sql("select doj from directDictionaryTable where regexp_replace(doj, '-', '/') NOT IN ('2016/03/14')"),
      Seq(Row(Date.valueOf("2016-04-14")))
    )
  }

  test("select doj from directDictionaryTable with greater than filter") {
    checkAnswer(
      sql("select doj from directDictionaryTable where doj > '2016-03-14 00:00:00'"),
      Seq(Row(Date.valueOf("2016-04-14")))
    )
  }

  test("select doj from directDictionaryTable with greater than filter with cast") {
    checkAnswer(
      sql("select doj from directDictionaryTable where doj > date('2016-03-14')"),
      Seq(Row(Date.valueOf("2016-04-14")))
    )
    checkAnswer(
      sql("select doj from directDictionaryTable where doj > cast('2016-03-14' as date)"),
      Seq(Row(Date.valueOf("2016-04-14")))
    )
  }

  test("select count(doj) from directDictionaryTable") {
    checkAnswer(
      sql("select count(doj) from directDictionaryTable"),
      Seq(Row(2))
    )
  }

  override def afterAll {
    sql("drop table directDictionaryTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    CarbonProperties.getInstance().addProperty("carbon.direct.dictionary", "false")
  }
}