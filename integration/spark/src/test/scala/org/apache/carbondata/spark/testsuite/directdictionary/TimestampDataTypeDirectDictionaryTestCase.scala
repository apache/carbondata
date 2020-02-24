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
  *
  *
  */
class TimestampDataTypeDirectDictionaryTest extends QueryTest with BeforeAndAfterAll {
  var hiveContext: HiveContext = _

  override def beforeAll {
    try {
      CarbonProperties.getInstance()
        .addProperty(TimeStampGranularityConstants.CARBON_CUTOFF_TIMESTAMP, "2000-12-13 02:10.00.0")
      CarbonProperties.getInstance()
        .addProperty(TimeStampGranularityConstants.CARBON_TIME_GRANULARITY,
          TimeStampGranularityConstants.TIME_GRAN_SEC.toString
        )
      CarbonProperties.getInstance().addProperty("carbon.direct.dictionary", "true")
      sql("drop table if exists directDictionaryTable")
      sql("drop table if exists directDictionaryTable_hive")
      sql(
        "CREATE TABLE if not exists directDictionaryTable (empno int,doj Timestamp, salary int) " +
          "STORED AS carbondata"
      )

      sql(
        "CREATE TABLE if not exists directDictionaryTable_hive (empno int,doj Timestamp, salary int) " +
          "row format delimited fields terminated by ','"
      )
      val csvFilePath = s"$resourcesPath/datasample.csv"
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE directDictionaryTable OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')")
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE directDictionaryTable_hive")
    } catch {
      case x: Throwable =>
    }
  }

  test("test direct dictionary for not null condition") {
    sql("select doj, empno from directDictionaryTable where doj >= '2016-03-14 15:00:09' and doj < '2016-03-14 15:00:10'").show()
    checkAnswer(
      sql("select doj from directDictionaryTable where doj is not null"),
      Seq(Row(Timestamp.valueOf("2016-03-14 15:00:09.0")),
        Row(Timestamp.valueOf("2016-04-14 15:00:09.0"))
      )
    )
  }

  test("test direct dictionary for getting all the values") {
    checkAnswer(
      sql("select doj from directDictionaryTable"),
      Seq(Row(Timestamp.valueOf("2016-03-14 15:00:09.0")),
        Row(Timestamp.valueOf("2016-04-14 15:00:09.0")),
        Row(null)
      )
    )
  }

  test("test direct dictionary for not equals condition") {
    checkAnswer(
      sql("select doj from directDictionaryTable where doj != '2016-04-14 15:00:09'"),
      Seq(Row(Timestamp.valueOf("2016-03-14 15:00:09"))
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
      sql("select doj from directDictionaryTable where doj='2016-03-14 15:00:09'"),
      Seq(Row(Timestamp.valueOf("2016-03-14 15:00:09")))
    )

  }

  test("select doj from directDictionaryTable with regexp_replace equals filter") {
    checkAnswer(
      sql("select doj from directDictionaryTable where regexp_replace(doj, '-', '/') = '2016/03/14 15:00:09'"),
      Seq(Row(Timestamp.valueOf("2016-03-14 15:00:09")))
    )
  }

  test("select doj from directDictionaryTable with regexp_replace NOT IN filter") {
    checkAnswer(
      sql("select doj from directDictionaryTable where regexp_replace(doj, '-', '/') NOT IN ('2016/03/14 15:00:09')"),
      sql("select doj from directDictionaryTable_hive where regexp_replace(doj, '-', '/') NOT IN ('2016/03/14 15:00:09')")
    )
  }

  test("select doj from directDictionaryTable with greater than filter") {
    checkAnswer(
      sql("select doj from directDictionaryTable where doj>'2016-03-14 15:00:09'"),
      Seq(Row(Timestamp.valueOf("2016-04-14 15:00:09")))
    )
  }

  test("select count(doj) from directDictionaryTable") {
    checkAnswer(
      sql("select count(doj) from directDictionaryTable"),
      Seq(Row(2))
    )
  }

  test("test timestamp with dictionary include and no_inverted index") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_PUSH_ROW_FILTERS_FOR_VECTOR, "true")
    sql("drop table if exists test_timestamp")
    sql("drop table if exists test_timestamp_hive")
    sql(
      "create table test_timestamp(col timestamp) STORED AS carbondata tblproperties" +
      "('no_inverted_index'='col')")
    val csvFilePath = s"$resourcesPath/data_timestamp.csv"
    sql(
      "load data inpath '" + csvFilePath +
      "' into table test_timestamp options('delimiter'='=','quotechar'=''," +
      "'bad_records_action'='force','fileheader'='col')")
    sql(
      "create table test_timestamp_hive(col timestamp) row format delimited fields terminated by " +
      "','")
    sql("load data inpath '" + csvFilePath + "' into table test_timestamp_hive ")
    checkAnswer(sql(
      "select col from test_timestamp where col not between '2014-01-01 18:00:00' and '0'"),
      sql("select col from test_timestamp_hive where col not between '2014-01-01 18:00:00' and " +
          "'0'"))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_PUSH_ROW_FILTERS_FOR_VECTOR,
        CarbonCommonConstants.CARBON_PUSH_ROW_FILTERS_FOR_VECTOR_DEFAULT)
  }

  override def afterAll {
    CarbonProperties.getInstance().addProperty("carbon.direct.dictionary", "false")
    sql("drop table directDictionaryTable")
    sql("drop table directDictionaryTable_hive")
    sql("drop table if exists test_timestamp")
    sql("drop table if exists test_timestamp_hive")
  }
}