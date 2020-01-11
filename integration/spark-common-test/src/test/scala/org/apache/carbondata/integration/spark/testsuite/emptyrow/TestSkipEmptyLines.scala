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

package org.apache.carbondata.spark.testsuite.singlevaluerow

import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

class TestSkipEmptyLines extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists skipEmptyRowCarbonTable")
  }

  test("test load options with true") {
    sql("drop table if exists skipEmptyRowCarbonTable")
    sql("CREATE TABLE skipEmptyRowCarbonTable (name string, age int) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/emptylines.csv' into table skipEmptyRowCarbonTable " +
        s"OPTIONS('skip_empty_line'='true')")
    checkAnswer(sql("select * from skipEmptyRowCarbonTable"), Seq(Row("a",25),Row("b",22),Row("c",23)))
  }

  test("test load options with false") {
    sql("drop table if exists skipEmptyRowCarbonTable")
    sql("CREATE TABLE skipEmptyRowCarbonTable (name string, age int) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/emptylines.csv' into table skipEmptyRowCarbonTable " +
        s"OPTIONS('skip_empty_line'='false')")
    checkAnswer(sql("select * from skipEmptyRowCarbonTable"),
      Seq(Row("a",25),Row("b",22),Row("c",23),Row(null,null),Row(null,null),Row(null,null)))
  }

  test("test carbonproperties with true") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SKIP_EMPTY_LINE, "true")
    sql("drop table if exists skipEmptyRowCarbonTable")
    sql("CREATE TABLE skipEmptyRowCarbonTable (name string, age int) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/emptylines.csv' into table skipEmptyRowCarbonTable")
    checkAnswer(sql("select * from skipEmptyRowCarbonTable"),
      Seq(Row("a",25),Row("b",22),Row("c",23)))
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SKIP_EMPTY_LINE,
      CarbonCommonConstants.CARBON_SKIP_EMPTY_LINE_DEFAULT)
  }

  test("test carbonproperties with false") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SKIP_EMPTY_LINE, "false")
    sql("drop table if exists skipEmptyRowCarbonTable")
    sql("CREATE TABLE skipEmptyRowCarbonTable (name string, age int) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/emptylines.csv' into table skipEmptyRowCarbonTable")
    checkAnswer(sql("select * from skipEmptyRowCarbonTable"),
      Seq(Row("a",25),Row("b",22),Row("c",23),Row(null,null),Row(null,null),Row(null,null)))
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SKIP_EMPTY_LINE,
      CarbonCommonConstants.CARBON_SKIP_EMPTY_LINE_DEFAULT)
  }

  test("test carbonproperties with false and load options true") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SKIP_EMPTY_LINE, "false")
    sql("drop table if exists skipEmptyRowCarbonTable")
    sql("CREATE TABLE skipEmptyRowCarbonTable (name string, age int) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/emptylines.csv' into table skipEmptyRowCarbonTable " +
        s"OPTIONS('skip_empty_line'='true')")
    checkAnswer(sql("select * from skipEmptyRowCarbonTable"),
      Seq(Row("a",25),Row("b",22),Row("c",23)))
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SKIP_EMPTY_LINE,
      CarbonCommonConstants.CARBON_SKIP_EMPTY_LINE_DEFAULT)
  }

  test("test carbonproperties with true and load options false") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SKIP_EMPTY_LINE, "true")
    sql("drop table if exists skipEmptyRowCarbonTable")
    sql("CREATE TABLE skipEmptyRowCarbonTable (name string, age int) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/emptylines.csv' into table skipEmptyRowCarbonTable " +
        s"OPTIONS('skip_empty_line'='false')")
    checkAnswer(sql("select * from skipEmptyRowCarbonTable"),
      Seq(Row("a",25),Row("b",22),Row("c",23),Row(null,null),Row(null,null),Row(null,null)))
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SKIP_EMPTY_LINE,
      CarbonCommonConstants.CARBON_SKIP_EMPTY_LINE_DEFAULT)
  }

  override def afterAll {
    sql("drop table skipEmptyRowCarbonTable")
  }
}
