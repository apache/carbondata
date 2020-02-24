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

package org.apache.carbondata.spark.testsuite.bigdecimal

import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

/**
  * Test cases for testing columns having null value
  */
class TestNullAndEmptyFieldsUnsafe extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists carbonTable")
    sql("drop table if exists hiveTable")
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
          CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
        .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
    val csvFilePath = s"$resourcesPath/nullandnonparsableValue.csv"
    sql(
      "CREATE TABLE IF NOT EXISTS carbonTable (ID String, date Timestamp, country String, name " +
          "String, phonetype String, serialname String, salary Decimal(17,2)) STORED AS carbondata"
    )
    sql(
      "create table if not exists hiveTable(ID String, date Timestamp, country String, name " +
          "String, " +
          "phonetype String, serialname String, salary Decimal(17,2))row format delimited fields " +
          "terminated by ','"
    )
    sql(
      "LOAD DATA LOCAL INPATH '" + csvFilePath + "' into table carbonTable OPTIONS " +
          "('FILEHEADER'='ID,date," +
          "country,name,phonetype,serialname,salary')"
    )
    sql(
      "LOAD DATA local inpath '" + csvFilePath + "' INTO table hiveTable"
    )
  }

  test("test detail query on column having null values") {
    checkAnswer(
      sql("select * from carbonTable"),
      sql("select * from hiveTable")
    )
  }

  test("test filter query on column is null") {
    checkAnswer(
      sql("select * from carbonTable where salary is null"),
      sql("select * from hiveTable where salary is null")
    )
  }

  test("test filter query on column is not null") {
    checkAnswer(
      sql("select * from carbonTable where salary is not null"),
      sql("select * from hiveTable where salary is not null")
    )
  }

  test("test filter query on columnValue=null") {
    checkAnswer(
      sql("select * from carbonTable where salary=null"),
      sql("select * from hiveTable where salary=null")
    )
  }

  test("test filter query where date is null") {
    checkAnswer(
      sql("select * from carbonTable where date is null"),
      sql("select * from hiveTable where date is null")
    )
  }

  test("test  subquery on column having null values") {
    checkAnswer(
      sql("select * from (select if(country='china','c', country) test from carbonTable)qq where test is null"),
      sql("select * from (select if(country='china','c', country) test from hiveTable)qq where test is null")
    )
  }

  test("test  subquery on column having not null values") {
    checkAnswer(
      sql("select * from (select if(country='china','c', country) test from carbonTable)qq where test is not null"),
      sql("select * from (select if(country='china','c', country) test from hiveTable)qq where test is not null")
    )
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
        CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_DEFAULT)
    sql("drop table if exists carbonTable")
    sql("drop table if exists hiveTable")
  }
}


