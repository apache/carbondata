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

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
  * Test Class for data loading with hive syntax and old syntax
  *
  */
class TestLoadDataWithNoMeasure extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("DROP TABLE IF EXISTS nomeasureTest")
    sql(
      "CREATE TABLE nomeasureTest (empno String, doj String) STORED AS carbondata"
    )
    val testData = s"$resourcesPath/datasample.csv"
    sql("LOAD DATA LOCAL INPATH '" + testData + "' into table nomeasureTest")
  }

  test("test data loading and validate query output") {

    checkAnswer(
      sql("select empno from nomeasureTest"),
      Seq(Row("11"), Row("12"), Row("13"))
    )
  }

  test("test data loading with single dictionary column") {
    sql("DROP TABLE IF EXISTS nomeasureTest_sd")
    sql("CREATE TABLE nomeasureTest_sd (city String) STORED AS carbondata")
    val testData = s"$resourcesPath/datasingleCol.csv"
    sql("LOAD DATA LOCAL INPATH '" + testData + "' into table nomeasureTest_sd options " +
      "('FILEHEADER'='city')"
    )

    checkAnswer(
      sql("select city from nomeasureTest_sd"),
      Seq(Row("CA"), Row("LA"), Row("AD"))
    )
  }

  test("test data loading with single no dictionary column") {
    sql("DROP TABLE IF EXISTS nomeasureTest_sd")
    sql("CREATE TABLE nomeasureTest_sd (city String) STORED AS carbondata ")
    val testData = s"$resourcesPath/datasingleCol.csv"
    sql("LOAD DATA LOCAL INPATH '" + testData + "' into table nomeasureTest_sd options " +
      "('FILEHEADER'='city')"
    )

    checkAnswer(
      sql("select city from nomeasureTest_sd"),
      Seq(Row("CA"), Row("LA"), Row("AD"))
    )
  }

  test("test data loading with single complex struct type column") {
    //only data load check
    sql("DROP TABLE IF EXISTS nomeasureTest_scd")
    sql(
      "CREATE TABLE nomeasureTest_scd (cityDetail struct<cityName:string,cityCode:string>) " +
        "STORED AS carbondata"
    )
    val testData = s"$resourcesPath/datasingleComplexCol.csv"
    sql("LOAD DATA LOCAL INPATH '" + testData + "' into table nomeasureTest_scd options " +
      "('DELIMITER'=',','QUOTECHAR'='\"','FILEHEADER'='cityDetail'," +
      "'COMPLEX_DELIMITER_LEVEL_1'=':','COMPLEX_DELIMITER_LEVEL_2'='$')"
    )
  }

  test("test data loading with single complex array type column") {
    //only data load check
    sql("DROP TABLE IF EXISTS nomeasureTest_scd")
    sql(
      "CREATE TABLE nomeasureTest_scd (cityDetail array<string>) " +
        "STORED AS carbondata"
    )
    val testData = s"$resourcesPath/datasingleComplexCol.csv"
    sql("LOAD DATA LOCAL INPATH '" + testData + "' into table nomeasureTest_scd options " +
      "('DELIMITER'=',','QUOTECHAR'='\"','FILEHEADER'='cityDetail'," +
      "'COMPLEX_DELIMITER_LEVEL_1'=':','COMPLEX_DELIMITER_LEVEL_2'='$')"
    )
  }

  override def afterAll {
    sql("drop table if exists nomeasureTest")
    sql("drop table if exists nomeasureTest_sd")
    sql("drop table if exists nomeasureTest_scd")
  }
}
