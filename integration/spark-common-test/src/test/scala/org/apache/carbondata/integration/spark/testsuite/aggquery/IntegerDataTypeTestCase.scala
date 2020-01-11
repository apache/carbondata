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

package org.apache.carbondata.spark.testsuite.aggquery


import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

/**
 * Test Class for aggregate query on Integer datatypes
 *
 */
class IntegerDataTypeTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("DROP TABLE IF EXISTS integertypetableAgg")
    sql("DROP TABLE IF EXISTS short_table")
    sql("CREATE TABLE integertypetableAgg (empno int, workgroupcategory string, deptno int, projectcode int, attendance int) STORED AS carbondata")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE integertypetableAgg OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""")
  }

  test("select empno from integertypetableAgg") {
    checkAnswer(
      sql("select empno from integertypetableAgg"),
      Seq(Row(11), Row(12), Row(13), Row(14), Row(15), Row(16), Row(17), Row(18), Row(19), Row(20)))
  }

  test("short int table boundary test, safe column page") {
    sql(
      """
        | DROP TABLE IF EXISTS short_int_table
      """.stripMargin)
    // value column is less than short int, value2 column is bigger than short int
    sql(
      """
        | CREATE TABLE short_int_table
        | (value int, value2 int, name string)
        | STORED AS carbondata
      """.stripMargin)
    sql(
      s"""
        | LOAD DATA LOCAL INPATH '$resourcesPath/shortintboundary.csv'
        | INTO TABLE short_int_table
      """.stripMargin)
    checkAnswer(
      sql("select value from short_int_table"),
      Seq(Row(0), Row(127), Row(128), Row(-127), Row(-128), Row(32767), Row(-32767), Row(32768), Row(-32768), Row(65535),
        Row(-65535), Row(8388606), Row(-8388606), Row(8388607), Row(-8388607), Row(0), Row(0), Row(0), Row(0))
    )
    checkAnswer(
      sql("select value2 from short_int_table"),
      Seq(Row(0), Row(0), Row(0), Row(0), Row(0), Row(0), Row(0), Row(0), Row(0), Row(0),
        Row(0), Row(0), Row(0), Row(0), Row(0), Row(8388608), Row(-8388608), Row(8388609), Row(-8388609))
    )
    sql(
      """
        | DROP TABLE short_int_table
      """.stripMargin)
  }

  test("short int table boundary test, unsafe column page") {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true"
    )
    sql(
      """
        | DROP TABLE IF EXISTS short_int_table
      """.stripMargin)
    // value column is less than short int, value2 column is bigger than short int
    sql(
      """
        | CREATE TABLE short_int_table
        | (value int, value2 int, name string)
        | STORED AS carbondata
      """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$resourcesPath/shortintboundary.csv'
         | INTO TABLE short_int_table
      """.stripMargin)
    checkAnswer(
      sql("select value from short_int_table"),
      Seq(Row(0), Row(127), Row(128), Row(-127), Row(-128), Row(32767), Row(-32767), Row(32768), Row(-32768), Row(65535),
        Row(-65535), Row(8388606), Row(-8388606), Row(8388607), Row(-8388607), Row(0), Row(0), Row(0), Row(0))
    )
    checkAnswer(
      sql("select value2 from short_int_table"),
      Seq(Row(0), Row(0), Row(0), Row(0), Row(0), Row(0), Row(0), Row(0), Row(0), Row(0),
        Row(0), Row(0), Row(0), Row(0), Row(0), Row(8388608), Row(-8388608), Row(8388609), Row(-8388609))
    )
    sql(
      """
        | DROP TABLE short_int_table
      """.stripMargin)
  }

  test("test all codecs") {
    sql(
      """
        | DROP TABLE IF EXISTS all_encoding_table
      """.stripMargin)

    //begin_time column will be encoded by deltaIntegerCodec
    sql(
      """
        | CREATE TABLE all_encoding_table
        | (begin_time bigint, name string,begin_time1 long,begin_time2 long,begin_time3 long,
        | begin_time4 long,begin_time5 int,begin_time6 int,begin_time7 int,begin_time8 short,
        | begin_time9 bigint,begin_time10 bigint,begin_time11 bigint,begin_time12 int,
        | begin_time13 int,begin_time14 short,begin_time15 double,begin_time16 double,
        | begin_time17 double,begin_time18 double,begin_time19 int,begin_time20 double)
        | STORED AS carbondata
      """.stripMargin.replaceAll(System.lineSeparator, ""))

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$resourcesPath/encoding_types.csv'
         | INTO TABLE all_encoding_table
      """.stripMargin)

    checkAnswer(
      sql("select begin_time from all_encoding_table"),
      sql("select begin_time from all_encoding_table")
    )

    val ff = BigInt(2147484000L)
    checkAnswer(
      sql("select begin_time,begin_time1,begin_time2,begin_time3,begin_time4,begin_time5,begin_time6,begin_time7,begin_time8,begin_time9,begin_time10,begin_time11,begin_time12,begin_time13,begin_time14,begin_time15,begin_time16,begin_time17,begin_time18,begin_time19,begin_time20 from all_encoding_table"),
      Seq(Row(1497376581,10000,8388600,125,1497376581,8386600,10000,100,125,1497376581,1497423738,2139095000,1497376581,1497423738,32000,123.4,11.1,3200.1,214744460.2,1497376581,1497376581),
        Row(1497408581,32000,45000,25,10000,55000,32000,75,35,1497423838,1497423838,ff,1497423838,1497423838,31900,838860.7,12.3,127.1,214748360.2,1497408581,1497408581))
    )

    sql(
      """
        | DROP TABLE all_encoding_table
      """.stripMargin)
  }


  test("Create a table that contains short data type") {
    sql("CREATE TABLE if not exists short_table(col1 short, col2 BOOLEAN) STORED AS carbondata")

    sql("insert into short_table values(1,true)")
    sql("insert into short_table values(11,false)")
    sql("insert into short_table values(211,false)")
    sql("insert into short_table values(3111,true)")
    sql("insert into short_table values(31111,false)")
    sql("insert into short_table values(411111,false)")
    sql("insert into short_table values(5111111,true)")

    checkAnswer(
      sql("select count(*) from short_table"),
      Row(7)
    )
    sql("DROP TABLE IF EXISTS short_table")
  }

  override def afterAll {
    sql("drop table if exists integertypetableAgg")
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
      CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_DEFAULT
    )
  }
}
