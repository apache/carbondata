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
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test Class for aggregate query on Integer datatypes
 *
 */
class IntegerDataTypeTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("DROP TABLE IF EXISTS integertypetableAgg")
    sql("CREATE TABLE integertypetableAgg (empno int, workgroupcategory string, deptno int, projectcode int, attendance int) STORED BY 'org.apache.carbondata.format'")
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
        | STORED BY 'org.apache.carbondata.format'
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
      CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_LOADING, "true"
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
        | STORED BY 'org.apache.carbondata.format'
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

  test("short int as target type in deltaIntegerCodec") {
    sql(
      """
        | DROP TABLE IF EXISTS short_int_target_table
      """.stripMargin)

    //begin_time column will be encoded by deltaIntegerCodec
    sql(
      """
        | CREATE TABLE short_int_target_table
        | (begin_time bigint, name string)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$resourcesPath/short_int_as_target_type.csv'
         | INTO TABLE short_int_target_table
      """.stripMargin)

    checkAnswer(
      sql("select begin_time from short_int_target_table"),
      Seq(Row(1497376581), Row(1497423838))
    )

    sql(
      """
        | DROP TABLE short_int_target_table
      """.stripMargin)
  }
  
  override def afterAll {
    sql("drop table if exists integertypetableAgg")
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_LOADING,
      CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_LOADING_DEFAULT
    )
  }
}
