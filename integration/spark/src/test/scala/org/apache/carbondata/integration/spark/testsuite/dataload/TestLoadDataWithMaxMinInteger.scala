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

package org.apache.carbondata.integration.spark.testsuite.dataload

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll
/**
 * Test Class for data loading when there are min integer value in int column
 *
 */
class TestLoadDataWithMaxMinInteger extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("drop table if exists integer_table_01")
    sql("drop table if exists integer_table_02")
    sql("drop table if exists integer_table_03")
  }
  test("test carbon table data loading when the int column " +
    "contains min integer value") {
    sql(
      """
        CREATE TABLE integer_table_01(imei string,age int)
        STORED AS carbondata
      """)
    sql(
      s"""
        LOAD DATA INPATH '$resourcesPath/datawithmininteger.csv'
        INTO table integer_table_01 options ('DELIMITER'=',',
        'QUOTECHAR'='"')
      """)
    checkAnswer(sql("select age from integer_table_01"),
      Seq(Row(10), Row(26), Row(10), Row(10), Row(20),
        Row(10), Row(10), Row(10), Row(10), Row(10),
        Row(-2147483648)))
  }

  test("test carbon table data loading when the int column " +
    "contains max integer value") {
    sql(
      """
        CREATE TABLE integer_table_02(imei string,age int)
        STORED AS carbondata
      """)
    sql(
      s"""
        LOAD DATA INPATH '$resourcesPath/datawithmaxinteger.csv'
        INTO table integer_table_02 options ('DELIMITER'=',',
        'QUOTECHAR'='"')
      """)
    checkAnswer(sql("select age from integer_table_02"),
      Seq(Row(10), Row(26), Row(10), Row(10), Row(20),
        Row(10), Row(10), Row(10), Row(10), Row(10),
        Row(2147483647)))
  }

  test("test carbon table data loading when the int column " +
    "contains min and max integer value") {
    sql(
      """
        CREATE TABLE integer_table_03(imei string,age int)
        STORED AS carbondata
      """)
    sql(
      s"""
        LOAD DATA INPATH '$resourcesPath/datawithmaxmininteger.csv'
        INTO table integer_table_03 options ('DELIMITER'=',',
        'QUOTECHAR'='"')
      """)
    checkAnswer(sql("select age from integer_table_03"),
      Seq(Row(10), Row(26), Row(10), Row(10), Row(20),
        Row(10), Row(10), Row(10), Row(10), Row(10),
        Row(-2147483648), Row(2147483647)))
  }
  override def afterAll {
    sql("drop table if exists integer_table_01")
    sql("drop table if exists integer_table_02")
    sql("drop table if exists integer_table_03")
  }
}
