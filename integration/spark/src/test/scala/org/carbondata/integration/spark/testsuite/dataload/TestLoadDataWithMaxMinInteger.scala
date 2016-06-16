/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.integration.spark.testsuite.dataload

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
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
        STORED BY 'org.apache.carbondata.format'
      """)
    sql(
      """
        LOAD DATA INPATH './src/test/resources/datawithmininteger.csv'
        INTO table integer_table_01 options ('DELIMITER'=',',
        'QUOTECHAR'='"', 'FILEHEADER'= 'imei,age')
      """)
    checkAnswer(sql("select age from integer_table_01"),
      Seq(Row(10.0), Row(26.0), Row(10.0), Row(10.0), Row(20.0),
        Row(10.0), Row(10.0), Row(10.0), Row(10.0), Row(10.0),
        Row(-2147483648.0)))
  }

  test("test carbon table data loading when the int column " +
    "contains max integer value") {
    sql(
      """
        CREATE TABLE integer_table_02(imei string,age int)
        STORED BY 'org.apache.carbondata.format'
      """)
    sql(
      """
        LOAD DATA INPATH './src/test/resources/datawithmaxinteger.csv'
        INTO table integer_table_02 options ('DELIMITER'=',',
        'QUOTECHAR'='"', 'FILEHEADER'= 'imei,age')
      """)
    checkAnswer(sql("select age from integer_table_02"),
      Seq(Row(10.0), Row(26.0), Row(10.0), Row(10.0), Row(20.0),
        Row(10.0), Row(10.0), Row(10.0), Row(10.0), Row(10.0),
        Row(2147483647.0)))
  }

  test("test carbon table data loading when the int column " +
    "contains min and max integer value") {
    sql(
      """
        CREATE TABLE integer_table_03(imei string,age int)
        STORED BY 'org.apache.carbondata.format'
      """)
    sql(
      """
        LOAD DATA INPATH './src/test/resources/datawithmaxmininteger.csv'
        INTO table integer_table_03 options ('DELIMITER'=',',
        'QUOTECHAR'='"', 'FILEHEADER'= 'imei,age')
      """)
    checkAnswer(sql("select age from integer_table_03"),
      Seq(Row(10.0), Row(26.0), Row(10.0), Row(10.0), Row(20.0),
        Row(10.0), Row(10.0), Row(10.0), Row(10.0), Row(10.0),
        Row(-2147483648.0), Row(2147483647.0)))
  }
  override def afterAll {
    sql("drop table if exists integer_table_01")
    sql("drop table if exists integer_table_02")
    sql("drop table if exists integer_table_03")
  }
}
