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
 * Test Class for data loading when there are min long value in int column
 *
 */
class TestLoadDataWithMaxMinBigInt extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("drop table if exists bigint_table_01")
    sql("drop table if exists bigint_table_02")
    sql("drop table if exists bigint_table_03")
  }

  test("test carbon table data loading when the bigint column " +
      "contains min bigint value") {
    sql(
        """
        CREATE TABLE bigint_table_01(imei string,age bigint)
    STORED AS carbondata
    """)
    sql(
        s"""
        LOAD DATA INPATH '$resourcesPath/datawithminbigint.csv'
    INTO table bigint_table_01 options ('DELIMITER'=',',
        'QUOTECHAR'='"')
    """)
    checkAnswer(sql("select age from bigint_table_01"),
        Seq(Row(10), Row(26), Row(10), Row(10), Row(20),
            Row(10), Row(10), Row(10), Row(10), Row(10),
            Row(-9223372036854775808L)))
  }

  test("test carbon table data loading when the bigint column " +
      "contains max bigint value") {
    sql(
        """
        CREATE TABLE bigint_table_02(imei string,age bigint)
    STORED AS carbondata
    """)
    sql(
        s"""
        LOAD DATA INPATH '$resourcesPath/datawithmaxbigint.csv'
    INTO table bigint_table_02 options ('DELIMITER'=',',
        'QUOTECHAR'='"')
    """)
    checkAnswer(sql("select age from bigint_table_02"),
        Seq(Row(10), Row(26), Row(10), Row(10), Row(20),
            Row(10), Row(10), Row(10), Row(10), Row(10),
            Row(9223372036854775807L)))
  }

  test("test carbon table data loading when the bigint column " +
      "contains min and max bigint value") {
    sql(
        """
        CREATE TABLE bigint_table_03(imei string,age bigint)
    STORED AS carbondata
    """)
    sql(
        s"""
        LOAD DATA INPATH '$resourcesPath/datawithmaxminbigint.csv'
    INTO table bigint_table_03 options ('DELIMITER'=',',
        'QUOTECHAR'='"')
    """)
    checkAnswer(sql("select age from bigint_table_03"),
        Seq(Row(10), Row(26), Row(10), Row(10), Row(20),
            Row(10), Row(10), Row(10), Row(10), Row(10),
            Row(-9223372036854775808L), Row(9223372036854775807L)))
  }
  override def afterAll {
    sql("drop table if exists bigint_table_01")
    sql("drop table if exists bigint_table_02")
    sql("drop table if exists bigint_table_03")
  }
}
