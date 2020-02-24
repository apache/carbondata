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
package org.apache.carbondata.integration.spark.testsuite.primitiveTypes

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for filter query on Float datatypes
 */
class FloatDataTypeTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("DROP TABLE IF EXISTS datatype_float_hive")
    sql("DROP TABLE IF EXISTS datatype_float_byte")
    sql("DROP TABLE IF EXISTS tfloat")
    sql("""
           CREATE TABLE IF NOT EXISTS tfloat
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int,rating float)
           STORED AS carbondata
           """)
    sql(s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/floatSample.csv' into table tfloat
           """)

  }

  test("select row whose rating is more than 2.8 from tfloat") {
    checkAnswer(
      sql("SELECT ID FROM tfloat where rating>2.8"),
      Seq(Row(6)))
  }

  test("select row whose rating is 3.5 from tfloat") {
    checkAnswer(
      sql("SELECT ID FROM tfloat where rating=3.5"),
      Seq(Row(6)))
  }

  test("select sum of rating column from tfloat") {
    checkAnswer(
      sql("SELECT sum(rating) FROM tfloat"),
      Seq(Row(24.56)))
  }

  test("test when float range exceeds") {
    sql("create table datatype_float_hive(f float, b byte)")
    sql("insert into datatype_float_hive select 1.7976931348623157E308,-127")
    sql("create table datatype_float_byte(f float, b byte) using carbon")
    sql("insert into datatype_float_byte select 1.7976931348623157E308,-127")
    checkAnswer(
      sql("SELECT f FROM datatype_float_byte"),
      sql("SELECT f FROM datatype_float_hive"))
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS tfloat")
    sql("DROP TABLE IF EXISTS datatype_float_byte")
    sql("DROP TABLE IF EXISTS datatype_float_hive")
  }
}