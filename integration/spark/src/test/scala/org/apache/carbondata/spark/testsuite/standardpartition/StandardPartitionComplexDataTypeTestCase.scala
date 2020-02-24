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

package org.apache.carbondata.spark.testsuite.standardpartition

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * StandardPartitionComplexDataTypeTestCase
 */
class StandardPartitionComplexDataTypeTestCase extends QueryTest with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    printConfiguration()
    dropTable
  }

  override protected def afterAll(): Unit = {
    dropTable
  }

  def dropTable = {
    sql("drop table if exists tbl_complex_p")
    sql("drop table if exists tbl_complex_p_carbondata")
  }

  test("test complex datatype for partition table") {
    val tableName1 = "tbl_complex_p"
    sql(s"drop table if exists $tableName1")
    sql(
      s"""
         | create table $tableName1 (
         | col1 int,
         | col2 string,
         | col3 float,
         | col4 struct<level: string, ratio: float, sub: struct<level: string, ratio: float>>,
         | col5 array<struct<ratio: float>>,
         | col6 map<string, struct<ratio: float>>,
         | col7 date
         | ) """.stripMargin)
    sql(s"""
           | insert into table $tableName1
           | select
           |   1,
           |   'a',
           |   1.1,
           |   struct('b', 1.2, struct('bc', 1.21)),
           |   array(struct(1.3), struct(1.4)),
           |   map('l1', struct(1.5), 'l2', struct(1.6)),
           |   to_date('2019-01-01')
           | """.stripMargin)

    val tableName2 = "tbl_complex_p_carbondata"
    sql(s"drop table if exists $tableName2")
    sql(
      s"""
         | create table $tableName2 (
         | col1 int,
         | col2 string,
         | col3 float,
         | col4 struct<level: string, ratio: float, sub: struct<level: string, ratio: float>>,
         | col5 array<struct<ratio: float>>,
         | col6 map<string, struct<ratio: float>>
         | )
         | stored as carbondata
         | partitioned by (col7 date)
         | """.stripMargin)
    sql(s"insert into table $tableName2 select * from $tableName1")
    checkAnswer(
      sql(
        s"""
           |select
           |  cast(round(col4.ratio, 1) as float),
           |  cast(round(col4.sub.ratio, 2) as float),
           |  cast(round(col5[1].ratio, 1) as float),
           |  cast(round(col6['l1'].ratio, 1) as float)
           | from $tableName2
           |""".stripMargin),
      sql(
        s"""
           |select
           |  col4.ratio, col4.sub.ratio,
           |  col5[1].ratio,
           |  col6['l1'].ratio
           | from $tableName1
           |""".stripMargin)
    )
  }
}
