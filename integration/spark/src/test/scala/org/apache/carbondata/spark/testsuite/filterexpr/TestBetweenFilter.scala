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

package org.apache.carbondata.spark.testsuite.filterexpr

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test cases for testing columns having \N or \null values for non numeric columns
 */
class TestBetweenFilter extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("DROP TABLE IF EXISTS carbonTableBetween")

    val csvFilePath = s"$resourcesPath/filter/betweenFilter.csv"
    sql("""
           CREATE TABLE IF NOT EXISTS carbonTableBetween
           (id String, name String, orders int)
           STORED AS carbondata
        """)
    sql(s"""
           LOAD DATA LOCAL INPATH '$csvFilePath' into table carbonTableBetween OPTIONS('BAD_RECORDS_ACTION'='FORCE')
           """)
  }


  test("SELECT id FROM carbonTableBetween where id >= 1") {
    checkAnswer(
      sql("SELECT id FROM carbonTableBetween where id >= 1"),
      Seq(Row("1"), Row("2"), Row("3"), Row("4"), Row("5"), Row("6"), Row("7")))
  }

  test("SELECT id FROM carbonTableBetween where id >= 1 and id < 5") {
    checkAnswer(
      sql("SELECT id FROM carbonTableBetween where id >= 1  and id < 5"),
      Seq(Row("1"), Row("2"), Row("3"), Row("4")))
  }

  test("SELECT id FROM carbonTableBetween where id > 1 and id < 3") {
    checkAnswer(
      sql("SELECT id FROM carbonTableBetween where id > 1 and id < 3"),
      Seq(Row("2")))
  }

  test("SELECT id FROM carbonTableBetween where id between 1 and 3") {
    checkAnswer(
      sql("SELECT id FROM carbonTableBetween where id between 1 and 3"),
      Seq(Row("1"),Row("2"),Row("3")))
  }



  override def afterAll {
    sql("drop table if exists carbonTableBetween")
  }
}