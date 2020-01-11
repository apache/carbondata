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

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
  * Test Class for data loading into table whose name is key word
  *
  */
class TestLoadTblNameIsKeyword extends QueryTest with BeforeAndAfterAll {
  val testData = s"$resourcesPath/dimSample.csv"
  override def beforeAll {
    sql("drop table if exists STRING")
    sql("drop table if exists DoUbLe")
    sql("drop table if exists timestamp")
    sql("""
          CREATE TABLE IF NOT EXISTS STRING
          (id Int, name String, city String)
          STORED AS carbondata
        """)
    sql("""
          CREATE TABLE IF NOT EXISTS DoUbLe
          (id Int, name String, city String)
          STORED AS carbondata
        """)
    sql("""
          CREATE TABLE IF NOT EXISTS timestamp
          (id Int, name String, city String)
          STORED AS carbondata
        """)
  }

  test("test load data whose name is a keyword of data type") {
    sql(s"""
          LOAD DATA LOCAL INPATH '$testData' into table STRING
        """)
    checkAnswer(
      sql("""
            SELECT count(*) from STRING
          """),
      Seq(Row(20)))
  }

  test("test case in-sensitiveness") {
    sql(s"""
          LOAD DATA LOCAL INPATH '$testData' into table DoUbLe
        """)
    checkAnswer(
      sql("""
            SELECT count(*) from DoUbLe
          """),
      Seq(Row(20)))
  }

  test("test other ddl whose table name a keyword of data type") {
    sql("describe timestamp")
    sql(s"""
          LOAD DATA LOCAL INPATH '$testData' into table timestamp
        """)
    sql("show segments for table timestamp")
    sql("delete from table timestamp where segment.starttime before '2099-10-01 18:00:00'")
    sql("clean files for table timestamp")
  }

  override def afterAll {
    sql("drop table STRING")
    sql("drop table DoUbLe")
    sql("drop table timestamp")
  }
}
