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

package org.apache.carbondata.spark.testsuite.createTable

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * test functionality for create table with sort scope
 */
class TestCreateTableWithSortScope extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("use default")
    sql("DROP TABLE IF EXISTS tableWithGlobalSort")
    sql("DROP TABLE IF EXISTS tableWithLocalSort")
    sql("DROP TABLE IF EXISTS tableWithNoSort")
    sql("DROP TABLE IF EXISTS tableWithUnsupportSortScope")
  }

  test("test create table with sort scope in normal cases") {
    sql(
      s"""
         | CREATE TABLE tableWithGlobalSort(
         | intField INT,
         | stringField STRING
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='stringField', 'SORT_SCOPE'='GLOBAL_SORT')
       """.stripMargin)

    checkExistence(sql("DESCRIBE FORMATTED tableWithGlobalSort"), true, "global_sort")

    sql(
      s"""
         | CREATE TABLE tableWithLocalSort(
         | intField INT,
         | stringField STRING
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='stringField', 'SORT_SCOPE'='LOCAL_SORT')
       """.stripMargin)

    sql("DESCRIBE FORMATTED tableWithLocalSort")

    checkExistence(sql("DESCRIBE FORMATTED tableWithLocalSort"), true, "local_sort")

    sql(
      s"""
         | CREATE TABLE tableWithNoSort(
         | intField INT,
         | stringField STRING
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='stringField', 'SORT_SCOPE'='NO_SORT')
       """.stripMargin)

    checkExistence(sql("DESCRIBE FORMATTED tableWithNoSort"), true, "no_sort")
  }

  test("test create table with sort scope in abnormal cases") {
    val exception_unsupported_sortscope: Exception = intercept[Exception] {
      sql(
        s"""
           | CREATE TABLE tableWithUnsupportSortScope(
           | intField INT,
           | stringField STRING
           | )
           | STORED AS carbondata
           | TBLPROPERTIES('SORT_COLUMNS'='stringField', 'SORT_SCOPE'='abc')
       """.stripMargin)
    }
    assert(exception_unsupported_sortscope.getMessage.contains(
      "Passing invalid SORT_SCOPE 'abc', valid SORT_SCOPE are 'NO_SORT'," +
      " 'LOCAL_SORT' and 'GLOBAL_SORT' "))
  }

  override def afterAll: Unit = {
    sql("use default")
    sql("DROP TABLE IF EXISTS tableWithGlobalSort")
    sql("DROP TABLE IF EXISTS tableWithLocalSort")
    sql("DROP TABLE IF EXISTS tableWithNoSort")
    sql("DROP TABLE IF EXISTS tableWithUnsupportSortScope")
    sql("DROP TABLE IF EXISTS tableLoadWithSortScope")
  }
}
