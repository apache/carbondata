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
package org.apache.carbondata.spark.testsuite.secondaryindex

import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.secondaryindex.joins.BroadCastSIFilterPushJoin
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants

/**
 * test class for verifying the OR filter pushDown filter to SI table
 */
class TestSecondaryIndexForORFilterPushDown extends QueryTest with BeforeAndAfterAll {

  private def dropTables: Unit = {
    sql("drop index if exists index_i1 on or_filter_pushDownValidation")
    sql("drop index if exists index_i2 on or_filter_pushDownValidation")
    sql("drop index if exists index_i3 on or_filter_pushDownValidation")
    sql("drop index if exists index_i4 on or_filter_pushDownValidation")
    sql("drop index if exists index_i5 on or_filter_pushDownValidation")
    sql("drop table if exists or_filter_pushDownValidation")
  }

  override def beforeAll: Unit = {
    dropTables
    // create table
    sql("CREATE table or_filter_pushDownValidation (empno int,empname String, " +
        "designation String, doj Timestamp, workgroupcategory int, " +
        "workgroupcategoryname String, deptno string, deptname String, projectcode int, " +
        "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
        "utilization int,salary int) STORED AS carbondata")
    // load table
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO " +
        "TABLE or_filter_pushDownValidation OPTIONS('DELIMITER'=',', " +
        "'BAD_RECORDS_LOGGER_ENABLE'='FALSE','BAD_RECORDS_ACTION'='FORCE')")
    // create index tables
    sql("create index index_i1 on table or_filter_pushDownValidation (workgroupcategoryname, empname) AS 'carbondata'")
    sql("create index index_i2 on table or_filter_pushDownValidation (designation) AS 'carbondata'")
    sql("create index index_i3 on table or_filter_pushDownValidation (workgroupcategoryname) AS 'carbondata'")
    sql("create index index_i4 on table or_filter_pushDownValidation (deptno) AS 'carbondata'")
    sql("create index index_i5 on table or_filter_pushDownValidation (deptname) AS 'carbondata'")
  }

  test("test OR filter pushdown when left and right subtree have index table") {
    val query = sql(
      "select count(*) from or_filter_pushDownValidation where empname='pramod' OR designation='TL'")
    val df = query.queryExecution.sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(query, Row(2))
  }

  test("test OR filter pushdown when root node is AND filter and right subtree does not have index table on it") {
    val query = sql(
      "select count(*) from or_filter_pushDownValidation where (empname='pramod' OR designation='TL') AND empno='13'")
    val df = query.queryExecution.sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(query, Row(0))
  }

  test("test OR filter pushdown when root node is OR filter and right subtree does not have index table on it") {
    val query = sql(
      "select count(*) from or_filter_pushDownValidation where empname='pramod' OR designation='TL' OR empno='13'")
    val df = query.queryExecution.sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(query, Row(3))
  }

  test("test OR filter pushdown when root node is OR filter and right subtree has NI applied to it") {
    val query = sql(
      "select count(*) from or_filter_pushDownValidation where empname='pramod' OR NI(designation='TL')")
    val df = query.queryExecution.sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(query, Row(2))
  }

  test("test OR filter pushdown when root node is OR filter and 2 filter columns have one index table") {
    val query = sql(
      "select count(*) from or_filter_pushDownValidation where empname='pramod' OR (designation='TL' OR workgroupcategoryname='manager')")
    val df = query.queryExecution.sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(query, Row(4))
  }

  test("test OR filter pushdown when root node is AND filter and 2 filter columns have one index table") {
    val query = sql(
      "select count(*) from or_filter_pushDownValidation where empname='pramod' OR (designation='TL' AND workgroupcategoryname='manager')")
    val df = query.queryExecution.sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(query, Row(1))
  }

  test("test filter pushdown with all AND filters") {
    val query = sql(
      "select count(*) from or_filter_pushDownValidation where empname='pramod' AND designation='SE' AND workgroupcategoryname='developer'")
    val df = query.queryExecution.sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(query, Row(1))
  }

  test("test OR filter pushdown when root node is AND filter and 2 filter columns have one index table with different combination") {
    val query = sql(
      "select count(*) from or_filter_pushDownValidation where (empname='pramod' OR designation='TL') AND workgroupcategoryname='tester'")
    val df = query.queryExecution.sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(query, Row(1))
  }

  test("test filter pushdown with all OR filters") {
    val query = sql(
      "select count(*) from or_filter_pushDownValidation where designation='SE' OR empname='pramod' OR workgroupcategoryname='developer'")
    val df = query.queryExecution.sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(query, Row(5))
  }

  test("test filter pushdown for starts with condition with all OR filters") {
    try {
      CarbonProperties.getInstance
        .addProperty(CarbonCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING, "false")
      val query = sql(
        "select count(*) from or_filter_pushDownValidation where designation like'SS%' OR empname like'pr%'")

      val df = query.queryExecution.sparkPlan
      // even though like filter pushDown is disabled to SI, still pushDown should happen because
      // condition has starts with
      if (!isFilterPushedDownToSI(df)) {
        assert(false)
      } else {
        assert(true)
      }
      checkAnswer(query, Row(3))
    } finally {
      CarbonProperties.getInstance
        .addProperty(CarbonCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING, "true")
    }
  }

  test("test filter pushdown with all 4 OR filters") {
    val query = sql(
      "select count(*) from or_filter_pushDownValidation where designation='SE' OR empname='pramod' OR workgroupcategoryname='developer' OR deptno='14'")
    val df = query.queryExecution.sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(query, Row(7))
  }

  test("test filter pushdown with all 4 OR filters and 1 and filter") {
    val query = sql(
      "select count(*) from or_filter_pushDownValidation where designation='SE' OR empname='pramod' OR workgroupcategoryname='developer' OR deptno='14' and deptname='network'")
    val df = query.queryExecution.sparkPlan
    query.show(false)
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(query, Row(5))
  }

  test("test query with database name in query when current database is different") {
    sql("create database if not exists diff_database")
    sql("use diff_database")
    val query = sql(
      "select count(*) from default.or_filter_pushDownValidation where designation='SE' OR empname='pramod' OR workgroupcategoryname='developer' OR deptno='14' and deptname='network'")
    val df = query.queryExecution.sparkPlan
    query.show(false)
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(query, Row(5))
    sql("drop database if exists diff_database")
    sql("use default")
  }

  override def afterAll: Unit = {
    dropTables
  }

  /**
   * Method to check whether the filter is push down to SI table or not
   *
   * @param sparkPlan
   * @return
   */
  def isFilterPushedDownToSI(sparkPlan: SparkPlan): Boolean = {
    var isValidPlan = false
    sparkPlan.transform {
      case broadCastSIFilterPushDown: BroadCastSIFilterPushJoin =>
        isValidPlan = true
        broadCastSIFilterPushDown
    }
    isValidPlan
  }

}
