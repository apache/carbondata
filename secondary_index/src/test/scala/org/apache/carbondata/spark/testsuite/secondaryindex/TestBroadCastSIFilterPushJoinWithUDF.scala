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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * test cases for testing BroadCastSIFilterPushJoin with udf
 */
class TestBroadCastSIFilterPushJoinWithUDF extends QueryTest with BeforeAndAfterAll {

  val testSecondaryIndexForORFilterPushDown = new TestSecondaryIndexForORFilterPushDown
  var carbonQuery: DataFrame = null
  var hiveQuery: DataFrame = null

  override def beforeAll: Unit = {
    dropTables
    sql("CREATE table udfValidation (empno int, empname String, " +
        "designation String, doj Timestamp, workgroupcategory int, " +
        "workgroupcategoryname String, deptno string, deptname String, projectcode int, " +
        "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
        "utilization int,salary int) STORED as carbondata")
    // load table
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO " +
        "TABLE udfValidation OPTIONS('DELIMITER'=',', " +
        "'BAD_RECORDS_LOGGER_ENABLE'='FALSE','BAD_RECORDS_ACTION'='FORCE')")

    // create index tables
    sql("create index ind_i1 on table udfValidation (workgroupcategoryname,empname) AS 'carbondata'")
    sql("create index ind_i2 on table udfValidation (designation) AS 'carbondata'")
    sql("create index ind_i3 on table udfValidation (workgroupcategoryname) AS 'carbondata'")
    sql("create index ind_i4 on table udfValidation (deptno) AS 'carbondata'")
    sql("create index ind_i5 on table udfValidation (deptname) AS 'carbondata'")

    sql("CREATE TABLE udfHive(empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate timestamp, projectenddate Timestamp, attendance int, utilization int,salary int) row format delimited fields terminated by ','")
    sql(s"LOAD DATA INPATH '$resourcesPath/datawithoutheader.csv' INTO TABLE udfHive")

  }

  override def afterAll: Unit = {
    dropTables
  }

  test("test approx_count_distinct udf") {
    // approx_count_distinct udf
    carbonQuery = sql("select approx_count_distinct(empname), approx_count_distinct(deptname) from udfValidation where empname = 'pramod' or deptname = 'network'")
    hiveQuery = sql("select approx_count_distinct(empname), approx_count_distinct(deptname) from udfHive where empname = 'pramod' or deptname = 'network'")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test collect_list udf") {
    // collect_list udf
    carbonQuery = sql("select collect_list(empname) from udfValidation where empname = 'pramod' or deptname = 'network' or designation='TL'")
    hiveQuery = sql("select collect_list(empname) from udfHive where empname = 'pramod' or deptname = 'network' or designation='TL'")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test collect_set udf") {
    // collect_set udf
    carbonQuery = sql("select collect_set(deptname) from udfValidation where empname = 'pramod' or deptname = 'network' or designation='TL'")
    hiveQuery = sql("select collect_set(deptname) from udfHive where empname = 'pramod' or deptname = 'network' or designation='TL'")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test correlation udf") {
    // corr udf
    carbonQuery = sql("select corr(deptno, empno) from udfValidation where empname = 'pramod' or deptname = 'network' or designation='TL'")
    hiveQuery = sql("select corr(deptno, empno) from udfHive where empname = 'pramod' or deptname = 'network' or designation='TL'")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test covariance population udf") {
    // covar_pop udf
    carbonQuery = sql("select covar_pop(deptno, empno) from udfValidation where empname = 'pramod' or deptname = 'network' or designation='TL'")
    hiveQuery = sql("select covar_pop(deptno, empno) from udfHive where empname = 'pramod' or deptname = 'network' or designation='TL'")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test covariance sample udf") {
    // covar_samp udf
    carbonQuery = sql("select covar_samp(deptno, empno) from udfValidation where empname = 'pramod' or deptname = 'network' or designation='TL'")
    hiveQuery = sql("select covar_samp(deptno, empno) from udfHive where empname = 'pramod' or deptname = 'network' or designation='TL'")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test grouping udf") {
    // grouping udf
    carbonQuery = sql("select grouping(designation), grouping(deptname) from udfValidation where empname = 'pramod' or deptname = 'network' or designation='TL' group by designation, deptname with ROLLUP")
    hiveQuery = sql("select grouping(designation), grouping(deptname) from udfHive where empname = 'pramod' or deptname = 'network' or designation='TL' group by designation, deptname with ROLLUP")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test mean udf") {
    // mean udf
    carbonQuery = sql("select mean(deptno), mean(empno) from udfValidation where empname = 'pramod' or deptname = 'network' or designation='TL'")
    hiveQuery = sql("select mean(deptno), mean(empno) from udfHive where empname = 'pramod' or deptname = 'network' or designation='TL'")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test skewness udf") {
    // skewness udf
    carbonQuery = sql("select skewness(deptno), skewness(empno) from udfValidation where empname = 'pramod' or deptname = 'network' or designation='TL'")
    hiveQuery = sql("select skewness(deptno), skewness(empno) from udfHive where empname = 'pramod' or deptname = 'network' or designation='TL'")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test standard deviation udf") {
    // stddev udf
    carbonQuery = sql("select stddev(deptno), stddev(empno) from udfValidation where empname = 'pramod' or deptname = 'network' or designation='TL'")
    hiveQuery = sql("select stddev(deptno), stddev(empno) from udfHive where empname = 'pramod' or deptname = 'network' or designation='TL'")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test standard deviation population udf") {
    // stddev_pop udf
    carbonQuery = sql("select stddev_pop(deptno), stddev_pop(empno) from udfValidation where empname = 'pramod' or deptname = 'network' or designation='TL'")
    hiveQuery = sql("select stddev_pop(deptno), stddev_pop(empno) from udfHive where empname = 'pramod' or deptname = 'network' or designation='TL'")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test standard deviation sample udf") {
    // stddev_samp udf
    carbonQuery = sql("select stddev_samp(deptno), stddev_samp(empno) from udfValidation where empname = 'pramod' or deptname = 'network' or designation='TL'")
    hiveQuery = sql("select stddev_samp(deptno), stddev_samp(empno) from udfHive where empname = 'pramod' or deptname = 'network' or designation='TL'")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test variance population udf") {
    // var_pop udf
    carbonQuery = sql("select var_pop(deptno), var_pop(empno) from udfValidation where empname = 'pramod' or deptname = 'network' or designation='TL'")
    hiveQuery = sql("select var_pop(deptno), var_pop(empno) from udfHive where empname = 'pramod' or deptname = 'network' or designation='TL'")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test variance sample udf") {
    // var_samp udf
    carbonQuery = sql("select var_samp(deptno), var_samp(empno) from udfValidation where empname = 'pramod' or deptname = 'network' or designation='TL'")
    hiveQuery = sql("select var_samp(deptno), var_samp(empno) from udfHive where empname = 'pramod' or deptname = 'network' or designation='TL'")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test variance udf") {
    // variance udf
    carbonQuery = sql("select variance(deptno), variance(empno) from udfValidation where empname = 'pramod' or deptname = 'network' or designation='TL'")
    hiveQuery = sql("select variance(deptno), variance(empno) from udfHive where empname = 'pramod' or deptname = 'network' or designation='TL'")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test coalesce, conv and substring") {
    // COALESCE, CONV and SUBSTRING udf
    carbonQuery = sql("select COALESCE(CONV(substring(empname, 3, 2), 16, 10), ''), COALESCE(CONV(substring(deptname, 3, 2), 16, 10), '') from udfValidation where empname = 'pramod' or deptname = 'network' or designation='TL'")
    hiveQuery = sql("select COALESCE(CONV(substring(empname, 3, 2), 16, 10), ''), COALESCE(CONV(substring(deptname, 3, 2), 16, 10), '') from udfHive where empname = 'pramod' or deptname = 'network' or designation='TL'")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test all the above udfs") {
    // all the above udf
    carbonQuery = sql(
      "select approx_count_distinct(empname), approx_count_distinct(deptname), collect_list" +
      "(empname), collect_set(deptname), corr(deptno, empno), covar_pop(deptno, empno), " +
      "covar_samp(deptno, empno), grouping(designation), grouping(deptname), mean(deptno), mean" +
      "(empno),skewness(deptno), skewness(empno), stddev(deptno), stddev(empno), stddev_pop" +
      "(deptno), stddev_pop(empno), stddev_samp(deptno), stddev_samp(empno), var_pop(deptno), " +
      "var_pop(empno), var_samp(deptno), var_samp(empno), variance(deptno), variance(empno), " +
      "COALESCE(CONV(substring(empname, 3, 2), 16, 10), ''), COALESCE(CONV(substring(deptname, 3," +
      " 2), 16, 10), '') from udfValidation where empname = 'pramod' or deptname = 'network' or " +
      "designation='TL' group by designation, deptname, empname with ROLLUP")
    hiveQuery = sql(
      "select approx_count_distinct(empname), approx_count_distinct(deptname), collect_list" +
      "(empname), collect_set(deptname), corr(deptno, empno), covar_pop(deptno, empno), " +
      "covar_samp(deptno, empno), grouping(designation), grouping(deptname), mean(deptno), mean" +
      "(empno),skewness(deptno), skewness(empno), stddev(deptno), stddev(empno), stddev_pop" +
      "(deptno), stddev_pop(empno), stddev_samp(deptno), stddev_samp(empno), var_pop(deptno), " +
      "var_pop(empno), var_samp(deptno), var_samp(empno), variance(deptno), variance(empno), " +
      "COALESCE(CONV(substring(empname, 3, 2), 16, 10), ''), COALESCE(CONV(substring(deptname, 3," +
      " 2), 16, 10), '') from udfHive where empname = 'pramod' or deptname = 'network' or " +
      "designation='TL' group by designation, deptname, empname with ROLLUP")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test alias of all the above udf") {
    // alias all the above udf
    carbonQuery = sql(
      "select approx_count_distinct(empname) as c1, approx_count_distinct(deptname) as c2, collect_list" +
      "(empname) as c3, collect_set(deptname) as c4, corr(deptno, empno) as c5, covar_pop(deptno, empno) as c6, " +
      "covar_samp(deptno, empno) as c7, grouping(designation) as c8, grouping(deptname) as c9, mean(deptno) as c10, mean" +
      "(empno) as c11,skewness(deptno) as c12, skewness(empno) as c13, stddev(deptno) as c14, stddev(empno) as c15, stddev_pop" +
      "(deptno) as c16, stddev_pop(empno) as c17, stddev_samp(deptno) as c18, stddev_samp(empno) as c18, var_pop(deptno) as c19, " +
      "var_pop(empno) as c20, var_samp(deptno) as c21, var_samp(empno) as c22, variance(deptno) as c23, variance(empno) as c24, " +
      "COALESCE(CONV(substring(empname, 3, 2), 16, 10), '') as c25, COALESCE(CONV(substring(deptname, 3," +
      " 2), 16, 10), '') as c26 from udfValidation where empname = 'pramod' or deptname = 'network' or " +
      "designation='TL' group by designation, deptname, empname with ROLLUP")
    hiveQuery = sql(
      "select approx_count_distinct(empname) as c1, approx_count_distinct(deptname) as c2, collect_list" +
      "(empname) as c3, collect_set(deptname) as c4, corr(deptno, empno) as c5, covar_pop(deptno, empno) as c6, " +
      "covar_samp(deptno, empno) as c7, grouping(designation) as c8, grouping(deptname) as c9, mean(deptno) as c10, mean" +
      "(empno) as c11,skewness(deptno) as c12, skewness(empno) as c13, stddev(deptno) as c14, stddev(empno) as c15, stddev_pop" +
      "(deptno) as c16, stddev_pop(empno) as c17, stddev_samp(deptno) as c18, stddev_samp(empno) as c18, var_pop(deptno) as c19, " +
      "var_pop(empno) as c20, var_samp(deptno) as c21, var_samp(empno) as c22, variance(deptno) as c23, variance(empno) as c24, " +
      "COALESCE(CONV(substring(empname, 3, 2), 16, 10), '') as c25, COALESCE(CONV(substring(deptname, 3," +
      " 2), 16, 10), '') as c26 from udfHive where empname = 'pramod' or deptname = 'network' or " +
      "designation='TL' group by designation, deptname, empname with ROLLUP")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test cast of all the above udf") {
    // cast all the above udf
    carbonQuery = sql(
      "select cast(approx_count_distinct(empname) as string), cast(approx_count_distinct(deptname) as string), collect_list" +
      "(empname), collect_set(deptname), cast(corr(deptno, empno) as string), cast(covar_pop(deptno, empno) as string), " +
      "cast(covar_samp(deptno, empno) as string), cast(grouping(designation) as string), cast(grouping(deptname) as string), cast(mean(deptno) as string), cast(mean" +
      "(empno) as string),cast(skewness(deptno) as string), cast(skewness(empno) as string), cast(stddev(deptno) as string), cast(stddev(empno) as string), cast(stddev_pop" +
      "(deptno) as string), cast(stddev_pop(empno) as string), cast(stddev_samp(deptno) as string), cast(stddev_samp(empno) as string), cast(var_pop(deptno) as string), " +
      "cast(var_pop(empno) as string), cast(var_samp(deptno) as string), cast(var_samp(empno) as string), cast(variance(deptno) as string), cast(variance(empno) as string), " +
      "COALESCE(CONV(substring(empname, 3, 2), 16, 10), ''), COALESCE(CONV(substring(deptname, 3," +
      " 2), 16, 10), '') from udfValidation where empname = 'pramod' or deptname = 'network' or " +
      "designation='TL' group by designation, deptname, empname with ROLLUP")
    hiveQuery = sql(
      "select cast(approx_count_distinct(empname) as string), cast(approx_count_distinct(deptname) as string), collect_list" +
      "(empname), collect_set(deptname), cast(corr(deptno, empno) as string), cast(covar_pop(deptno, empno) as string), " +
      "cast(covar_samp(deptno, empno) as string), cast(grouping(designation) as string), cast(grouping(deptname) as string), cast(mean(deptno) as string), cast(mean" +
      "(empno) as string),cast(skewness(deptno) as string), cast(skewness(empno) as string), cast(stddev(deptno) as string), cast(stddev(empno) as string), cast(stddev_pop" +
      "(deptno) as string), cast(stddev_pop(empno) as string), cast(stddev_samp(deptno) as string), cast(stddev_samp(empno) as string), cast(var_pop(deptno) as string), " +
      "cast(var_pop(empno) as string), cast(var_samp(deptno) as string), cast(var_samp(empno) as string), cast(variance(deptno) as string), cast(variance(empno) as string), " +
      "COALESCE(CONV(substring(empname, 3, 2), 16, 10), ''), COALESCE(CONV(substring(deptname, 3," +
      " 2), 16, 10), '') from udfHive where empname = 'pramod' or deptname = 'network' or " +
      "designation='TL' group by designation, deptname, empname with ROLLUP")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test cast and alias with all the above udf") {
    // cast and alias with all the above udf
    carbonQuery = sql(
      "select cast(approx_count_distinct(empname) as string) as c1, cast(approx_count_distinct(deptname) as string) as c2, collect_list" +
      "(empname) as c3, collect_set(deptname) as c4, cast(corr(deptno, empno) as string) as c5, cast(covar_pop(deptno, empno) as string) as c6, " +
      "cast(covar_samp(deptno, empno) as string) as c7, cast(grouping(designation) as string) as c8, cast(grouping(deptname) as string) as c9, cast(mean(deptno) as string) as c10, cast(mean" +
      "(empno) as string) as c11,cast(skewness(deptno) as string) as c12, cast(skewness(empno) as string) as c13, cast(stddev(deptno) as string) as c14, cast(stddev(empno) as string) as c15, cast(stddev_pop" +
      "(deptno) as string) as c16, cast(stddev_pop(empno) as string) as c17, cast(stddev_samp(deptno) as string) as c18, cast(stddev_samp(empno) as string) as c19, cast(var_pop(deptno) as string) as c20, " +
      "cast(var_pop(empno) as string) as c21, cast(var_samp(deptno) as string) as c22, cast(var_samp(empno) as string) as c23, cast(variance(deptno) as string) as c24, cast(variance(empno) as string) as c25, " +
      "COALESCE(CONV(substring(empname, 3, 2), 16, 10), '') as c26, COALESCE(CONV(substring(deptname, 3," +
      " 2), 16, 10), '') as c27 from udfValidation where empname = 'pramod' or deptname = 'network' or " +
      "designation='TL' group by designation, deptname, empname with ROLLUP")
    hiveQuery = sql(
      "select cast(approx_count_distinct(empname) as string) as c1, cast(approx_count_distinct(deptname) as string) as c2, collect_list" +
      "(empname) as c3, collect_set(deptname) as c4, cast(corr(deptno, empno) as string) as c5, cast(covar_pop(deptno, empno) as string) as c6, " +
      "cast(covar_samp(deptno, empno) as string) as c7, cast(grouping(designation) as string) as c8, cast(grouping(deptname) as string) as c9, cast(mean(deptno) as string) as c10, cast(mean" +
      "(empno) as string) as c11,cast(skewness(deptno) as string) as c12, cast(skewness(empno) as string) as c13, cast(stddev(deptno) as string) as c14, cast(stddev(empno) as string) as c15, cast(stddev_pop" +
      "(deptno) as string) as c16, cast(stddev_pop(empno) as string) as c17, cast(stddev_samp(deptno) as string) as c18, cast(stddev_samp(empno) as string) as c19, cast(var_pop(deptno) as string) as c20, " +
      "cast(var_pop(empno) as string) as c21, cast(var_samp(deptno) as string) as c22, cast(var_samp(empno) as string) as c23, cast(variance(deptno) as string) as c24, cast(variance(empno) as string) as c25, " +
      "COALESCE(CONV(substring(empname, 3, 2), 16, 10), '') as c26, COALESCE(CONV(substring(deptname, 3," +
      " 2), 16, 10), '') as c27 from udfHive where empname = 'pramod' or deptname = 'network' or " +
      "designation='TL' group by designation, deptname, empname with ROLLUP")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test udf on filter - concat") {
    carbonQuery = sql("select concat_ws(deptname)from udfValidation where concat_ws(deptname) IS NOT NULL or concat_ws(deptname) is null")
    hiveQuery = sql("select concat_ws(deptname)from udfHive where concat_ws(deptname) IS NOT NULL or concat_ws(deptname) is null")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test udf on filter - find_in_set") {
    carbonQuery = sql("select find_in_set(deptname,'o')from udfValidation where find_in_set(deptname,'o') =0 or find_in_set(deptname,'a') is null")
    hiveQuery = sql("select find_in_set(deptname,'o')from udfHive where find_in_set(deptname,'o') =0 or find_in_set(deptname,'a') is null")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  test("test udf on filter - agg") {
    carbonQuery = sql("select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfValidation where length(empname)=6 or length(empname) is NULL")
    hiveQuery = sql("select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfHive where length(empname)=6 or length(empname) is NULL")
    if (testSecondaryIndexForORFilterPushDown.isFilterPushedDownToSI(carbonQuery.queryExecution.executedPlan)) {
      assert(true)
    } else {
      assert(false)
    }
    checkAnswer(carbonQuery, hiveQuery)
  }

  private def dropTables: Unit = {
    sql("drop index if exists ind_i1 on udfValidation")
    sql("drop index if exists ind_i2 on udfValidation")
    sql("drop index if exists ind_i3 on udfValidation")
    sql("drop index if exists ind_i4 on udfValidation")
    sql("drop index if exists ind_i5 on udfValidation")
    sql("drop table if exists udfValidation")
    sql("drop table if exists udfHive")
  }

}
