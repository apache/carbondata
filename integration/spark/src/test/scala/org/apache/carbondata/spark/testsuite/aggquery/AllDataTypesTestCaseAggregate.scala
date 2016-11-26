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

package org.apache.carbondata.spark.testsuite.aggquery

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test Class for aggregate query on multiple datatypes
 *
 */
class AllDataTypesTestCaseAggregate extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    sql("DROP TABLE IF EXISTS alldatatypestableAGG")
    sql("DROP TABLE IF EXISTS alldatatypescubeAGG_hive")
    sql(
      "CREATE TABLE alldatatypestableAGG (empno int, empname String, designation String, doj " +
      "Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname " +
      "String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance " +
      "int,utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    sql(
      "LOAD DATA LOCAL INPATH './src/test/resources/data.csv' INTO TABLE alldatatypestableAGG " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')")
    sql("DROP TABLE IF EXISTS alldatatypescubeAGG_hive")
    sql(
      "CREATE TABLE alldatatypescubeAGG_hive (empno int, empname String, designation String, doj " +
      "Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname " +
      "String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance " +
      "int,utilization int,salary int)row format delimited fields terminated by ','")
    sql(
      "LOAD DATA LOCAL INPATH './src/test/resources/datawithoutheader.csv' INTO TABLE alldatatypescubeAGG_hive")
  }

  test(
    "select empno,empname,utilization,count(salary),sum(empno) from alldatatypestableAGG where " +
    "empname in ('arvind','ayushi') group by empno,empname,utilization")
  {
    checkAnswer(
      sql(
        "select empno,empname,utilization,count(salary),sum(empno) from alldatatypestableAGG where" +
        " empname in ('arvind','ayushi') group by empno,empname,utilization"),
      sql(
        "select empno,empname,utilization,count(salary),sum(empno) from alldatatypescubeAGG_hive where" +
        " empname in ('arvind','ayushi') group by empno,empname,utilization"))
  }

  test(
    "select empname,trim(designation),avg(salary),avg(empno) from alldatatypestableAGG where " +
    "empname in ('arvind','ayushi') group by empname,trim(designation)")
  {
    checkAnswer(
      sql(
        "select empname,trim(designation),avg(salary),avg(empno) from alldatatypestableAGG where " +
        "empname in ('arvind','ayushi') group by empname,trim(designation)"),
      sql(
        "select empname,trim(designation),avg(salary),avg(empno) from alldatatypescubeAGG_hive where " +
        "empname in ('arvind','ayushi') group by empname,trim(designation)"))
  }

  test(
    "select empname,length(designation),max(empno),min(empno), avg(empno) from " +
    "alldatatypestableAGG where empname in ('arvind','ayushi') group by empname,length" +
    "(designation) order by empname")
  {
    checkAnswer(
      sql(
        "select empname,length(designation),max(empno),min(empno), avg(empno) from " +
        "alldatatypestableAGG where empname in ('arvind','ayushi') group by empname,length" +
        "(designation) order by empname"),
      sql(
        "select empname,length(designation),max(empno),min(empno), avg(empno) from " +
        "alldatatypescubeAGG_hive where empname in ('arvind','ayushi') group by empname,length" +
        "(designation) order by empname"))
  }

  test("select count(empno), count(distinct(empno)) from alldatatypestableAGG")
  {
    checkAnswer(
      sql("select count(empno), count(distinct(empno)) from alldatatypestableAGG"),
      sql("select count(empno), count(distinct(empno)) from alldatatypescubeAGG_hive"))
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS alldatatypescubeAGG")
    sql("DROP TABLE IF EXISTS alldatatypescubeAGG_hive")
  }
}