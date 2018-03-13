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

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for filter expression query on multiple datatypes
 */

class AllDataTypesTestCaseFilter extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("CREATE TABLE alldatatypestableFilter (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE alldatatypestableFilter OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""");
    
    sql("CREATE TABLE alldatatypestableFilter_hive (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int)row format delimited fields terminated by ','")
    sql(s"""LOAD DATA local inpath '$resourcesPath/datawithoutheader.csv' INTO TABLE alldatatypestableFilter_hive""");

  }

  test("select empno,empname,utilization,count(salary),sum(empno) from alldatatypestableFilter where empname in ('arvind','ayushi') group by empno,empname,utilization") {
    checkAnswer(
      sql("select empno,empname,utilization,count(salary),sum(empno) from alldatatypestableFilter where empname in ('arvind','ayushi') group by empno,empname,utilization"),
      sql("select empno,empname,utilization,count(salary),sum(empno) from alldatatypestableFilter_hive where empname in ('arvind','ayushi') group by empno,empname,utilization"))
  }
  
  test("select empno,empname from alldatatypestableFilter where regexp_replace(workgroupcategoryname, 'er', 'ment') NOT IN ('development')") {
    checkAnswer(
      sql("select empno,empname from alldatatypestableFilter where regexp_replace(workgroupcategoryname, 'er', 'ment') NOT IN ('development')"),
      sql("select empno,empname from alldatatypestableFilter_hive where regexp_replace(workgroupcategoryname, 'er', 'ment') NOT IN ('development')"))
  }
  
  test("select empno,empname from alldatatypescubeFilter where regexp_replace(workgroupcategoryname, 'er', 'ment') != 'development'") {
    checkAnswer(
      sql("select empno,empname from alldatatypestableFilter where regexp_replace(workgroupcategoryname, 'er', 'ment') != 'development'"),
      sql("select empno,empname from alldatatypestableFilter_hive where regexp_replace(workgroupcategoryname, 'er', 'ment') != 'development'"))
  }

  test("verify like query ends with filter push down") {
    val df = sql("select * from alldatatypestableFilter where empname like '%nandh'").queryExecution
      .sparkPlan
    assert(df.metadata.get("PushedFilters").get.contains("CarbonEndsWith"))
  }

  test("verify like query contains with filter push down") {
    val df = sql("select * from alldatatypestableFilter where empname like '%nand%'").queryExecution
      .sparkPlan
    assert(df.metadata.get("PushedFilters").get.contains("CarbonContainsWith"))
  }

  test("verify like query contains with filter push down with OR expression") {
    sql("drop table if exists like_contains_with")
    sql(
      s"""create table like_contains_with (id bigint, country string, hscode string, hsdes string,
      |importer string, importer_address string, exporter string, exporter_address string, port
      |string, product string, velus double, qty double, unit string, weight double, tel string,
      |pro1 string, pro2 string, pro3 string, pro4 string, pro5 string, pro6 string, pro7 string)
      | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    val df = sql(
      s"""select pro7,pro6,pro4,pro5,pro3,pro2,pro1 from like_contains_with where pro7 like '%15b%'
      |or pro4 like '%9d%' or pro6 like '%a77h%' or pro5 like '%hina%' or pro3 like '%44d%' or
      |pro2 like '%ro2%' or pro1 like '%cv%'
      """.stripMargin)
      .queryExecution
      .sparkPlan
    val pushedFilters: Option[String] = df.metadata.get("PushedFilters")
    assert(!pushedFilters.isDefined)
    sql("drop table if exists like_contains_with")
  }

  test("verify like query ends with filter push down with OR expression") {
    sql("drop table if exists like_ends_with")
    sql(
      s"""create table like_ends_with (id bigint, country string, hscode string, hsdes string,
        |importer string, importer_address string, exporter string, exporter_address string, port
        |string, product string, velus double, qty double, unit string, weight double, tel string,
        |pro1 string, pro2 string, pro3 string, pro4 string, pro5 string, pro6 string, pro7 string)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    val df = sql(
      s"""select pro7,pro6,pro4,pro5,pro3,pro2,pro1 from like_ends_with where pro7 like '%15b'
          |or pro4 like '%9d' or pro6 like '%a77h' or pro5 like '%hina' or pro3 like '%44d' or
          |pro2 like '%ro2' or pro1 like '%cv'
      """.stripMargin)
      .queryExecution
      .sparkPlan
    val pushedFilters: Option[String] = df.metadata.get("PushedFilters")
    assert(!pushedFilters.isDefined)
    sql("drop table if exists like_ends_with")
  }
  
  override def afterAll {
    sql("drop table alldatatypestableFilter")
    sql("drop table alldatatypestableFilter_hive")
  }
}