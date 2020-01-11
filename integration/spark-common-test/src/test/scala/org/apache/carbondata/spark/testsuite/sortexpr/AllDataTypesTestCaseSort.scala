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

package org.apache.carbondata.spark.testsuite.sortexpr

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for sort expression query on multiple datatypes
 */

class AllDataTypesTestCaseSort extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists alldatatypestablesort")
    sql("drop table if exists alldatatypestablesort_hive")
    sql("CREATE TABLE alldatatypestablesort (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE alldatatypestablesort OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""");

    sql("CREATE TABLE alldatatypestablesort_hive (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int)row format delimited fields terminated by ','")
    sql(s"""LOAD DATA local inpath '$resourcesPath/datawithoutheader.csv' INTO TABLE alldatatypestablesort_hive""");

  }

  test("select empno,empname,utilization,count(salary),sum(empno) from alldatatypestablesort where empname in ('arvind','ayushi') group by empno,empname,utilization order by empno") {
    checkAnswer(
      sql("select empno,empname,utilization,count(salary),sum(empno) from alldatatypestablesort where empname in ('arvind','ayushi') group by empno,empname,utilization order by empno"),
      sql("select empno,empname,utilization,count(salary),sum(empno) from alldatatypestablesort_hive where empname in ('arvind','ayushi') group by empno,empname,utilization order by empno"))
  }

  test("select * from alldatatypestablesort order by empname limit 10") {
    sql("select * from alldatatypestablesort order by empname limit 10").collect()
  }

  test("select * from alldatatypestablesort order by salary limit 2") {
    sql("select * from alldatatypestablesort order by salary limit 2").collect()
  }

  test("select * from alldatatypestablesort where empname='arvind' order by salary limit 2") {
    sql("select * from alldatatypestablesort where empname='arvind' order by salary limit 2").collect()
  }

  override def afterAll {
    sql("drop table if exists alldatatypestablesort")
    sql("drop table if exists alldatatypestablesort_hive")
  }
}