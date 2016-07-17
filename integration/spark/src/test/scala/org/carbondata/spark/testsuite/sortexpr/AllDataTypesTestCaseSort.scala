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

package org.carbondata.spark.testsuite.sortexpr

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for sort expression query on multiple datatypes
 * @author N00902756
 *
 */

class AllDataTypesTestCaseSort extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("CREATE TABLE alldatatypescubesort (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    sql("LOAD DATA local inpath './src/test/resources/data.csv' INTO TABLE alldatatypescubesort OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')");

    sql("CREATE TABLE alldatatypescubesort_hive (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int)row format delimited fields terminated by ','")
    sql("LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' INTO TABLE alldatatypescubesort_hive");

  }

  test("select empno,empname,utilization,count(salary),sum(empno) from alldatatypescubesort where empname in ('arvind','ayushi') group by empno,empname,utilization order by empno") {
    checkAnswer(
      sql("select empno,empname,utilization,count(salary),sum(empno) from alldatatypescubesort where empname in ('arvind','ayushi') group by empno,empname,utilization order by empno"),
      sql("select empno,empname,utilization,count(salary),sum(empno) from alldatatypescubesort_hive where empname in ('arvind','ayushi') group by empno,empname,utilization order by empno"))
  }

  override def afterAll {
    sql("drop table alldatatypescubesort")
    sql("drop table alldatatypescubesort_hive")
  }
}