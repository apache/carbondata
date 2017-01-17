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

package org.apache.carbondata.spark.testsuite.joinquery

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for join query on multiple datatypes
 * @author N00902756
 *
 */

class AllDataTypesTestCaseJoin extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("CREATE TABLE alldatatypestableJoin (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE alldatatypestableJoin OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""");

    sql("CREATE TABLE alldatatypestableJoin_hive (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int)row format delimited fields terminated by ','")
    sql(s"LOAD DATA local inpath '$resourcesPath/datawithoutheader.csv' INTO TABLE alldatatypestableJoin_hive");

  }

  test("select empno,empname,utilization,count(salary),sum(empno) from alldatatypestableJoin where empname in ('arvind','ayushi') group by empno,empname,utilization") {
    checkAnswer(
      sql("select empno,empname,utilization,count(salary),sum(empno) from alldatatypestableJoin where empname in ('arvind','ayushi') group by empno,empname,utilization"),
      sql("select empno,empname,utilization,count(salary),sum(empno) from alldatatypestableJoin_hive where empname in ('arvind','ayushi') group by empno,empname,utilization"))
  }

  test("select e.empid from employee e inner join manager m on e.mgrid=m.empid") {
    sql("drop table if exists employee")
    sql("create table employee(name string, empid string, mgrid string, mobileno bigint) stored by 'carbondata'")
    sql(s"load data inpath '$resourcesPath/join/emp.csv' into table employee options('fileheader'='name,empid,mgrid,mobileno')")
    
    sql("drop table if exists manager")
    sql("create table manager(name string, empid string, mgrid string, mobileno bigint) stored by 'carbondata'")
    sql(s"load data inpath '$resourcesPath/join/mgr.csv' into table manager options('fileheader'='name,empid,mgrid,mobileno')")
    checkAnswer(
    sql("select e.empid from employee e inner join manager m on e.mgrid=m.empid"),
    Seq(Row("t23717"))
    )
   
  }
  override def afterAll {
    sql("drop table alldatatypestableJoin")
    sql("drop table alldatatypestableJoin_hive")
    sql("drop table if exists manager")
    sql("drop table if exists employee")
  }
}