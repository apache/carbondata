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
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for join query on multiple datatypes
 */

class AllDataTypesTestCaseJoin extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("CREATE TABLE alldatatypestableJoin (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata TBLPROPERTIES('TABLE_BLOCKSIZE'='4')")
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
    sql("create table employee(name string, empid string, mgrid string, mobileno bigint) STORED AS carbondata")
    sql(s"load data inpath '$resourcesPath/join/emp.csv' into table employee options('fileheader'='name,empid,mgrid,mobileno')")
    
    sql("drop table if exists manager")
    sql("create table manager(name string, empid string, mgrid string, mobileno bigint) STORED AS carbondata")
    sql(s"load data inpath '$resourcesPath/join/mgr.csv' into table manager options('fileheader'='name,empid,mgrid,mobileno')")
    checkAnswer(
    sql("select e.empid from employee e inner join manager m on e.mgrid=m.empid"),
    Seq(Row("t23717"))
    )
   
  }

  test("Union with alias fails") {
    sql("DROP TABLE IF EXISTS carbon_table1")
    sql("DROP TABLE IF EXISTS carbon_table2")

    sql("CREATE TABLE carbon_table1(shortField smallint,intField int,bigintField bigint,doubleField double,stringField string,timestampField timestamp,decimalField decimal(18,2),dateField date,charField char(5),floatField float) STORED AS carbondata ")

    sql("CREATE TABLE carbon_table2(shortField smallint,intField int,bigintField bigint,doubleField double,stringField string,timestampField timestamp,decimalField decimal(18,2),dateField date,charField char(5),floatField float) STORED AS carbondata ")

    val path1 = s"$resourcesPath/join/data1.csv"
    val path2 = s"$resourcesPath/join/data2.csv"

    sql(
      s"""
         LOAD DATA LOCAL INPATH '$path1'
         INTO TABLE carbon_table1
         options('FILEHEADER'='shortField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData','COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)
    sql(
      s"""
         LOAD DATA LOCAL INPATH '$path2'
         INTO TABLE carbon_table2
         options('FILEHEADER'='shortField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData','COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)

    checkAnswer(sql("""SELECT t.a a FROM (select charField a from  carbon_table1 t1 union all  select charField a from  carbon_table2 t2) t order by a """),
      Seq(Row("aaa"),Row("bbb"),Row("ccc"),Row("ddd"))
     )

    // Drop table
    sql("DROP TABLE IF EXISTS carbon_table1")
    sql("DROP TABLE IF EXISTS carbon_table2")
  }

  test("join with aggregate plan") {
    checkAnswer(sql("SELECT c1.empno,c1.empname, c2.empno FROM (SELECT empno,empname FROM alldatatypestableJoin GROUP BY empno,empname) c1 FULL JOIN " +
                    "(SELECT empno FROM alldatatypestableJoin GROUP BY empno) c2 ON c1.empno = c2.empno"),
      sql("SELECT c1.empno,c1.empname, c2.empno FROM (SELECT empno,empname FROM alldatatypestableJoin_hive GROUP BY empno,empname) c1 FULL JOIN " +
          "(SELECT empno FROM alldatatypestableJoin_hive GROUP BY empno) c2 ON c1.empno = c2.empno"))
  }

  override def afterAll {
    sql("drop table alldatatypestableJoin")
    sql("drop table alldatatypestableJoin_hive")
    sql("drop table if exists manager")
    sql("drop table if exists employee")
  }
}