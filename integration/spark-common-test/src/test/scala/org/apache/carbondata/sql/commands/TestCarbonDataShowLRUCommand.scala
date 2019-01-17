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

package org.apache.carbondata.sql.commands

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestCarbonDataShowLRUCommand extends QueryTest with BeforeAndAfterAll {
  override protected def beforeAll(): Unit = {
    // use new database
    sql("drop database if exists lru_db cascade").collect()
    sql("drop database if exists lru_empty_db cascade").collect()
    sql("create database lru_db").collect()
    sql("create database lru_empty_db").collect()
    dropTable
    sql("use lru_db").collect()
    sql(
      """
        | CREATE TABLE lru_db.lru_1
        | (empno int, empname String, designation String, doj Timestamp, workgroupcategory int,
        |  workgroupcategoryname String, deptno int, deptname String, projectcode int,
        |  projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,
        |  salary int)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('DICTIONARY_INCLUDE'='deptname')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE lru_1 ")

    sql(
      """
        | CREATE TABLE lru_2
        | (empno int, empname String, designation String, doj Timestamp, workgroupcategory int,
        |  workgroupcategoryname String, deptno int, deptname String, projectcode int,
        |  projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,
        |  salary int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE lru_db.lru_2 ")
    sql("insert into table lru_2 select * from lru_1").collect()

    sql(
      """
        | CREATE TABLE lru_3
        | (empno int, empname String, designation String, doj Timestamp, workgroupcategory int,
        |  workgroupcategoryname String, deptno int, deptname String, projectcode int,
        |  projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,
        |  salary int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE lru_3 ")

    // use default database
    sql("use default").collect()
    sql(
      """
        | CREATE TABLE lru_4
        | (empno int, empname String, designation String, doj Timestamp, workgroupcategory int,
        |  workgroupcategoryname String, deptno int, deptname String, projectcode int,
        |  projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,
        |  salary int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql("insert into table lru_4 select * from lru_db.lru_2").collect()

    // standard partition table
    sql(
      """
        | CREATE TABLE lru_5
        | (empno int, empname String, designation String, doj Timestamp, workgroupcategory int,
        |  workgroupcategoryname String, deptname String, projectcode int,
        |  projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,
        |  salary int)
        | PARTITIONED BY (deptno int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      "insert into table lru_5 select empno,empname,designation,doj,workgroupcategory," +
      "workgroupcategoryname,deptname,projectcode,projectjoindate,projectenddate,attendance," +
      "utilization,salary,deptno from lru_4").collect()

    // count star to cache index
    sql("select max(deptname) from lru_db.lru_1").collect()
    sql("select count(*) from lru_db.lru_2").collect()
    sql("select count(*) from lru_4").collect()
    sql("select count(*) from lru_5").collect()
  }


  override protected def afterAll(): Unit = {
    dropTable
  }

  private def dropTable = {
    sql("DROP TABLE IF EXISTS lru_db.lru_1")
    sql("DROP TABLE IF EXISTS lru_db.lru_2")
    sql("DROP TABLE IF EXISTS lru_db.lru_3")
    sql("DROP TABLE IF EXISTS default.lru_4")
    sql("DROP TABLE IF EXISTS default.lru_5")
  }

  test("show lru") {
    sql("use lru_empty_db").collect()
    val result1 = sql("show lru").collect()
    assertResult(2)(result1.length)
    assertResult(Row("lru_empty_db", "ALL", 0L, 0L))(result1(1))

    sql("use lru_db").collect()
    val result2 = sql("show lru").collect()
    assertResult(4)(result2.length)

    sql("use default").collect()
    val result3 = sql("show lru").collect()
    assertResult(4)(result3.length)
  }

  test("show lru on table") {
    val result1 = sql("show lru on table lru_db.lru_1").collect()
    assertResult(1)(result1.length)

    val result2 = sql("show lru on table lru_db.lru_2").collect()
    assertResult(1)(result2.length)

    checkAnswer(sql("show lru on table lru_db.lru_3"),
      Row("lru_db", "lru_3", 0L, 0L))

    val result4 = sql("show lru on table default.lru_4").collect()
    assertResult(1)(result4.length)

    val result5 = sql("show lru on table default.lru_5").collect()
    assertResult(1)(result5.length)
  }
}
