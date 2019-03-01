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

class TestCarbonShowCacheCommand extends QueryTest with BeforeAndAfterAll {
  override protected def beforeAll(): Unit = {
    // use new database
    sql("drop database if exists cache_db cascade").collect()
    sql("drop database if exists cache_empty_db cascade").collect()
    sql("create database cache_db").collect()
    sql("create database cache_empty_db").collect()
    dropTable
    sql("use cache_db").collect()
    sql(
      """
        | CREATE TABLE cache_db.cache_1
        | (empno int, empname String, designation String, doj Timestamp, workgroupcategory int,
        |  workgroupcategoryname String, deptno int, deptname String, projectcode int,
        |  projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,
        |  salary int)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('DICTIONARY_INCLUDE'='deptname')
      """.stripMargin)
    // bloom
    sql("CREATE DATAMAP IF NOT EXISTS cache_1_bloom ON TABLE cache_db.cache_1 USING 'bloomfilter' " +
        "DMPROPERTIES('INDEX_COLUMNS'='deptno')")
    sql(s"LOAD DATA INPATH '$resourcesPath/data.csv' INTO TABLE cache_1 ")

    sql(
      """
        | CREATE TABLE cache_2
        | (empno int, empname String, designation String, doj Timestamp, workgroupcategory int,
        |  workgroupcategoryname String, deptno int, deptname String, projectcode int,
        |  projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,
        |  salary int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"LOAD DATA INPATH '$resourcesPath/data.csv' INTO TABLE cache_db.cache_2 ")
    sql("insert into table cache_2 select * from cache_1").collect()

    sql(
      """
        | CREATE TABLE cache_3
        | (empno int, empname String, designation String, doj Timestamp, workgroupcategory int,
        |  workgroupcategoryname String, deptno int, deptname String, projectcode int,
        |  projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,
        |  salary int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"LOAD DATA INPATH '$resourcesPath/data.csv' INTO TABLE cache_3 ")

    // use default database
    sql("use default").collect()
    sql(
      """
        | CREATE TABLE cache_4
        | (empno int, empname String, designation String, doj Timestamp, workgroupcategory int,
        |  workgroupcategoryname String, deptno int, deptname String, projectcode int,
        |  projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,
        |  salary int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql("insert into table cache_4 select * from cache_db.cache_2").collect()

    // standard partition table
    sql(
      """
        | CREATE TABLE cache_5
        | (empno int, empname String, designation String, doj Timestamp, workgroupcategory int,
        |  workgroupcategoryname String, deptname String, projectcode int,
        |  projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,
        |  salary int)
        | PARTITIONED BY (deptno int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      "insert into table cache_5 select empno,empname,designation,doj,workgroupcategory," +
      "workgroupcategoryname,deptname,projectcode,projectjoindate,projectenddate,attendance," +
      "utilization,salary,deptno from cache_4").collect()

    // datamap
    sql("create datamap cache_4_count on table cache_4 using 'preaggregate' as " +
        "select workgroupcategoryname,count(empname) as count from cache_4 group by workgroupcategoryname")

    // count star to cache index
    sql("select max(deptname) from cache_db.cache_1").collect()
    sql("SELECT deptno FROM cache_db.cache_1 where deptno=10").collect()
    sql("select count(*) from cache_db.cache_2").collect()
    sql("select count(*) from cache_4").collect()
    sql("select count(*) from cache_5").collect()
    sql("select workgroupcategoryname,count(empname) as count from cache_4 group by workgroupcategoryname").collect()
  }


  override protected def afterAll(): Unit = {
    sql("use default").collect()
    dropTable
  }

  private def dropTable = {
    sql("DROP TABLE IF EXISTS cache_db.cache_1")
    sql("DROP TABLE IF EXISTS cache_db.cache_2")
    sql("DROP TABLE IF EXISTS cache_db.cache_3")
    sql("DROP TABLE IF EXISTS default.cache_4")
    sql("DROP TABLE IF EXISTS default.cache_5")
  }

  test("show cache") {
    sql("use cache_empty_db").collect()
    val result1 = sql("show metacache").collect()
    assertResult(2)(result1.length)
    assertResult(Row("cache_empty_db", "ALL", "0", "0", "0"))(result1(1))

    sql("use cache_db").collect()
    val result2 = sql("show metacache").collect()
    assertResult(4)(result2.length)

    sql("use default").collect()
    val result3 = sql("show metacache").collect()
    val dataMapCacheInfo = result3
      .map(row => row.getString(1))
      .filter(table => table.equals("cache_4_cache_4_count"))
    assertResult(1)(dataMapCacheInfo.length)
  }

  test("show metacache for table") {
    sql("use cache_db").collect()
    val result1 = sql("show metacache for table cache_1").collect()
    assertResult(3)(result1.length)

    val result2 = sql("show metacache for table cache_db.cache_2").collect()
    assertResult(2)(result2.length)

    checkAnswer(sql("show metacache for table cache_db.cache_3"),
      Seq(Row("Index", "0", "0/1 index files cached"), Row("Dictionary", "0", "")))

    val result4 = sql("show metacache for table default.cache_4").collect()
    assertResult(3)(result4.length)

    sql("use default").collect()
    val result5 = sql("show metacache for table cache_5").collect()
    assertResult(2)(result5.length)
  }
}
