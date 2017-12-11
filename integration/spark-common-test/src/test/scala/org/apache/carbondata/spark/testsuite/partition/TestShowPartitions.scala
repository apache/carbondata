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

package org.apache.carbondata.spark.testsuite.partition

import java.sql.Timestamp

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.core.api.CarbonProperties

class TestShowPartition  extends QueryTest with BeforeAndAfterAll {
  override def beforeAll = {

    CarbonProperties.getInstance()
      .addProperty("carbon.timestamp.format", "dd-MM-yyyy")

    sql("drop table if exists notPartitionTable")
    sql("""
                | CREATE TABLE notPartitionTable
                | (
                | vin String,
                | logdate Timestamp,
                | phonenumber Int,
                | country String,
                | area String
                | )
                | STORED BY 'carbondata'
              """.stripMargin)

    sql("drop table if exists hashTable")
    sql(
      """
        | CREATE TABLE hashTable (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('PARTITION_TYPE'='HASH','NUM_PARTITIONS'='3')
      """.stripMargin)

    sql("drop table if exists rangeTable")
    sql(
      """
        | CREATE TABLE rangeTable (empno int, empname String, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (doj Timestamp)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('PARTITION_TYPE'='RANGE',
        |  'RANGE_INFO'='01-01-2010, 01-01-2015')
      """.stripMargin)

    sql("drop table if exists listTable")
    sql(
      """
        | CREATE TABLE listTable (empno int, empname String, designation String, doj Timestamp,
        |  workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (workgroupcategory int)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('PARTITION_TYPE'='LIST',
        |  'LIST_INFO'='0, 1, (2, 3)')
      """.stripMargin)

    sql(s"CREATE DATABASE if not exists partitionDB")
    sql("drop table if exists partitionDB.hashTable")
    sql("drop table if exists partitionDB.rangeTable")
    sql("drop table if exists partitionDB.listTable")
    sql(
      """
        | CREATE TABLE partitionDB.hashTable (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('PARTITION_TYPE'='HASH','NUM_PARTITIONS'='3')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE partitionDB.rangeTable (empno int, empname String, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (doj Timestamp)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('PARTITION_TYPE'='RANGE',
        |  'RANGE_INFO'='01-01-2010, 01-01-2015')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE partitionDB.listTable (empno int, empname String, designation String,
        |   doj Timestamp,workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (workgroupcategory int)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('PARTITION_TYPE'='LIST',
        |  'LIST_INFO'='0, 1, (2, 3)')
      """.stripMargin)

    sql("DROP TABLE IF EXISTS hiveTable")
    sql("""
       | create table hiveTable(id int, name string) partitioned by (city string)
       | row format delimited fields terminated by ','
       """.stripMargin)
    sql("alter table hiveTable add partition (city = 'Hangzhou')")

    sql(s"CREATE DATABASE if not exists hiveDB")
    sql("DROP TABLE IF EXISTS hiveDB.hiveTable")
    sql("""
       | create table hiveDB.hiveTable(id int, name string) partitioned by (city string)
       | row format delimited fields terminated by ','
       """.stripMargin)
    sql("alter table hiveDB.hiveTable add partition (city = 'Shanghai')")
  }

  test("show partition table: exception when show not partition table") {
    val errorMessage =
      intercept[AnalysisException] { sql("show partitions notPartitionTable").show() }
    assert(errorMessage.getMessage.contains(
      "SHOW PARTITIONS is not allowed on a table that is not partitioned: notpartitiontable"))
  }

  test("show partition table: hash table") {
    // EqualTo
    checkAnswer(sql("show partitions hashTable"), Seq(Row("empno = HASH_NUMBER(3)")))

  }

  test("show partition table: range partition") {
    // EqualTo
    checkAnswer(sql("show partitions rangeTable"), Seq(Row("0, doj = DEFAULT"),
      Row("1, doj < 01-01-2010"), Row("2, 01-01-2010 <= doj < 01-01-2015")))
  }

  test("show partition table: list partition") {
    // EqualTo
    checkAnswer(sql("show partitions listTable"), Seq(Row("0, workgroupcategory = DEFAULT"),
      Row("1, workgroupcategory = 0"), Row("2, workgroupcategory = 1"), Row("3, workgroupcategory = 2, 3")))

  }
  test("show partition table: not default db") {
    // EqualTo
    checkAnswer(sql("show partitions partitionDB.hashTable"), Seq(Row("empno = HASH_NUMBER(3)")))
    // EqualTo
    checkAnswer(sql("show partitions partitionDB.rangeTable"), Seq(Row("0, doj = DEFAULT"),
      Row("1, doj < 01-01-2010"), Row("2, 01-01-2010 <= doj < 01-01-2015")))
    // EqualTo
    checkAnswer(sql("show partitions partitionDB.listTable"), Seq(Row("0, workgroupcategory = DEFAULT"),
      Row("1, workgroupcategory = 0"), Row("2, workgroupcategory = 1"), Row("3, workgroupcategory = 2, 3")))

  }

  test("show partition table: hive partition table") {
    // EqualTo
    checkAnswer(sql("show partitions hiveTable"), Seq(Row("city=Hangzhou")))
    sql("use hiveDB").show()
    checkAnswer(sql("show partitions hiveTable"), Seq(Row("city=Shanghai")))
    sql("use default").show()
  }

  override def afterAll = {
    sql("drop table if exists notPartitionTable")
    sql("drop table if exists  hashTable")
    sql("drop table if exists  listTable")
    sql("drop table if exists  rangeTable")
    sql("drop table if exists  hiveTable")
    try {
      sql("drop table if exists  partitionDB.hashTable")

    } catch {
      case ex: NoSuchDatabaseException => print(ex.getMessage())
    }
    try {
      sql("drop table if exists  partitionDB.rangeTable")
    } catch {
      case ex: NoSuchDatabaseException => print(ex.getMessage())
    }
    try {
      sql("drop table if exists  partitionDB.listTable")
    } catch {
      case ex: NoSuchDatabaseException => print(ex.getMessage())
    }
    try {
      sql("drop table if exists  hiveDB.hiveTable")
    } catch {
      case ex: NoSuchDatabaseException => print(ex.getMessage())
    }
    sql("DROP DATABASE if exists partitionDB")
    sql("DROP DATABASE if exists hiveDB")
  }
}
