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

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestShowPartition  extends QueryTest with BeforeAndAfterAll {
  override def beforeAll = {
    dropTable

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")

  }

  test("show partition table: hash table") {
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
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv'
       INTO TABLE hashTable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    // EqualTo
    checkAnswer(sql("show partitions hashTable"), Seq(Row("HASH PARTITION", "", "3")))

    sql("drop table hashTable")
  }

  test("show partition table: range partition") {
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
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv'
       INTO TABLE rangeTable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    // EqualTo
    checkAnswer(sql("show partitions rangeTable"), Seq(Row("0", "", "default"),
        Row("1", "", "< 01-01-2010"), Row("2", "", "< 01-01-2015")))
    sql("drop table rangeTable")
  }

  test("show partition table: list partition") {
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
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE
       listTable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    // EqualTo
    checkAnswer(sql("show partitions listTable"), Seq(Row("0", "", "0"),
        Row("1", "", "1"), Row("2", "", "2, 3")))

  sql("drop table listTable")
  }
  test("show partition table: not default db") {
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
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv'
       INTO TABLE partitionDB.hashTable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
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
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv'
       INTO TABLE partitionDB.rangeTable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
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
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE
       partitionDB.listTable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    // EqualTo
    checkAnswer(sql("show partitions partitionDB.hashTable"), Seq(Row("HASH PARTITION", "", "3")))
    // EqualTo
    checkAnswer(sql("show partitions partitionDB.rangeTable"), Seq(Row("0", "", "default"),
        Row("1", "", "< 01-01-2010"), Row("2", "", "< 01-01-2015")))
    // EqualTo
    checkAnswer(sql("show partitions partitionDB.listTable"), Seq(Row("0", "", "0"),
        Row("1", "", "1"), Row("2", "", "2, 3")))

    sql("drop table partitionDB.hashTable")
    sql("drop table partitionDB.rangeTable")
    sql("drop table partitionDB.listTable")
    sql(s"DROP DATABASE partitionDB")
  }
  override def afterAll = {
    dropTable
  }

  def dropTable = {
    sql("drop table if exists hashTable")
    sql("drop table if exists rangeTable")
    sql("drop table if exists listTable")
  }
}
