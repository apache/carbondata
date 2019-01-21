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
package org.apache.carbondata.cluster.sdv.generated

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util._
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory

/**
 * Test Class for SetParameterTestcase to verify all scenarios
 */

class SetParameterTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    cleanAllTable()
  }

  private def cleanAllTable(): Unit = {
    sql("drop table if exists carbon_table")
    sql("drop table if exists emptyColumnValues")
    sql("drop table if exists carbon_table_bad_record_logger")
    sql("drop table if exists carbon_table_single_pass")
    sql("drop table if exists carbon_table_disable_bad_record_logger")
    sql("drop table if exists carbon_table_load")
    sqlContext.sparkSession.catalog.clearCache()
    sql("RESET")
  }

  override def afterAll(): Unit = {
    cleanAllTable()
  }

  test("TC_001-test SET property for Bad Record Logger Enable=FALSE") {
    sql("drop table if exists carbon_table_disable_bad_record_logger")
    sql("SET carbon.options.bad.records.logger.enable=false")
    sql(
      "create table carbon_table_disable_bad_record_logger(empno int, empname String, designation String, " +
      "doj Timestamp," +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String," +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/sortcolumns/data.csv' into table
          |carbon_table_disable_bad_record_logger options('FILEHEADER'='empno,empname,designation,doj,workgroupcategory,workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate,attendance,utilization,salary',
          |'BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORD_PATH'='$resourcesPath')"""
        .stripMargin)
    assert(getLogFileCount("default", "carbon_table_disable_bad_record_logger", "0") == 0)
  }

  test("TC_002-test SET property for Bad Record Logger Enable=TRUE") {
    sql("drop table if exists carbon_table_bad_record_logger")
    sql("SET carbon.options.bad.records.logger.enable=true")
    sql(
      "create table carbon_table_bad_record_logger(empno int, empname String, designation String," +
      " doj Timestamp," +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String," +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/sortcolumns/data.csv' into table
          |carbon_table_bad_record_logger options('FILEHEADER'='empno,empname,designation,doj,workgroupcategory,workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate,attendance,utilization,salary',
          |'BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORD_PATH'='$resourcesPath')"""
        .stripMargin)
    assert(getLogFileCount("default", "carbon_table_bad_record_logger", "0") >= 1)
  }

  test("TC_003-test SET property for Bad Record Action=FORCE") {
    sql("drop table if exists carbon_table")
    sql("SET carbon.options.bad.records.action=force")
    sql(
      "create table carbon_table(empno int, empname String, designation String, doj Timestamp," +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String," +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/sortcolumns/data.csv' into table
          |carbon_table options('FILEHEADER'='empno,empname,designation,doj,workgroupcategory,workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate,attendance,utilization,salary')"""
        .stripMargin)
    checkAnswer(
      s"""select count(*) from carbon_table""",
      Seq(Row(11)), "SetParameterTestCase-TC_003-test SET property for Bad Record Action=FORCE")
  }

  test("TC_004-test SET property for Bad Record Action=REDIRECT") {
    sql("drop table if exists carbon_table")
    sql("SET carbon.options.bad.records.logger.enable=true")
    sql("SET carbon.options.bad.records.action=redirect")
    sql(s"SET carbon.options.bad.record.path=$resourcesPath")
    sql(
      "create table carbon_table(empno int, empname String, designation String, doj date," +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String," +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/sortcolumns/data.csv' into table
          |carbon_table options('FILEHEADER'='empno,empname,designation,doj,workgroupcategory,workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate,attendance,utilization,salary')"""
        .stripMargin)
    checkAnswer(
      s"""select count(*) from carbon_table""",
      Seq(Row(0)), "SetParameterTestCase-TC_004-test SET property for Bad Record Action=REDIRECT")
  }

  test("TC_005-test SET property for Bad Record Action=IGNORE") {
    sql("drop table if exists carbon_table")
    sql("SET carbon.options.bad.records.logger.enable=true")
    sql("SET carbon.options.bad.records.action=ignore")
    sql(
      "create table carbon_table(empno int, empname String, designation String, doj date," +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String," +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/sortcolumns/data.csv' into table
          |carbon_table options('FILEHEADER'='empno,empname,designation,doj,workgroupcategory,workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate,attendance,utilization,salary')"""
        .stripMargin)
    checkAnswer(
      s"""select count(*) from carbon_table""",
      Seq(Row(0)), "SetParameterTestCase-TC_005-test SET property for Bad Record Action=IGNORE")
  }

  test("TC_006-test SET property for Bad Record Action=FAIL") {
    sql("drop table if exists carbon_table")
    sql("SET carbon.options.bad.records.logger.enable=true")
    sql("SET carbon.options.bad.records.action=fail")
    sql(
      "create table carbon_table(empno int, empname String, designation String, doj date," +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String," +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    val exMessage = intercept[Exception] {
      sql(
        s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/sortcolumns/data.csv' into table
            |carbon_table options('FILEHEADER'='empno,empname,designation,doj,workgroupcategory,workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate,attendance,utilization,salary')"""
          .stripMargin)
    }
    assert(exMessage.getMessage.contains("Data load failed due to bad record"))
  }

  test("TC_007-test SET property IS__EMPTY_DATA_BAD_RECORD=FALSE") {
    sql("drop table if exists emptyColumnValues")
    sqlContext.sparkSession.catalog.clearCache()
    sql("RESET")
    sql("SET carbon.options.bad.records.logger.enable=true")
    sql("SET carbon.options.is.empty.data.badrecord=false")
    sql(
      """CREATE TABLE IF NOT EXISTS emptyColumnValues(ID int,CUST_ID int,cust_name string) STORED
          BY 'org.apache.carbondata.format'
      """)
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/badrecord/doubleqoute.csv' into table
         |emptyColumnValues options('SINGLE_PASS'='true')"""
        .stripMargin)
    checkAnswer(
      s"""select count(*) from emptyColumnValues""",
      Seq(Row(1)), "SetParameterTestCase-TC_007-test SET property IS__EMPTY_DATA_BAD_RECORD=FALSE")
  }

  test("TC_008-test SET property IS__EMPTY_DATA_BAD_RECORD=TRUE") {
    sql("drop table if exists emptyColumnValues")
    sql("SET carbon.options.bad.records.logger.enable=true")
    sql("SET carbon.options.is.empty.data.badrecord=true")
    sql("SET carbon.options.bad.records.action=redirect")
    sql(s"SET carbon.options.bad.record.path=$resourcesPath")
    sql(
      """CREATE TABLE IF NOT EXISTS emptyColumnValues(ID int,CUST_ID int,cust_name string) STORED
          BY 'org.apache.carbondata.format'
      """)
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/badrecord/doubleqoute.csv' into table
         |emptyColumnValues options('SINGLE_PASS'='true')"""
        .stripMargin)
    checkAnswer(
      s"""select count(*) from emptyColumnValues""",
      Seq(Row(1)), "SetParameterTestCase-TC_008-test SET property IS__EMPTY_DATA_BAD_RECORD=TRUE")
  }

  test("TC_009-test SET property for Single Pass") {
    sql("drop table if exists carbon_table_single_pass")
    sql("SET carbon.options.single.pass=true")
    sql(
      "create table carbon_table_single_pass(empno int, empname String, designation String, doj " +
      "Timestamp,workgroupcategory int, workgroupcategoryname String, deptno int, deptname " +
      "String," +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
  }

  test("TC_010-test SET property for Sort Scope-Local_Sort") {
    sql("drop table if exists carbon_table")
    sql("SET carbon.options.bad.records.logger.enable=true")
    sql("SET carbon.options.sort.scope=local_sort")
    sql(
      "create table carbon_table(empno int, empname String, designation String, doj Timestamp," +
      "workgroupcategory int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('SORT_COLUMNS'='empno,empname')")
    checkExistence(sql("DESC FORMATTED carbon_table"), true, "local_sort")
    val sortscope=sql("DESC FORMATTED carbon_table").collect().filter(_.getString(1).trim.equals("local_sort"))
    assertResult(1)(sortscope.length)
    assertResult("local_sort")(sortscope(0).getString(1).trim)
  }

  test("TC_011-test SET property to Enable Unsafe Sort") {
    sql("drop table if exists carbon_table")
    sql("SET carbon.options.bad.records.logger.enable=true")
    sql("SET enable.unsafe.sort=true")
    sql(
      "create table carbon_table(empno int, empname String, designation String, doj Timestamp," +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String," +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
  }

  test("TC_012-test same property with SET and LOAD") {
    sql("drop table if exists carbon_table_load")
    sql("SET carbon.options.bad.records.logger.enable=false")
    sql(
      "create table carbon_table_load(empno int, empname String, designation String, doj Timestamp," +
        "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String," +
        "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
        "utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/sortcolumns/data.csv' into table
         |carbon_table_load options('BAD_RECORDS_LOGGER_ENABLE'='TRUE','FILEHEADER'='empno,empname,designation,doj,workgroupcategory,workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate,attendance,utilization,salary',
         |'BAD_RECORDS_ACTION'='REDIRECT','BAD_RECORD_PATH'='$resourcesPath')"""
        .stripMargin)
    assert(getLogFileCount("default", "carbon_table_load", "0") >= 1)
  }

  private def getLogFileCount(dbName: String, tableName: String, segment: String): Int = {
    var path = resourcesPath + "/" + dbName + "/" + tableName + "/" + segment + "/" + segment
    val carbonFiles = FileFactory.getCarbonFile(path).listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = {
        file.getName.endsWith(".log")
      }
    })
    if (carbonFiles != null) {
      carbonFiles.length
    } else {
      0
    }
  }
}
