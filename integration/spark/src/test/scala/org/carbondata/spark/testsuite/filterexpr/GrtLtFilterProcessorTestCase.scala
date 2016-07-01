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

package org.carbondata.spark.testsuite.filterexpr

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.scalatest.BeforeAndAfterAll

/**
  * Test Class for filter expression query on String datatypes
  *
  */
class GrtLtFilterProcessorTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists a12")
    sql("drop table if exists a12_allnull")
    sql("drop table if exists a12_no_null")

    sql(
      "create table a12(empid String,ename String,sal double,deptno int,mgr string,gender string," +
        "dob timestamp,comm decimal(4,2),desc string) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "create table a12_allnull(empid String,ename String,sal double,deptno int,mgr string,gender" +
        " string," +
        "dob timestamp,comm decimal(4,2),desc string) stored by 'org.apache.carbondata.format'"
    )
    sql(
      "create table a12_no_null(empid String,ename String,sal double,deptno int,mgr string,gender" +
        " string," +
        "dob timestamp,comm decimal(4,2),desc string) stored by 'org.apache.carbondata.format'"
    )
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
    val basePath = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    var testData = basePath + "/src/test/resources/filter/emp2.csv"
    sql(
      s"""LOAD DATA LOCAL INPATH '$testData' into table a12 OPTIONS('DELIMITER'=',',
         'QUOTECHAR'='"','FILEHEADER'='empid,ename,sal,deptno,mgr,gender,dob,comm,desc')"""
        .stripMargin
    )
    testData = basePath + "/src/test/resources/filter/emp2allnull.csv"

    sql(
      s"""LOAD DATA LOCAL INPATH '$testData' into table a12_allnull OPTIONS('DELIMITER'=',',
         'QUOTECHAR'='"','FILEHEADER'='empid,ename,sal,deptno,mgr,gender,dob,comm,desc')"""
        .stripMargin
    )
    testData = basePath + "/src/test/resources/filter/emp2nonull.csv"

    sql(
      s"""LOAD DATA LOCAL INPATH '$testData' into table a12_no_null OPTIONS('DELIMITER'=',',
         'QUOTECHAR'='"')"""
        .stripMargin
    )
  }
  //mixed value test
  test("Less Than Filter") {
    checkAnswer(
      sql("select count(empid) from a12 where dob < '2014-07-01 12:07:28'"),
      Seq(Row(0))
    )
  }

  test("Les Than equal Filter") {
    checkAnswer(
      sql("select count (empid) from a12 where dob <= '2014-07-01 12:07:28'"),
      Seq(Row(2))
    )
  }

  test("Greater Than Filter") {
    checkAnswer(
      sql("select count (empid) from a12 where dob > '2014-07-01 12:07:28'"),
      Seq(Row(3))
    )
  }

  test("Greater Than equal to Filter") {
    sql("select count (empid) from a12 where dob >= '2014-07-01 12:07:28'").show()
    checkAnswer(
      sql("select count (empid) from a12 where dob >= '2014-07-01 12:07:28'"),
      Seq(Row(5))
    )
  }
  //all null test cases

  test("Less Than Filter all null") {
    checkAnswer(
      sql("select count(empid) from a12_allnull where dob < '2014-07-01 12:07:28'"),
      Seq(Row(0))
    )
  }

  test("Les Than equal Filter all null") {
    checkAnswer(
      sql("select count (empid) from a12_allnull where dob <= '2014-07-01 12:07:28'"),
      Seq(Row(0))
    )
  }

  test("Greater Than Filter all null") {
    checkAnswer(
      sql("select count (empid) from a12_allnull where dob > '2014-07-01 12:07:28'"),
      Seq(Row(0))
    )
  }

  test("Greater Than equal to Filter all null") {
    checkAnswer(
      sql("select count (empid) from a12_allnull where dob >= '2014-07-01 12:07:28'"),
      Seq(Row(0))
    )
  }

  //no null test cases

  test("Less Than Filter no null") {
    checkAnswer(
      sql("select count(empid) from a12_no_null where dob < '2014-07-01 12:07:28'"),
      Seq(Row(0))
    )
  }

  test("Les Than equal Filter no null") {
    sql("select empid from a12_no_null where dob <= '2014-07-01 12:07:28'").show()
    checkAnswer(
      sql("select count(empid) from a12_no_null where dob <= '2014-07-01 12:07:28'"),
      Seq(Row(4))
    )
  }

  test("Greater Than Filter no null") {
    checkAnswer(
      sql("select count (empid) from a12_no_null where dob > '2014-07-01 12:07:28'"),
      Seq(Row(3))
    )
  }

  test("Greater Than equal to Filter no null") {
    checkAnswer(
      sql("select count (empid) from a12_no_null where dob >= '2014-07-01 12:07:28'"),
      Seq(Row(7))
    )
  }

  override def afterAll {
    sql("drop table a12")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }
}
