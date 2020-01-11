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

package org.apache.carbondata.spark.testsuite.filterexpr

import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

/**
  * Test Class for filter expression query on String datatypes
  *
  */
class GrtLtFilterProcessorTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists a12")
    sql("drop table if exists a12_all_null")
    sql("drop table if exists a12_no_null")
    sql("drop table if exists Test_Boundary1")

    sql(
      "create table a12(empid String,ename String,sal double,deptno int,mgr string,gender string," +
        "dob timestamp,comm decimal(4,2),desc string) STORED AS carbondata"
    )
    sql(
      "create table a12_all_null(empid String,ename String,sal double,deptno int,mgr string,gender" +
        " string," +
        "dob timestamp,comm decimal(4,2),desc string) STORED AS carbondata"
    )
    sql(
      "create table a12_no_null(empid String,ename String,sal double,deptno int,mgr string,gender" +
        " string," +
        "dob timestamp,comm decimal(4,2),desc string) STORED AS carbondata"
    )
    sql("create table Test_Boundary1 (c1_int int,c2_Bigint Bigint,c3_Decimal Decimal(38,38),c4_double double,c5_string string,c6_Timestamp Timestamp,c7_Datatype_Desc string) STORED AS carbondata")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
    var testData = s"$resourcesPath/filter/emp2.csv"
    sql(
      s"""LOAD DATA LOCAL INPATH '$testData' into table a12 OPTIONS('DELIMITER'=',',
         'QUOTECHAR'='"','FILEHEADER'='empid,ename,sal,deptno,mgr,gender,dob,comm,desc')"""
        .stripMargin
    )
    testData = s"$resourcesPath/filter/emp2allnull.csv"

    sql(
      s"""LOAD DATA LOCAL INPATH '$testData' into table a12_all_null OPTIONS('DELIMITER'=',',
         'QUOTECHAR'='"','FILEHEADER'='empid,ename,sal,deptno,mgr,gender,dob,comm,desc')"""
        .stripMargin
    )
    testData = s"$resourcesPath/filter/emp2nonull.csv"

    sql(
      s"""LOAD DATA LOCAL INPATH '$testData' into table a12_no_null OPTIONS('DELIMITER'=',',
         'QUOTECHAR'='"')"""
        .stripMargin
    )
    
    sql(
      s"LOAD DATA INPATH '$resourcesPath/Test_Data1_Logrithmic.csv' INTO table Test_Boundary1 OPTIONS('DELIMITER'=',','QUOTECHAR'='','FILEHEADER'='')")
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
  test("0.0 and -0.0 equality check for double data type applying log function") {
    checkAnswer(
      sql("select log(c4_double,1) from Test_Boundary1 where log(c4_double,1)= -0.0"),
      Seq(Row(0.0),Row(0.0))
    )
  }

  test("Greater Than equal to Filter") {
    checkAnswer(
      sql("select count (empid) from a12 where dob >= '2014-07-01 12:07:28'"),
      Seq(Row(5))
    )
  }
  //all null test cases

  test("Less Than Filter all null") {
    checkAnswer(
      sql("select count(empid) from a12_all_null where dob < '2014-07-01 12:07:28'"),
      Seq(Row(0))
    )
  }

  test("Les Than equal Filter all null") {
    checkAnswer(
      sql("select count (empid) from a12_all_null where dob <= '2014-07-01 12:07:28'"),
      Seq(Row(0))
    )
  }

  test("Greater Than Filter all null") {
    checkAnswer(
      sql("select count (empid) from a12_all_null where dob > '2014-07-01 12:07:28'"),
      Seq(Row(0))
    )
  }

  test("Greater Than equal to Filter all null") {
    checkAnswer(
      sql("select count (empid) from a12_all_null where dob >= '2014-07-01 12:07:28'"),
      Seq(Row(0))
    )
  }
//  test("In condition With improper format query regarding Null filter") {
//    checkAnswer(
//      sql("select empid from a12_all_null " + "where empid not in ('china',NULL)"),
//      Seq()
//    )
//  }

  //no null test cases

  test("Less Than Filter no null") {
    checkAnswer(
      sql("select count(empid) from a12_no_null where dob < '2014-07-01 12:07:28'"),
      Seq(Row(0))
    )
  }

  test("Less Than equal Filter no null") {
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
    sql("drop table if exists a12_all_null")
    sql("drop table if exists a12_no_null")
    sql("drop table if exists Test_Boundary1")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }
}
