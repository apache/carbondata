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

package org.apache.carbondata.integration.spark.testsuite.complexType

import java.sql.Timestamp

import scala.collection.mutable

import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test class of testing projection with complex data type
 *
 */

class TestComplexDataType extends QueryTest with BeforeAndAfterAll {

  val hugeBinary = RandomStringUtils.randomAlphabetic(33000)

  override def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS table1")
    sql("DROP TABLE IF EXISTS test")
    sql("DROP TABLE IF EXISTS datatype_struct_carbondata")
    sql("DROP TABLE IF EXISTS datatype_struct_parquet")
  }

  override def afterAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, LoggerAction.FORCE.name())
      .removeProperty(CarbonCommonConstants.COMPLEX_DELIMITERS_LEVEL_1)
    sql("DROP TABLE IF EXISTS table1")
    sql("DROP TABLE IF EXISTS test")
  }

  test("test Projection PushDown for Struct - Integer type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll string,person Struct<detail:int>) " +
      "STORED AS carbondata")
    sql("insert into table1 values('abc',named_struct('detail', 1))")
    checkAnswer(sql("select roll,person,person.detail from table1"),
      Seq(Row("abc", Row(1), 1)))
    checkAnswer(sql("select person,person.detail from table1"),
      Seq(Row(Row(1), 1)))
    checkAnswer(sql("select roll,person from table1"), Seq(Row("abc", Row(1))))
    checkAnswer(sql("select roll from table1"), Seq(Row("abc")))
  }

  test("test projection pushDown for Array") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll string,person array<int>) " +
      "STORED AS carbondata")
    sql("insert into table1 values('abc',array(1,2,3))")
    sql("select * from table1").show(false)
    checkAnswer(sql("select roll,person from table1"),
      Seq(Row("abc", mutable.WrappedArray.make(Array(1, 2, 3)))))
  }

  test("test Projection PushDown for StructofArray - Integer type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:array<int>>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1,named_struct('detail', array(1,2)))")
    checkAnswer(sql("select person.detail[0] from table1"), Seq(Row(1)))
    checkAnswer(sql("select person.detail[1] from table1"), Seq(Row(2)))
    checkAnswer(sql("select roll,person from table1"),
      Seq(Row(1, Row(mutable.WrappedArray.make(Array(1, 2))))))
    checkAnswer(sql("select roll,person.detail[0],person,person.detail[1] from table1"),
      Seq(Row(1, 1, Row(mutable.WrappedArray.make(Array(1, 2))), 2)))
  }

  test("test Projection PushDown for Struct - String type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:string>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1,named_struct('detail', 'abc'))")
    checkExistence(sql("select person from table1"), true, "abc")
    checkAnswer(sql("select roll,person,person.detail from table1"), Seq(Row(1, Row("abc"), "abc")))
    checkExistence(sql("select person.detail from table1"), true, "abc")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row("abc"))))
    checkAnswer(sql("select roll,person,roll,person from table1"),
      Seq(Row(1, Row("abc"), 1, Row("abc"))))
  }

  test("test Projection PushDown for StructofArray - String type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:array<string>>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1,named_struct('detail', array('abc','bcd')))")
    checkAnswer(sql("select person.detail[0] from table1"), Seq(Row("abc")))
    checkAnswer(sql("select person.detail[1] from table1"), Seq(Row("bcd")))
    checkAnswer(sql("select roll,person from table1"),
      Seq(Row(1, Row(mutable.WrappedArray.make(Array("abc", "bcd"))))))
    checkAnswer(sql("select roll,person.detail[0],person,person.detail[1] from table1"),
      Seq(Row(1, "abc", Row(mutable.WrappedArray.make(Array("abc", "bcd"))), "bcd")))
  }

  test("test Projection PushDown for Array - String type when Array is Empty") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "FAIL")
    sql("drop table if exists table1")
    sql("create table table1 (detail array<string>) STORED AS carbondata")
    sql("insert into table1 values(array(''))")
    checkAnswer(sql("select detail[0] from table1"), Seq(Row("")))
    sql("drop table if exists table1")
  }

  test("test Projection PushDown for Struct - Array type when Array is Empty") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "FAIL")
    sql("drop table if exists table1")
    sql("create table table1 (person struct<detail:array<string>,age:int>) STORED AS carbondata")
    sql("insert into table1 values(named_struct('detail', array(''), 'age', 1))")
    checkAnswer(sql("select person.detail[0] from table1"), Seq(Row("")))
    checkAnswer(sql("select person.age from table1"), Seq(Row(1)))
    sql("drop table if exists table1")
  }

  test("test Projection PushDown for Struct - Double type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:double>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1, named_struct('detail', 10.00))")
    checkExistence(sql("select person from table1"), true, "10.0")
    checkAnswer(sql("select roll,person,person.detail from table1"), Seq(Row(1, Row(10.0), 10.0)))
    checkExistence(sql("select person.detail from table1"), true, "10.0")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(10.0))))
  }

  test("test Projection PushDown for StructofArray - Double type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:array<double>>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1, named_struct('detail', array(10.00,20.00)))")
    checkAnswer(sql("select person.detail[0] from table1"), Seq(Row(10.0)))
    checkAnswer(sql("select person.detail[1] from table1"), Seq(Row(20.0)))
    checkAnswer(sql("select roll,person from table1"),
      Seq(Row(1, Row(mutable.WrappedArray.make(Array(10.0, 20.0))))))
  }

  test("test Projection PushDown for Struct - Decimal type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:decimal(3,2)>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1, named_struct('detail', 3.4))")
    checkExistence(sql("select person from table1"), true, "3")
    checkExistence(sql("select person.detail from table1"), true, "3")
    checkAnswer(sql("select roll,person.detail from table1"), Seq(Row(1, 3.40)))
  }

  test("test Projection PushDown for StructofArray - Decimal type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:array<decimal(3,2)>>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1, named_struct('detail', array(3.4,4.2)))")
    checkAnswer(sql("select person.detail[0] from table1"), Seq(Row(3.40)))
    checkAnswer(sql("select person.detail[1] from table1"), Seq(Row(4.20)))
    checkAnswer(sql("select roll,person.detail[0] from table1"), Seq(Row(1, 3.40)))
  }

  test("test Projection PushDown for Struct - timestamp type") {
    sql("DROP TABLE IF EXISTS table1")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(
      "create table table1 (roll int,person Struct<detail:timestamp>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1, named_struct('detail', '2018-01-01 00:00:00.0'))")
    checkExistence(sql("select person from table1"), true, "2018-01-01 00:00:00.0")
    checkAnswer(sql("select person,roll,person.detail from table1"),
      Seq(Row(Row(Timestamp.valueOf("2018-01-01 00:00:00.0")), 1,
        Timestamp.valueOf("2018-01-01 00:00:00.0"))))
    checkAnswer(sql("select roll,person from table1"),
      Seq(Row(1, Row(Timestamp.valueOf("2018-01-01 00:00:00.0")))))
    checkAnswer(sql("select roll,person.detail from table1"),
      Seq(Row(1, Timestamp.valueOf("2018-01-01 00:00:00.0"))))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
  }

  test("test Projection PushDown for StructofArray - timestamp type") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:array<timestamp>>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1, named_struct('detail', array('2018-01-01 00:00:00.0','2017-01-01 00:00:00.0')))")
    checkExistence(sql("select person.detail[0] from table1"), true, "2018-01-01 00:00:00.0")
    checkExistence(sql("select person.detail[1] from table1"), true, "2017-01-01 00:00:00.0")
    checkAnswer(sql("select roll,person from table1"),
      Seq(Row(1, Row(mutable.WrappedArray
        .make(Array(Timestamp.valueOf("2018-01-01 00:00:00.0"),
          Timestamp.valueOf("2017-01-01 00:00:00.0")))))))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }

  test("test Projection PushDown for Struct - long type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:long>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1, named_struct('detail', 2018888))")
    checkExistence(sql("select person from table1"), true, "2018888")
    checkAnswer(sql("select person,roll,person.detail from table1"),
      Seq(Row(Row(2018888), 1, 2018888)))
    checkExistence(sql("select person.detail from table1"), true, "2018888")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(2018888))))
  }

  test("test Projection PushDown for StructofArray - long type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:array<long>>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1, named_struct('detail', array(2018888,2018889)))")
    checkAnswer(sql("select person.detail[0] from table1"), Seq(Row(2018888)))
    checkAnswer(sql("select person.detail[1] from table1"), Seq(Row(2018889)))
    checkAnswer(sql("select person,roll from table1"),
      Seq(Row(Row(mutable.WrappedArray.make(Array(2018888, 2018889))), 1)))
  }

  test("test Projection PushDown for Struct - short type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:short>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1, named_struct('detail', 20))")
    checkExistence(sql("select person from table1"), true, "20")
    checkAnswer(sql("select person,roll,person.detail from table1"), Seq(Row(Row(20), 1, 20)))
    checkExistence(sql("select person.detail from table1"), true, "20")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(20))))
  }

  test("test Projection PushDown for StructofArray - short type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:array<short>>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1, named_struct('detail', array(20,30)))")
    checkAnswer(sql("select person.detail[0] from table1"), Seq(Row(20)))
    checkAnswer(sql("select person.detail[1] from table1"), Seq(Row(30)))
    checkAnswer(sql("select person,roll from table1"),
      Seq(Row(Row(mutable.WrappedArray.make(Array(20, 30))), 1)))
  }

  test("test Projection PushDown for Struct - boolean type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:boolean>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1, named_struct('detail', true))")
    checkExistence(sql("select person from table1"), true, "true")
    checkAnswer(sql("select person,roll,person.detail from table1"), Seq(Row(Row(true), 1, true)))
    checkExistence(sql("select person.detail from table1"), true, "true")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(true))))
  }

  test("test Projection PushDown for StructofArray - boolean type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:array<boolean>>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1, named_struct('detail', array(true,false)))")
    checkAnswer(sql("select person.detail[0] from table1"), Seq(Row(true)))
    checkAnswer(sql("select person.detail[1] from table1"), Seq(Row(false)))
    checkAnswer(sql("select person,roll from table1"),
      Seq(Row(Row(mutable.WrappedArray.make(Array(true, false))), 1)))
  }

  test("test Projection PushDown for StructofStruct - Integer type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:Struct<age:int>>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1, named_struct('detail', named_struct('age', 1)))")
    checkExistence(sql("select person from table1"), true, "1")
    checkAnswer(sql("select person,roll,person.detail from table1"),
      Seq(Row(Row(Row(1)), 1, Row(1))))
    checkExistence(sql("select person.detail.age from table1"), true, "1")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(Row(1)))))
  }

  test("test Projection PushDown for StructofStruct - String type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:Struct<age:string>>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1, named_struct('detail', named_struct('age', 'abc')))")
    checkExistence(sql("select person from table1"), true, "abc")
    checkAnswer(sql("select person,person.detail from table1"),
      Seq(Row(Row(Row("abc")), Row("abc"))))
    checkExistence(sql("select person.detail.age from table1"), true, "abc")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(Row("abc")))))
  }

  test("test Projection PushDown for StructofStruct - Double type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:Struct<age:double>>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1, named_struct('detail', named_struct('age', 10.00)))")
    checkExistence(sql("select person from table1"), true, "10.0")
    checkAnswer(sql("select person,person.detail from table1"), Seq(Row(Row(Row(10.0)), Row(10.0))))
    checkExistence(sql("select person.detail.age from table1"), true, "10.0")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(Row(10.0)))))
  }

  test("test Projection PushDown for StructofStruct - Decimal type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:Struct<age:decimal(3,2)>>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1, named_struct('detail', named_struct('age', 3.2)))")
    checkExistence(sql("select person from table1"), true, "3")
    checkExistence(sql("select person.detail.age from table1"), true, "3")
  }

  test("test Projection PushDown for StructofStruct - timestamp type") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:Struct<age:timestamp>>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1, named_struct('detail', named_struct('age', '2018-01-01 00:00:00.0')))")
    checkExistence(sql("select person from table1"), true, "2018-01-01 00:00:00.0")
    checkAnswer(sql("select person,person.detail from table1"),
      Seq(Row(Row(Row(Timestamp.valueOf("2018-01-01 00:00:00.0"))),
        Row(Timestamp.valueOf("2018-01-01 00:00:00.0")))))
    checkExistence(sql("select person.detail.age from table1"), true, "2018-01-01 00:00:00.0")
    checkAnswer(sql("select roll,person from table1"),
      Seq(Row(1, Row(Row(Timestamp.valueOf("2018-01-01 00:00:00.0"))))))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }

  test("test Projection PushDown for StructofStruct - long type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:Struct<age:long>>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1, named_struct('detail', named_struct('age', 2018888)))")
    checkExistence(sql("select person from table1"), true, "2018888")
    checkAnswer(sql("select person,person.detail from table1"),
      Seq(Row(Row(Row(2018888)), Row(2018888))))
    checkExistence(sql("select person.detail.age from table1"), true, "2018888")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(Row(2018888)))))

  }

  test("test Projection PushDown for StructofStruct - short type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:Struct<age:short>>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1, named_struct('detail', named_struct('age', 20)))")
    checkExistence(sql("select person from table1"), true, "20")
    checkAnswer(sql("select person,person.detail from table1"), Seq(Row(Row(Row(20)), Row(20))))
    checkExistence(sql("select person.detail.age from table1"), true, "20")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(Row(20)))))
  }

  test("test Projection PushDown for  StructofStruct - boolean type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:Struct<age:boolean>>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1, named_struct('detail', named_struct('age', true)))")
    checkExistence(sql("select person from table1"), true, "true")
    checkAnswer(sql("select person,person.detail from table1"), Seq(Row(Row(Row(true)), Row(true))))
    checkExistence(sql("select person.detail.age from table1"), true, "true")
    checkAnswer(sql("select roll,person from table1"), Seq(Row(1, Row(Row(true)))))
  }

  test("test StructofArray pushdown") {
    sql("DROP TABLE IF EXISTS table1")
    sql("create table table1 (person Struct<detail:string,ph:array<int>>) STORED AS carbondata ")
    sql("insert into table1 values(named_struct('detail', 'abc', 'ph', array(2)))")
    sql("select person from table1").show(false)
    sql("select person.detail, person.ph[0] from table1").show(false)
  }

  test("test Projection PushDown for Struct - Merge column") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:int,age:string,height:double>) " +
      "STORED AS carbondata")
    sql(
      "load data inpath '" + resourcesPath +
      "/Struct.csv' into table table1 options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,person','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'='&')")
    checkAnswer(sql("select person from table1"), Seq(
      Row(Row(11, "abc", 10.0)),
      Row(Row(12, "abcd", 10.01)),
      Row(Row(13, "abce", 10.02)),
      Row(Row(14, "abcr", 10.03)),
      Row(Row(15, "abct", 10.04)),
      Row(Row(16, "abcn", 10.05)),
      Row(Row(17, "abcq", 10.06)),
      Row(Row(18, "abcs", 10.07)),
      Row(Row(19, "abcm", 10.08)),
      Row(Row(20, "abck", 10.09))
    ))
    checkAnswer(sql("select person.detail,person.age,person.height from table1"), Seq(
      Row(11, "abc", 10.0),
      Row(12, "abcd", 10.01),
      Row(13, "abce", 10.02),
      Row(14, "abcr", 10.03),
      Row(15, "abct", 10.04),
      Row(16, "abcn", 10.05),
      Row(17, "abcq", 10.06),
      Row(18, "abcs", 10.07),
      Row(19, "abcm", 10.08),
      Row(20, "abck", 10.09)))
    checkAnswer(sql("select person.age,person.detail,person.height from table1"), Seq(
      Row("abc", 11, 10.0),
      Row("abcd", 12, 10.01),
      Row("abce", 13, 10.02),
      Row("abcr", 14, 10.03),
      Row("abct", 15, 10.04),
      Row("abcn", 16, 10.05),
      Row("abcq", 17, 10.06),
      Row("abcs", 18, 10.07),
      Row("abcm", 19, 10.08),
      Row("abck", 20, 10.09)))
    checkAnswer(sql("select person.height,person.age,person.detail from table1"), Seq(
      Row(10.0, "abc", 11),
      Row(10.01, "abcd", 12),
      Row(10.02, "abce", 13),
      Row(10.03, "abcr", 14),
      Row(10.04, "abct", 15),
      Row(10.05, "abcn", 16),
      Row(10.06, "abcq", 17),
      Row(10.07, "abcs", 18),
      Row(10.08, "abcm", 19),
      Row(10.09, "abck", 20)))
  }

  test("test Projection PushDown for StructofStruct - Merging columns") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,person Struct<detail:Struct<age:int,name:string," +
      "height:double>>) STORED AS carbondata")
    sql(
      "load data inpath '" + resourcesPath +
      "/StructofStruct.csv' into table table1 options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,person','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'='&')")
    checkAnswer(sql("select person from table1"), Seq(
      Row(Row(Row(11, "abc", 10.0))),
      Row(Row(Row(12, "abcd", 10.01))),
      Row(Row(Row(13, "abce", 10.02))),
      Row(Row(Row(14, "abcr", 10.03))),
      Row(Row(Row(15, "abct", 10.04))),
      Row(Row(Row(16, "abcn", 10.05))),
      Row(Row(Row(17, "abcq", 10.06))),
      Row(Row(Row(18, "abcs", 10.07))),
      Row(Row(Row(19, "abcm", 10.08))),
      Row(Row(Row(20, "abck", 10.09)))))
    checkAnswer(sql("select person.detail.age,person.detail.name,person.detail.height from table1"),
      Seq(
        Row(11, "abc", 10.0),
        Row(12, "abcd", 10.01),
        Row(13, "abce", 10.02),
        Row(14, "abcr", 10.03),
        Row(15, "abct", 10.04),
        Row(16, "abcn", 10.05),
        Row(17, "abcq", 10.06),
        Row(18, "abcs", 10.07),
        Row(19, "abcm", 10.08),
        Row(20, "abck", 10.09)))
    checkAnswer(sql("select person.detail.name,person.detail.age,person.detail.height from table1"),
      Seq(
        Row("abc", 11, 10.0),
        Row("abcd", 12, 10.01),
        Row("abce", 13, 10.02),
        Row("abcr", 14, 10.03),
        Row("abct", 15, 10.04),
        Row("abcn", 16, 10.05),
        Row("abcq", 17, 10.06),
        Row("abcs", 18, 10.07),
        Row("abcm", 19, 10.08),
        Row("abck", 20, 10.09)))
    checkAnswer(sql("select person.detail.height,person.detail.name,person.detail.age from table1"),
      Seq(
        Row(10.0, "abc", 11),
        Row(10.01, "abcd", 12),
        Row(10.02, "abce", 13),
        Row(10.03, "abcr", 14),
        Row(10.04, "abct", 15),
        Row(10.05, "abcn", 16),
        Row(10.06, "abcq", 17),
        Row(10.07, "abcs", 18),
        Row(10.08, "abcm", 19),
        Row(10.09, "abck", 20)))
    checkAnswer(sql("select person.detail from table1"),
      Seq(
        Row(Row(11, "abc", 10.0)),
        Row(Row(12, "abcd", 10.01)),
        Row(Row(13, "abce", 10.02)),
        Row(Row(14, "abcr", 10.03)),
        Row(Row(15, "abct", 10.04)),
        Row(Row(16, "abcn", 10.05)),
        Row(Row(17, "abcq", 10.06)),
        Row(Row(18, "abcs", 10.07)),
        Row(Row(19, "abcm", 10.08)),
        Row(Row(20, "abck", 10.09))))
    checkAnswer(sql("select person.detail.age from table1"), Seq(
      Row(11), Row(12), Row(13), Row(14), Row(15), Row(16), Row(17), Row(18), Row(19), Row(20)))
  }

  test("test Projection PushDown for more than one Struct column- Integer type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll string,person Struct<detail:int,age:string>,person1 " +
      "Struct<detail:int,age:array<string>>) " +
      "STORED AS carbondata")
    sql("insert into table1 values('abc', named_struct('detail', 1, 'age', 'abc'), named_struct('detail', 2, 'age', array('cde')))")
    sql("select person.detail,person1.age from table1").show(false)
  }

  test("test Projection PushDown for more than one Struct column Cases -1") {
    sql("drop table if exists test")
    sql("create table test (a struct<b:int, c:struct<d:int,e:int>>) STORED AS carbondata")
    sql("insert into test values(named_struct('b', 1, 'c', named_struct('d', 2, 'e', 3)))")
    checkAnswer(sql("select * from test"), Seq(Row(Row(1, Row(2, 3)))))
    checkAnswer(sql("select a.b,a.c from test"), Seq(Row(1, Row(2, 3))))
    checkAnswer(sql("select a.c, a.b from test"), Seq(Row(Row(2, 3), 1)))
    checkAnswer(sql("select a.c,a,a.b from test"), Seq(Row(Row(2, 3), Row(1, Row(2, 3)), 1)))
    checkAnswer(sql("select a.c from test"), Seq(Row(Row(2, 3))))
    checkAnswer(sql("select a.b from test"), Seq(Row(1)))
    sql("drop table if exists test")
  }

  test("test Projection PushDown for with more than one StructofArray column - Integer type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (person Struct<detail:array<int>>,person1 Struct<detail:array<int>>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(named_struct('detail', array(1)), named_struct('detail', array(2)))")
    sql("select person.detail[0],person1.detail[0] from table1").show(false)
  }

  test("test Projection PushDown for StructofStruct case1 - Merging columns") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,a struct<b:int,c:string,d:int,e:string,f:struct<g:int," +
      "h:string,i:int>,j:int>) " +
      "STORED AS carbondata")
    sql("insert into table1 values(1,named_struct('b', 1, 'c', 'abc', 'd', 2, 'e', 'efg', 'f', named_struct('g', 3, 'h', 'mno', 'i', 4), 'j', 5))")
    sql("insert into table1 values(2,named_struct('b', 1, 'c', 'abc', 'd', 2, 'e', 'efg', 'f', named_struct('g', 3, 'h', 'mno', 'i', 4), 'j', 5))")
    sql("insert into table1 values(3,named_struct('b', 1, 'c', 'abc', 'd', 2, 'e', 'efg', 'f', named_struct('g', 3, 'h', 'mno', 'i', 4), 'j', 5))")
    checkAnswer(sql("select a.b from table1"), Seq(Row(1), Row(1), Row(1)))
    checkAnswer(sql("select a.c from table1"), Seq(Row("abc"), Row("abc"), Row("abc")))
    checkAnswer(sql("select a.d from table1"), Seq(Row(2), Row(2), Row(2)))
    checkAnswer(sql("select a.e from table1"), Seq(Row("efg"), Row("efg"), Row("efg")))
    checkAnswer(sql("select a.f from table1"),
      Seq(Row(Row(3, "mno", 4)), Row(Row(3, "mno", 4)), Row(Row(3, "mno", 4))))
    checkAnswer(sql("select a.f.g  from table1"), Seq(Row(3), Row(3), Row(3)))
    checkAnswer(sql("select a.f.h  from table1"), Seq(Row("mno"), Row("mno"), Row("mno")))
    checkAnswer(sql("select a.f.i  from table1"), Seq(Row(4), Row(4), Row(4)))
    checkAnswer(sql("select a.f.g,a.f.h,a.f.i  from table1"),
      Seq(Row(3, "mno", 4), Row(3, "mno", 4), Row(3, "mno", 4)))
    checkAnswer(sql("select a.b,a.f from table1"),
      Seq(Row(1, Row(3, "mno", 4)), Row(1, Row(3, "mno", 4)), Row(1, Row(3, "mno", 4))))
    checkAnswer(sql("select a.c,a.f from table1"),
      Seq(Row("abc", Row(3, "mno", 4)), Row("abc", Row(3, "mno", 4)), Row("abc", Row(3, "mno", 4))))
    checkAnswer(sql("select a.d,a.f from table1"),
      Seq(Row(2, Row(3, "mno", 4)), Row(2, Row(3, "mno", 4)), Row(2, Row(3, "mno", 4))))
    checkAnswer(sql("select a.j from table1"), Seq(Row(5), Row(5), Row(5)))
    checkAnswer(sql("select * from table1"),
      Seq(Row(1, Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5)),
        Row(2, Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5)),
        Row(3, Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5))))
    checkAnswer(sql("select *,a from table1"),
      Seq(Row(1,
        Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5),
        Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5)),
        Row(2,
          Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5),
          Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5)),
        Row(3,
          Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5),
          Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5))))
  }

  test("test Projection PushDown for StructofStruct for Dictionary Include ") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (roll int,a struct<b:int,c:string,d:int,e:string,f:struct<g:int," +
      "h:string,i:int>,j:int>) STORED AS carbondata ")
    sql("insert into table1 values(1,named_struct('b', 1, 'c', 'abc', 'd', 2, 'e', 'efg', 'f', named_struct('g', 3, 'h', 'mno', 'i', 4), 'j', 5))")
    sql("insert into table1 values(2,named_struct('b', 1, 'c', 'abc', 'd', 2, 'e', 'efg', 'f', named_struct('g', 3, 'h', 'mno', 'i', 4), 'j', 5))")
    sql("insert into table1 values(3,named_struct('b', 1, 'c', 'abc', 'd', 2, 'e', 'efg', 'f', named_struct('g', 3, 'h', 'mno', 'i', 4), 'j', 5))")
    checkAnswer(sql("select a.b from table1"), Seq(Row(1), Row(1), Row(1)))
    checkAnswer(sql("select a.c from table1"), Seq(Row("abc"), Row("abc"), Row("abc")))
    checkAnswer(sql("select a.d from table1"), Seq(Row(2), Row(2), Row(2)))
    checkAnswer(sql("select a.e from table1"), Seq(Row("efg"), Row("efg"), Row("efg")))
    checkAnswer(sql("select a.f from table1"),
      Seq(Row(Row(3, "mno", 4)), Row(Row(3, "mno", 4)), Row(Row(3, "mno", 4))))
    checkAnswer(sql("select a.f.g  from table1"), Seq(Row(3), Row(3), Row(3)))
    checkAnswer(sql("select a.f.h  from table1"), Seq(Row("mno"), Row("mno"), Row("mno")))
    checkAnswer(sql("select a.f.i  from table1"), Seq(Row(4), Row(4), Row(4)))
    checkAnswer(sql("select a.f.g,a.f.h,a.f.i  from table1"),
      Seq(Row(3, "mno", 4), Row(3, "mno", 4), Row(3, "mno", 4)))
    checkAnswer(sql("select a.b,a.f from table1"),
      Seq(Row(1, Row(3, "mno", 4)), Row(1, Row(3, "mno", 4)), Row(1, Row(3, "mno", 4))))
    checkAnswer(sql("select a.c,a.f from table1"),
      Seq(Row("abc", Row(3, "mno", 4)), Row("abc", Row(3, "mno", 4)), Row("abc", Row(3, "mno", 4))))
    checkAnswer(sql("select a.d,a.f from table1"),
      Seq(Row(2, Row(3, "mno", 4)), Row(2, Row(3, "mno", 4)), Row(2, Row(3, "mno", 4))))
    checkAnswer(sql("select a.j from table1"), Seq(Row(5), Row(5), Row(5)))
    checkAnswer(sql("select * from table1"),
      Seq(Row(1, Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5)),
        Row(2, Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5)),
        Row(3, Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5))))
    checkAnswer(sql("select *,a from table1"),
      Seq(Row(1,
        Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5),
        Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5)),
        Row(2,
          Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5),
          Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5)),
        Row(3,
          Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5),
          Row(1, "abc", 2, "efg", Row(3, "mno", 4), 5))))
  }

  test("ArrayofArray PushDown") {
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(a array<array<int>>) STORED AS carbondata")
    sql("insert into test values(array(array(1))) ")
    sql("select a[0][0] from test").show(false)
  }

  test("Struct and ArrayofArray PushDown") {
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(a array<array<int>>,b struct<c:array<int>>) STORED AS carbondata")
    sql("insert into test values(array(array(1)),named_struct('c', array(1))) ")
    sql("select b.c[0],a[0][0] from test").show(false)
  }

  test("test structofarray with count(distinct)") {
    sql("DROP TABLE IF EXISTS test")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(
      "create table test(cus_id string, struct_of_array struct<id:int,date:timestamp," +
      "sno:array<int>,sal:array<double>,state:array<string>,date1:array<timestamp>>) " +
      "STORED AS carbondata")
    sql("insert into test values('cus_01',named_struct('id', 1, 'date', '2017-01-01 00:00:00', 'sno', array(1,2), 'sal', array(2.0,3.0), 'state', array('ab','ac'), 'date1', array('2018-01-01 00:00:00')))")
    //    sql("select *from test").show(false)
    sql(
      "select struct_of_array.state[0],count(distinct struct_of_array.id) as count_int,count" +
      "(distinct struct_of_array.state[0]) as count_string from test group by struct_of_array" +
      ".state[0]")
      .show(false)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }

  test("test arrayofstruct with count(distinct)") {
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(cus_id string,array_of_struct array<struct<id:int,country:string," +
        "state:string,city:string>>) STORED AS carbondata")
    sql("insert into test values('cus_01',array(named_struct('id', 123, 'country', 'abc', 'state', 'mno', 'city', 'xyz'),named_struct('id', 1234, 'country', 'abc1', 'state', 'mno1', 'city', 'xyz1')))")
    checkAnswer(sql("select array_of_struct.state[0],count(distinct array_of_struct.id[0]) as count_country," +
      "count(distinct array_of_struct.state[0]) as count_city from test group by array_of_struct" +
      ".state[0]"), Seq(Row("mno", 1, 1)))
  }

  test("test struct complex type with filter") {
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(id int,a struct<b:int,c:int>) STORED AS carbondata")
    sql("insert into test values(1,named_struct('b', 2, 'c', 3))")
    sql("insert into test values(3,named_struct('b', 5, 'c', 3))")
    sql("insert into test values(2,named_struct('b', 4, 'c', 5))")
    checkAnswer(sql("select a.b from test where id=3"),Seq(Row(5)))
    checkAnswer(sql("select a.b from test where a.c!=3"),Seq(Row(4)))
    checkAnswer(sql("select a.b from test where a.c=3"),Seq(Row(5),Row(2)))
    checkAnswer(sql("select a.b from test where id=1 or !a.c=3"),Seq(Row(4),Row(2)))
    checkAnswer(sql("select a.b from test where id=3 or a.c=3"),Seq(Row(5),Row(2)))
  }

  /* test struct of date*/
  test("test struct complex type with date") {
    var backupdateFormat = CarbonProperties.getInstance().getProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT, CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(a struct<b:date>) STORED AS carbondata")
    sql("insert into test values(named_struct('b', '1992-02-19'))")
    checkAnswer(sql("select * from test "), Row(Row(java.sql.Date.valueOf("1992-02-19"))))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        backupdateFormat)
  }

  test("test Projection with two struct") {
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(id int,a struct<b:int,c:int>, d struct<e:int,f:int>) STORED AS carbondata")
    sql("insert into test values(1, named_struct('b', 2, 'c', 3), named_struct('e', 3, 'f', 2))")
    checkAnswer(sql("select * from test"),Seq(Row(1,Row(2,3),Row(3,2))))
    checkAnswer(sql("select a.b,id,a.c from test"),Seq(Row(2,1,3)))
    checkAnswer(sql("select d.e,d.f from test"),Seq(Row(3,2)))
    checkAnswer(sql("select a.b,d.e,d.f,id,a.c from test"),Seq(Row(2,3,2,1,3)))
    checkAnswer(sql("select a.b,d.e,id,a.c,d.f,a.c from test"), Seq(Row(2, 3, 1, 3, 2, 3)))
    checkAnswer(sql("select a.b,d.e,d.f from test"), Seq(Row(2, 3, 2)))
    checkAnswer(sql("select a.b,a.c,id,d.e,d.f from test"), Seq(Row(2, 3, 1, 3, 2)))
    checkAnswer(sql("select d.e,d.f,id,a.b,a.c from test"), Seq(Row(3, 2, 1, 2, 3)))
    checkAnswer(sql("select d.e,1,d.f from test"), Seq(Row(3, 1, 2)))
    checkAnswer(sql("select d.e,1,d.f,a.b,id,a.c from test"), Seq(Row(3, 1, 2, 2, 1, 3)))
    checkAnswer(sql("select d.e+1,d.f,a.b,d.e,a.c,id from test"), Seq(Row(4,2,2,3,3,1)))
    checkAnswer(sql("select d.f,a.c,a.b,id,a.c,a.b from test"), Seq(Row(2,3,2,1,3,2)))
    checkAnswer(sql("select sum(d.e) from test"), Seq(Row(3)))
    checkAnswer(sql("select d.f,a.c,a.b,id,a.c,a.b,id,1,id,3,d.f from test"), Seq(Row(2,3,2,1,3,2,1,1,1,3,2)))
  }

  test("test project with struct and array") {
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(id int,a struct<b:int,c:int>, d struct<e:int,f:int>,person Struct<detail:array<int>>) STORED AS carbondata")
    sql("insert into test values(1, named_struct('b', 2, 'c', 3), named_struct('e', 3, 'f', 2), named_struct('detail', array(5,6,7,8)))")
    checkAnswer(sql("select * from test"),Seq(Row(1,Row(2,3),Row(3,2),Row(mutable.WrappedArray.make(Array(5,6,7,8))))))
    checkAnswer(sql("select a.b,id,a.c,person.detail[0] from test"),Seq(Row(2,1,3,5)))
    checkAnswer(sql("select a.b,id,a.c,person.detail[0],d.e,d.f,person.detail[1],id from test"),Seq(Row(2,1,3,5,3,2,6,1)))
    checkAnswer(sql("select a.b,id,a.c,person.detail[0],d.e,d.f,person.detail[1],id,1,a.b from test"),Seq(Row(2,1,3,5,3,2,6,1,1,2)))
  }

  test("test block Update for complex datatype") {
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(id int,a struct<b:int,c:int>,d array<int>) STORED AS carbondata")
    sql("insert into test values(1, named_struct('b', 2, 'c', 3), array(4))")
    val structException = intercept[UnsupportedOperationException](
    sql("update test set(a.b)=(4) where id=1").show(false))
    assertResult("Unsupported operation on Complex data type")(structException.getMessage)
    val arrayException = intercept[UnsupportedOperationException](
    sql("update test set(a)=(4) where id=1").show(false))
    assertResult("Unsupported operation on Complex data type")(arrayException.getMessage)
  }

  test("test block partition column") {
    sql("DROP TABLE IF EXISTS test")
    val arrayException = intercept[AnalysisException](
    sql("""
          | CREATE TABLE IF NOT EXISTS test
          | (
          | id Int,
          | vin string,
          | logdate Timestamp,
          | phonenumber Long,
          | country array<string>,
          | salary Int
          | )
          | PARTITIONED BY (area array<string>)
          | STORED AS carbondata
        """.stripMargin))
    assertResult("Cannot use array<string> for partition column;")(arrayException.getMessage)
    sql("DROP TABLE IF EXISTS test")
    val structException = intercept[AnalysisException](
      sql("""
            | CREATE TABLE IF NOT EXISTS test
            | (
            | id Int,
            | vin string,
            | logdate Timestamp,
            | phonenumber Long,
            | country array<string>,
            | salary Int
            | )
            | PARTITIONED BY (area struct<b:int>)
            | STORED AS carbondata
          """.stripMargin)
    )
    assertResult("Cannot use struct<b:int> for partition column;")(structException.getMessage)
  }

  test("test complex datatype double for encoding") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (person struct<height:double>) STORED AS carbondata")
    sql("insert into table1 values(named_struct('height', 1000000000))")
    checkExistence(sql("select * from table1"),true,"1.0E9")
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (person struct<height:double>) STORED AS carbondata")
    sql("insert into table1 values(named_struct('height', 12345678912))")
    checkExistence(sql("select * from table1"),true,"1.2345678912E10")
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (person struct<b:array<double>>) STORED AS carbondata")
    sql("insert into table1 values(named_struct('b', array(10000000,2000000000,2900000000)))")
    checkExistence(sql("select * from table1"),true,"2.9E9")
  }

  test("test compaction - auto merge") {
    sql("DROP TABLE IF EXISTS table1")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    sql(
      "create table table1 (roll int,person Struct<detail:int,age:string,height:double>) " +
      "STORED AS carbondata")
    sql(
      "load data inpath '" + resourcesPath +
      "/Struct.csv' into table table1 options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,person','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'='&')")
    sql(
      "load data inpath '" + resourcesPath +
      "/Struct.csv' into table table1 options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,person','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'='&')")
    sql(
      "load data inpath '" + resourcesPath +
      "/Struct.csv' into table table1 options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,person','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'='&')")
    sql(
      "load data inpath '" + resourcesPath +
      "/Struct.csv' into table table1 options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,person','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'='&')")
    checkExistence(sql("show segments for table table1"),true, "Compacted")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
  }

  test("decimal with two level struct type") {
    sql("DROP TABLE IF EXISTS test")
    sql(
      "create table test(id int,a struct<c:struct<d:decimal(20,10)>>) STORED AS carbondata ")
    checkExistence(sql("desc test"),true,"struct<c:struct<d:decimal(20,10)>>")
    checkExistence(sql("describe formatted test"),true,"struct<c:struct<d:decimal(20,10)>>")
    sql("insert into test values(1, named_struct('c', named_struct('d', 3999.999)))")
    checkExistence(sql("select * from test"),true,"3999.9990000000")
  }

  test("test dictionary include for second struct and array column") {
    sql("DROP TABLE IF EXISTS test")
    sql(
      "create table test(id int,a struct<b:int,c:int>, d struct<e:int,f:int>, d1 struct<e1:int," +
      "f1:int>) STORED AS carbondata ")
    sql("insert into test values(1, named_struct('b', 2, 'c', 3), named_struct('e', 4, 'f', 5), named_struct('e1', 6, 'f1', 7))")
    checkAnswer(sql("select * from test"),Seq(Row(1,Row(2,3),Row(4,5),Row(6,7))))
    sql("DROP TABLE IF EXISTS test")
    sql(
      "create table test(a array<int>, b array<int>) STORED AS carbondata")
    sql("insert into test values(array(1),array(2)) ")
    checkAnswer(sql("select b[0] from test"),Seq(Row(2)))
    sql("DROP TABLE IF EXISTS test")
    sql(
      "create table test(intval array<array<int>>,str array<array<string>>, bool " +
      "array<array<boolean>>, sint array<array<short>>, big array<array<bigint>>)  STORED AS carbondata ")
    sql("insert into test values(array(array(1)), array(array('ab')), array(array(true)), array(array(22)), array(array(33))) ")
    checkExistence(sql("select * from test"), true, "33")
  }

  test("date with struct and array") {
    printConfiguration()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "true")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION_DEFAULT)
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(a struct<b:date>) STORED AS carbondata")
    val exception1 = intercept[Exception] {
      sql("insert into test values(named_struct('b', 'a')) ")
    }
    assert(exception1.getMessage
      .contains(
        "Data load failed due to bad record: The value with column name a.b and column data type " +
        "DATE is not a valid DATE type.Please enable bad record logger to know the detail reason."))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION_DEFAULT)
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(a array<date>) STORED AS carbondata")
    val exception2 = intercept[Exception] {
      sql("insert into test values(array('a')) ")
    }
    assert(exception2.getMessage
      .contains(
        "Data load failed due to bad record: The value with column name a.val and column data type " +
        "DATE is not a valid DATE type.Please enable bad record logger to know the detail reason."))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        "MM-dd-yyyy")
    sql("DROP TABLE IF EXISTS test")
    sql("create table test(a struct<d1:date,d2:date>) STORED AS carbondata")
    sql("insert into test values(named_struct('d1', '2012-02-18', 'd2', '2016-12-09'))")
    checkAnswer(sql("select * from test "), Row(Row(java.sql.Date.valueOf("2012-02-18"),java.sql.Date.valueOf("2016-12-09"))))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT,
        CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT_DEFAULT)
  }
  test("test null values in primitive data type and select all data types including complex data type") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      "create table table1 (id int, name string, structField struct<intval:int, stringval:string>) STORED AS carbondata")
    sql("insert into table1 values(null, 'aaa', named_struct('intval', 23, 'stringval', 'bb'))")
    checkAnswer(sql("select * from table1"),Seq(Row(null,"aaa", Row(23,"bb"))))
    checkAnswer(sql("select id,name,structField.intval,structField.stringval from table1"),Seq(Row(null,"aaa",23,"bb")))
    checkAnswer(sql("select id,name,structField.intval,structField.stringval,name from table1"),Seq(Row(null,"aaa",23,"bb","aaa")))
    checkAnswer(sql("select id,name,structField.intval,name,structField.stringval from table1"),Seq(Row(null,"aaa",23,"aaa","bb")))
  }

  test("test array of binary data type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists hive_table")
    sql("create table if not exists hive_table(id int, label boolean, name string," +
        "binaryField array<binary>, autoLabel boolean) row format delimited fields terminated by ','")
    sql("insert into hive_table values(1,true,'abc',array('binary'),false)")
    sql("create table if not exists carbon_table(id int, label boolean, name string," +
        "binaryField array<binary>, autoLabel boolean) STORED AS carbondata")
    sql("insert into carbon_table values(1,true,'abc',array('binary'),false)")
    checkAnswer(sql("SELECT binaryField[0] FROM carbon_table"),
      sql("SELECT binaryField[0] FROM hive_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists hive_table")
  }

  test("test array of huge binary data type") {
    sql("drop table if exists carbon_table")
    sql("create table if not exists carbon_table(id int, label boolean, name string," +
        "binaryField array<binary>, autoLabel boolean) STORED AS carbondata")
    sql(s"insert into carbon_table values(1,true,'abc',array('$hugeBinary'),false)")
    val result = sql("SELECT binaryField[0] FROM carbon_table").collect()
    assert(hugeBinary.equals(new String(result(0).get(0).asInstanceOf[Array[Byte]])))
    sql("drop table if exists carbon_table")
  }

  test("test struct of binary data type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    sql("create table if not exists parquet_table(id int, label boolean, name string," +
        "binaryField struct<b:binary>, autoLabel boolean) using parquet")
    sql("insert into parquet_table values(1,true,'abc',named_struct('b','binary'),false)")
    sql("create table if not exists carbon_table(id int, label boolean, name string," +
        "binaryField struct<b:binary>, autoLabel boolean) STORED AS carbondata")
    sql("insert into carbon_table values(1,true,'abc',named_struct('b','binary'),false)")
    sql("SELECT binaryField.b FROM carbon_table").show(false)
    checkAnswer(sql("SELECT binaryField.b FROM carbon_table"),
      sql("SELECT binaryField.b FROM parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test struct of huge binary data type") {
    sql("drop table if exists carbon_table")
    sql("create table if not exists carbon_table(id int, label boolean, name string," +
        "binaryField struct<b:binary>, autoLabel boolean) stored as carbondata ")
    sql(s"insert into carbon_table values(1,true,'abc',named_struct('b','$hugeBinary'),false)")
    val result = sql("SELECT binaryField.b FROM carbon_table").collect()
    assert(hugeBinary.equals(new String(result(0).get(0).asInstanceOf[Array[Byte]])))
    sql("drop table if exists carbon_table")
  }

  test("test map of binary data type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists hive_table")
    sql("create table if not exists hive_table(id int, label boolean, name string," +
        "binaryField map<int, binary>, autoLabel boolean) row format delimited fields terminated by ','")
    sql("insert into hive_table values(1,true,'abc',map(1,'binary'),false)")
    sql("create table if not exists carbon_table(id int, label boolean, name string," +
        "binaryField map<int, binary>, autoLabel boolean) STORED AS carbondata")
    sql("insert into carbon_table values(1,true,'abc',map(1,'binary'),false)")
    checkAnswer(sql("SELECT binaryField[1] FROM carbon_table"),
      sql("SELECT binaryField[1] FROM hive_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists hive_table")
  }

  test("test map of huge binary data type") {
    sql("drop table if exists carbon_table")
    sql("create table if not exists carbon_table(id int, label boolean, name string," +
        "binaryField map<int, binary>, autoLabel boolean) STORED AS carbondata")
    sql(s"insert into carbon_table values(1,true,'abc',map(1,'$hugeBinary'),false)")
    val result = sql("SELECT binaryField[1] FROM carbon_table").collect()
    assert(hugeBinary.equals(new String(result(0).get(0).asInstanceOf[Array[Byte]])))
    sql("drop table if exists carbon_table")
  }

  test("test map of array and struct binary data type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    sql("create table if not exists parquet_table(id int, label boolean, name string," +
        "binaryField1 map<int, array<binary>>, binaryField2 map<int, struct<b:binary>> ) " +
        "using parquet")
    sql("insert into parquet_table values(1,true,'abc',map(1,array('binary')),map(1," +
        "named_struct('b','binary')))")
    sql("create table if not exists carbon_table(id int, label boolean, name string," +
        "binaryField1 map<int, array<binary>>, binaryField2 map<int, struct<b:binary>> ) " +
        "STORED AS carbondata")
    sql("insert into carbon_table values(1,true,'abc',map(1,array('binary')),map(1," +
        "named_struct('b','binary')))")
    checkAnswer(sql("SELECT binaryField1[1][1] FROM carbon_table"),
      sql("SELECT binaryField1[1][1] FROM parquet_table"))
    checkAnswer(sql("SELECT binaryField2[1].b FROM carbon_table"),
      sql("SELECT binaryField2[1].b FROM parquet_table"))
    sql("drop table if exists parquet_table")
    sql("drop table if exists carbon_table")
  }

  test("test of array of struct and struct of array of binary data type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists hive_table")
    sql("create table if not exists hive_table(id int, label boolean, name string," +
        "binaryField1 array<struct<b1:binary>>, binaryField2 struct<b2:array<binary>> ) " +
        "row format delimited fields terminated by ','")
    sql("insert into hive_table values(1,true,'abc',array(named_struct('b1','binary'))," +
        "named_struct('b2',array('binary')))")
    sql("create table if not exists carbon_table(id int, label boolean, name string," +
        "binaryField1 array<struct<b1:binary>>, binaryField2 struct<b2:array<binary>> ) " +
        "STORED AS carbondata")
    sql("insert into carbon_table values(1,true,'abc',array(named_struct('b1','binary'))," +
        "named_struct('b2',array('binary')))")
    checkAnswer(sql("SELECT binaryField1[1].b1 FROM carbon_table"),
      sql("SELECT  binaryField1[1].b1 FROM hive_table"))
    checkAnswer(sql("SELECT binaryField2.b2[0] FROM carbon_table"),
      sql("SELECT binaryField2.b2[0] FROM hive_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists hive_table")
  }

  test("test dataload to complex of binary type column using load ddl ") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists hive_table")
    sql("create table if not exists hive_table(id int, label boolean, name string," +
        "binaryField1 array<binary>, binaryField2 struct<b2:binary>, binaryField3 map<int," +
        "binary>) row format delimited fields terminated by ','")
    sql(
      "insert into hive_table values(1,true,'abc',array('binary1','binary2'), named_struct('b2'," +
      "'binary1'), map(1,'binary1'))")
    sql("create table if not exists carbon_table(id int, label boolean, name string," +
        "binaryField1 array<binary>, binaryField2 struct<b2:binary>, binaryField3 map<int,binary>) " +
        "STORED AS carbondata")
    sql(
      "load data inpath '" + resourcesPath + "/complexbinary.csv' into table carbon_table options" +
      "('delimiter'=',',  'quotechar'='\\','fileheader'='id,label,name,binaryField1,binaryField2," +
      "binaryField3','complex_delimiter_level_1'='$', 'complex_delimiter_level_2'='&')")
    checkAnswer(sql("SELECT binaryField1[0] FROM carbon_table where id=1"),
      sql("SELECT  binaryField1[0] FROM hive_table where id=1"))
    checkAnswer(sql("SELECT binaryField2.b2 FROM carbon_table where id=1"),
      sql("SELECT  binaryField2.b2 FROM hive_table where id=1"))
    checkAnswer(sql("SELECT binaryField3[1] FROM carbon_table where id=1"),
      sql("SELECT binaryField3[1] FROM hive_table where id=1"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists hive_table")
  }

  test("test when insert select from a parquet table with an struct with binary and custom complex delimiter") {
    var carbonProperties = CarbonProperties.getInstance()
    carbonProperties.addProperty(CarbonCommonConstants.COMPLEX_DELIMITERS_LEVEL_1, "#")

    sql("create table datatype_struct_parquet(price struct<a:binary>) stored as parquet")
    sql("insert into table datatype_struct_parquet values(named_struct('a', 'col1\001col2'))")
    sql("create table datatype_struct_carbondata(price struct<a:binary>) stored as carbondata")
    sql("insert into datatype_struct_carbondata select * from datatype_struct_parquet")
    checkAnswer(
      sql("SELECT * FROM datatype_struct_carbondata"),
      sql("SELECT * FROM datatype_struct_parquet"))
    sql("DROP TABLE IF EXISTS datatype_struct_carbondata")
    sql("DROP TABLE IF EXISTS datatype_struct_parquet")

    carbonProperties.removeProperty(CarbonCommonConstants.COMPLEX_DELIMITERS_LEVEL_1)
  }

  test("[CARBONDATA-3527] Fix 'String length cannot exceed 32000 characters' issue when load data with 'GLOBAL_SORT' from csv files which include big complex type data") {
    val tableName = "complexdata3_table"
    sql(s"drop table if exists ${tableName}")
    sql(
      s"""
         |CREATE TABLE IF NOT EXISTS ${tableName} (
         | begin_time LONG,
         | id string,
         | phone string,
         | other_phone string,
         | vtl LONG,
         | gender string,
         | lang string,
         | lang_dec string,
         | phone_country string,
         | phone_province string,
         | phone_city string,
         | other_phone_country string,
         | other_phone_province string,
         | other_phone_city string,
         | call_type INT,
         | begin_hhmm INT,
         | ds string,
         | voice_flag INT,
         | dss string,
         | dur LONG,
         | modela array < array < FLOAT >>, modelb array < array < FLOAT >>, modela_pk array < array < FLOAT >>, modelb_pk array < array < FLOAT >>, modela_ms array < array < FLOAT >>, modelb_ms array < array < FLOAT >>, tl LONG,
         | lang_sc FLOAT,
         | nlp_sc FLOAT,
         | create_time LONG,
         | cdr_create_time LONG,
         | fulltext string,
         | tag_label string,
         | tag_memo string,
         | tag_listen string,
         | tag_imp string,
         | prop string,
         | files string
         | )
         | STORED AS carbondata TBLPROPERTIES (
         | 'SORT_COLUMNS' = 'begin_time,id,phone,other_phone,vtl,gender,lang,lang_dec,phone_country,phone_province,phone_city,other_phone_country,other_phone_province,other_phone_city,call_type,begin_hhmm,ds,voice_flag',
         | 'SORT_SCOPE' = 'GLOBAL_SORT','LONG_STRING_COLUMNS' = 'fulltext,files')""".stripMargin)
    sql(s"""LOAD DATA inpath '${resourcesPath}/complexdata3.csv' INTO table ${tableName}
        options('DELIMITER'='\t','QUOTECHAR'='"','COMMENTCHAR'='#','HEADER'='false',
                'FILEHEADER'='id,phone,phone_country,phone_province,phone_city,other_phone,other_phone_country,other_phone_province,other_phone_city,call_type,begin_time,begin_hhmm,ds,dss,dur,voice_flag,modela,modelb,modela_pk,modelb_pk,modela_ms,modelb_ms,lang,lang_dec,lang_sc,gender,nlp_sc,tl,vtl,create_time,cdr_create_time,fulltext,tag_label,tag_memo,tag_listen,tag_imp,prop,files',
                'MULTILINE'='true','ESCAPECHAR'='\','COMPLEX_DELIMITER_LEVEL_1'='\\001','COMPLEX_DELIMITER_LEVEL_2'='\\002'
                )""")
    checkAnswer(sql(s"select count(1) from ${tableName}"), Seq(Row(10)))
    checkAnswer(sql(s"select modela[0][0], modela_ms[0][1] from ${tableName} where id = 'e01a1773-bd37-40be-a1de-d7e74837a281'"),
      Seq(Row(0.0, 0.10781755)))
    sql(s"drop table if exists ${tableName}")
  }
}
