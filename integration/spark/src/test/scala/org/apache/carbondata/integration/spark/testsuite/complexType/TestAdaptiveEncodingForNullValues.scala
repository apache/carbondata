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

package org.apache.carbondata.spark.testsuite.dataload

import java.sql.Timestamp

import scala.collection.mutable

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test class of Adaptive Encoding UnSafe Column Page with Null values
 *
 */

class TestAdaptiveEncodingForNullValues
  extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS adaptive")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
        "true")
  }

  override def afterAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
        "true")
    sql("DROP TABLE IF EXISTS adaptive")
  }

  test("test INT with struct and array, Encoding INT-->BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:int,name:string,marks:array<int>>) " +
      "STORED AS carbondata")
    //sql("insert into adaptive values(1,'null\001abc\001null\002null\002null')")
    sql("insert into adaptive values(1,named_struct('id', 1, 'name', 'abc', 'marks', array(1,null,null)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(1, "abc", mutable.WrappedArray.make(Array(1, null, null))))))
  }


  test("test SMALLINT with struct and array SMALLINT --> BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:smallint,name:string," +
      "marks:array<smallint>>) STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 1, 'name', 'abc', 'marks', array(1,null,null)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(1, "abc", mutable.WrappedArray.make(Array(1, null, null))))))
  }


  test("test BigInt with struct and array BIGINT --> BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:bigint,name:string," +
      "marks:array<bigint>>) STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 1, 'name', 'abc', 'marks', array(1,null,null)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(1, "abc", mutable.WrappedArray.make(Array(1, null, null))))))
  }

  test("test Double with Struct and Array DOUBLE --> BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:double,name:string," +
      "marks:array<double>>) STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 1, 'name', 'abc', 'marks', array(1,null,null)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(1, "abc", mutable.WrappedArray.make(Array(1, null, null))))))
  }

  test("test Decimal with Struct") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:decimal(3,2),name:string," +
      "marks:array<decimal(3,2)>>) " +
      "STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 3.42, 'name', 'abc', 'marks', array(3.42,null,null)))")
    val decimal = new java.math.BigDecimal("3.42").setScale(2)
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(decimal, "abc", mutable.WrappedArray.make(Array(decimal, null, null))))))
  }

  // Same behaviour as Hive. Null value in complex data type is not supported.
  ignore("test Timestamp with Struct") {
    sql("Drop table if exists adaptive")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(
      "create table adaptive(roll int, student struct<id:timestamp,name:string>)" +
      "STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', null, 'name', 'abc'))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(null, "abc"))))
  }

  test("test Timestamp with Array") {
    sql("Drop table if exists adaptive")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(
      "create table adaptive(roll int, student struct<name:string," +
      "marks:array<timestamp>>) STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('name', 'abc', 'marks', array('2018-01-01 00:00:00', null, null)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row("abc", mutable.WrappedArray.make(Array(Timestamp.valueOf("2018-01-01 00:00:00.0"), null, null))))))
  }

  test("test DATE with Array") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<name:string," +
      "marks:array<date>>) STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('name', 'abc', 'marks', array('1992-02-19', null, null)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row("abc", mutable.WrappedArray.make(Array(java.sql.Date.valueOf("1992-02-19"), null, null))))))
  }

  test("test LONG with Array and Struct") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:long,name:string,marks:array<long>>) " +
      "STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 11, 'name', 'abc', 'marks', array(111, null, null)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(11, "abc", mutable.WrappedArray.make(Array(111, null, null))))))
  }

  test("test SHORT with Array and Struct") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:short,name:string,marks:array<short>>) " +
      "STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 11, 'name', 'abc', 'marks', array(11, null, null)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(11, "abc", mutable.WrappedArray.make(Array(11, null, null))))))
  }

  test("test Boolean with Struct and Array") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:boolean,name:string," +
      "marks:array<boolean>>) " +
      "STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', true, 'name', 'abc', 'marks', array(false, null, null)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(true, "abc", mutable.WrappedArray.make(Array(false, null, null))))))
  }
}
