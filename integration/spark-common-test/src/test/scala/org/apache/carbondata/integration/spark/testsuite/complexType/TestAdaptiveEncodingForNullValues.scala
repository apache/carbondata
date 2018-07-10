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
    sql("DROP TABLE IF EXISTS adaptive")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
        "true")
  }

  test("test INT with struct and array, Encoding INT-->BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:int,name:string,marks:array<int>>) " +
      "stored by 'carbondata'")
    sql("insert into adaptive values(1,'null$abc$null:null:null')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(null, "abc", mutable.WrappedArray.make(Array(null, null, null))))))
  }


  test("test SMALLINT with struct and array SMALLINT --> BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:smallint,name:string," +
      "marks:array<smallint>>) stored by 'carbondata'")
    sql("insert into adaptive values(1,'null$abc$null:null:null')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(null, "abc", mutable.WrappedArray.make(Array(null, null, null))))))
  }


  test("test BigInt with struct and array BIGINT --> BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:bigint,name:string," +
      "marks:array<bigint>>) stored by 'carbondata'")
    sql("insert into adaptive values(1,'null$abc$null:null:null')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(null, "abc", mutable.WrappedArray.make(Array(null, null, null))))))
  }

  test("test Double with Struct and Array DOUBLE --> BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:double,name:string," +
      "marks:array<double>>) stored by 'carbondata'")
    sql("insert into adaptive values(1,'null$abc$null:null:null')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(null, "abc", mutable.WrappedArray.make(Array(null, null, null))))))
  }

  test("test Decimal with Struct") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:decimal(3,2),name:string," +
      "marks:array<decimal>>) stored by " +
      "'carbondata'")
    sql("insert into adaptive values(1,'null$abc$null:null:null')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(null, "abc", mutable.WrappedArray.make(Array(null, null, null))))))
  }

  test("test Timestamp with Struct") {
    sql("Drop table if exists adaptive")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(
      "create table adaptive(roll int, student struct<id:timestamp,name:string>) stored by " +
      "'carbondata'")
    sql("insert into adaptive values(1,'null$abc')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(null, "abc"))))
  }

  test("test Timestamp with Array") {
    sql("Drop table if exists adaptive")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(
      "create table adaptive(roll int, student struct<name:string," +
      "marks:array<timestamp>>) stored by 'carbondata'")
    sql("insert into adaptive values(1,'abc$null:null:null')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row("abc", mutable.WrappedArray.make(Array(null, null, null))))))
  }

  test("test DATE with Array") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<name:string," +
      "marks:array<date>>) stored by 'carbondata'")
    sql("insert into adaptive values(1,'abc$null:null:null')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row("abc", mutable.WrappedArray.make(Array(null, null, null))))))
  }

  test("test LONG with Array and Struct") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:long,name:string,marks:array<long>>) " +
      "stored by 'carbondata'")
    sql("insert into adaptive values(1,'null$abc$null:null:null')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(null, "abc", mutable.WrappedArray.make(Array(null, null, null))))))
  }

  test("test SHORT with Array and Struct") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:short,name:string,marks:array<short>>) " +
      "stored by 'carbondata'")
    sql("insert into adaptive values(1,'null$abc$null:null:null')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(null, "abc", mutable.WrappedArray.make(Array(null, null, null))))))
  }

  test("test Boolean with Struct and Array") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:boolean,name:string," +
      "marks:array<boolean>>) " +
      "stored by 'carbondata'")
    sql("insert into adaptive values(1,'null$abc$null:null:null')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(null, "abc", mutable.WrappedArray.make(Array(null, null, null))))))
  }
}
