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

import java.sql.{Date, Timestamp}

import scala.collection.mutable

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestArrayContainsPushDown extends QueryTest with BeforeAndAfterAll {

  override protected def afterAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("DROP TABLE IF EXISTS compactComplex")
  }

  test("test array contains pushdown for array of string") {
    sql("drop table if exists complex1")
    sql("create table complex1 (arr array<String>) stored as carbondata")
    sql("insert into complex1 select array('as') union all " +
        "select array('sd','df','gh') union all " +
        "select array('rt','ew','rtyu','jk',null) union all " +
        "select array('ghsf','dbv','','ty') union all " +
        "select array('hjsd','fggb','nhj','sd','asd')")

    checkExistence(sql(" explain select * from complex1 where array_contains(arr,'sd')"),
      true,
      "PushedFilters: [arr = 'sd']")

    checkExistence(sql(" explain select count(*) from complex1 where array_contains(arr,'sd')"),
      true,
      "PushedFilters: [arr = 'sd']")

    checkAnswer(sql(" select * from complex1 where array_contains(arr,'sd')"),
      Seq(Row(mutable.WrappedArray.make(Array("sd", "df", "gh"))),
        Row(mutable.WrappedArray.make(Array("hjsd", "fggb", "nhj", "sd", "asd")))))

    checkAnswer(sql(" select count(*) from complex1 where array_contains(arr,'sd')"), Seq(Row(2)))
    // test for empty
    checkAnswer(sql(" select * from complex1 where array_contains(arr,'')"),
      Seq(Row(mutable.WrappedArray.make(Array("ghsf", "dbv", "", "ty")))))

    sql("drop table complex1")
  }

  test("test array contains push down using limit " +
       "in row level scan when order by column is sort column") {
    sql("drop table if exists complex1")

    sql("create table complex1 (id int, arr array<String>) " +
        "stored as carbondata TBLPROPERTIES ('SORT_COLUMNS'='id')")
    sql("insert into complex1 select 1, array('as') union all " +
        "select 2, array('sd','df','gh') union all " +
        "select 3, array('rt','ew','rtyu','jk',null) union all " +
        "select 4, array('ghsf','dbv','','ty') union all " +
        "select 5, array('hjsd','fggb','nhj','sd','asd')")

    // enable the property
    CarbonProperties.getInstance()
      .addProperty("carbon.mapOrderPushDown.default_complex1.column", "id")
    // check for CarbonTakeOrderedAndProjectExec node in plan
    checkExistence(sql(
      " explain select * from complex1 where array_contains(arr,'sd') order by id limit 1"),
      true,
      "CarbonTakeOrderedAndProjectExec")
    CarbonProperties.getInstance().removeProperty("carbon.mapOrderPushDown.default_complex1.column")
    sql("drop table complex1")
  }

  test("test array contains pushdown for array of boolean") {
    sql("drop table if exists complex1")
    sql("create table complex1 (arr array<boolean>) stored as carbondata")
    sql("insert into complex1 select array(true) union all " +
        "select array(false, null, false) union all " +
        "select array(false, false, true, false) union all " +
        "select array(true, true, true, false) union all " +
        "select array(false)")

    checkExistence(sql(" explain select * from complex1 where array_contains(arr,true)"),
      true,
      "PushedFilters: [arr = true]")

    checkExistence(sql(" explain select count(*) from complex1 where array_contains(arr,true)"),
      true,
      "PushedFilters: [arr = true]")

    checkAnswer(sql(" select * from complex1 where array_contains(arr,true)"),
      Seq(Row(mutable.WrappedArray.make(Array(true))),
        Row(mutable.WrappedArray.make(Array(false, false, true, false))),
          Row(mutable.WrappedArray.make(Array(true, true, true, false)))))

    checkAnswer(sql(" select count(*) from complex1 where array_contains(arr,true)"), Seq(Row(3)))
    sql("drop table complex1")
  }

  test("test array contains pushdown for array of short") {
    sql("drop table if exists complex1")
    sql("create table complex1 (arr array<short>) stored as carbondata")
    sql("insert into complex1 select array(12) union all " +
        "select array(20, 30, 31000) union all " +
        "select array(11, 12, 13, 14, 15, 16, -31000) union all " +
        "select array(20, 31000, 60, 80) union all " +
        "select array(41, 41, -41, -42)")

    checkExistence(sql(" explain select * from complex1 where array_contains(arr,31000)"),
      true,
      "PushedFilters: [arr = 31000]")

    checkExistence(sql(" explain select count(*) from complex1 where array_contains(arr,31000)"),
      true,
      "PushedFilters: [arr = 31000]")

    checkAnswer(sql(" select * from complex1 where array_contains(arr,31000)"),
      Seq(Row(mutable.WrappedArray.make(Array(20, 30, 31000))),
        Row(mutable.WrappedArray.make(Array(20, 31000, 60, 80)))))

    checkAnswer(sql(" select count(*) from complex1 where array_contains(arr,31000)"), Seq(Row(2)))

    sql("drop table complex1")
  }

  test("test array contains pushdown for array of int") {
    sql("drop table if exists complex1")
    sql("create table complex1 (arr array<int>) stored as carbondata")
    sql("insert into complex1 select array(12) union all " +
        "select array(20, 30, 33000) union all " +
        "select array(11, 12, 13, 14, 15, 16, -33000) union all " +
        "select array(20, 33000, 60, 80) union all " +
        "select array(41, 41, -41, -42)")

    checkExistence(sql(" explain select * from complex1 where array_contains(arr,33000)"),
      true,
      "PushedFilters: [arr = 33000]")

    checkExistence(sql(" explain select count(*) from complex1 where array_contains(arr,33000)"),
      true,
      "PushedFilters: [arr = 33000]")

    checkAnswer(sql(" select * from complex1 where array_contains(arr,33000)"),
      Seq(Row(mutable.WrappedArray.make(Array(20, 30, 33000))),
        Row(mutable.WrappedArray.make(Array(20, 33000, 60, 80)))))

    checkAnswer(sql(" select count(*) from complex1 where array_contains(arr,33000)"), Seq(Row(2)))

    sql("drop table complex1")
  }

  test("test array contains pushdown for array of long") {
    sql("drop table if exists complex1")
    sql("create table complex1 (arr array<long>) stored as carbondata")
    sql("insert into complex1 select array(12) union all " +
        "select array(20, 30, 33000) union all " +
        "select array(11, 12, 13, 14, 15, 16, -33000) union all " +
        "select array(20, 33000, 60, 80) union all " +
        "select array(41, 41, -41, -42)")

    checkExistence(sql(" explain select * from complex1 where array_contains(arr,33000)"),
      true,
      "PushedFilters: [arr = 33000]")

    checkExistence(sql(" explain select count(*) from complex1 where array_contains(arr,33000)"),
      true,
      "PushedFilters: [arr = 33000]")

    checkAnswer(sql(" select * from complex1 where array_contains(arr,33000)"),
      Seq(Row(mutable.WrappedArray.make(Array(20, 30, 33000))),
        Row(mutable.WrappedArray.make(Array(20, 33000, 60, 80)))))

    checkAnswer(sql(" select count(*) from complex1 where array_contains(arr,33000)"), Seq(Row(2)))
    sql("drop table complex1")
  }

  test("test array contains pushdown for array of double") {
    sql("drop table if exists complex1")
    sql("create table complex1 (arr array<double>) stored as carbondata")
    sql("insert into complex1 select array(2.2) union all " +
        "select array(3.3, 4.4) union all " +
        "select array(3.3, 4.4, 2.2) union all " +
        "select array(-2.2, 3.3, 4.4)")

    checkExistence(
      sql(" explain select * from complex1 where array_contains(arr,cast(2.2 as double))"),
      true,
      "PushedFilters: [arr = 2.2]")

    checkExistence(
      sql(" explain select count(*) from complex1 where array_contains(arr,cast(2.2 as double))"),
      true,
      "PushedFilters: [arr = 2.2]")

    checkAnswer(sql(" select * from complex1 where array_contains(arr,cast(2.2 as double))"),
      Seq(Row(mutable.WrappedArray.make(Array(2.2))),
        Row(mutable.WrappedArray.make(Array(3.3, 4.4, 2.2)))))

    checkAnswer(sql(" select count(*) from complex1 where array_contains(arr,cast(2.2 as double))"),
      Seq(Row(2)))
    sql("drop table complex1")
  }

  test("test array contains pushdown for array of decimal") {
    sql("drop table if exists complex1")
    sql("create table complex1 (arr array<decimal(5,2)>) stored as carbondata")
    sql("insert into complex1 select array(2.2) union all " +
        "select array(3.3, 4.4) union all " +
        "select array(3.3, 4.4, 2.2) union all " +
        "select array(-2.2, 3.3, 4.4)")

    checkExistence(
      sql(" explain select * from complex1 where array_contains(arr,cast(2.2 as decimal(5,2)))"),
      true,
      "PushedFilters: [arr = 2.20]")

    checkExistence(
      sql("explain select count(*) from complex1 " +
          "where array_contains(arr,cast(2.2 as decimal(5,2)))"),
      true,
      "PushedFilters: [arr = 2.20]")

    checkAnswer(sql(" select * from complex1 where array_contains(arr,cast(2.2 as decimal(5,2)))"),
      Seq(Row(mutable.WrappedArray.make(Array(java.math.BigDecimal.valueOf(2.20).setScale(2)))),
        Row(mutable.WrappedArray.make(Array(
          java.math.BigDecimal.valueOf(3.30).setScale(2),
          java.math.BigDecimal.valueOf(4.40).setScale(2),
          java.math.BigDecimal.valueOf(2.20).setScale(2))))))

    checkAnswer(
      sql(" select count(*) from complex1 where array_contains(arr,cast(2.2 as decimal(5,2)))"),
      Seq(Row(2)))
    sql("drop table complex1")
  }

  test("test array contains pushdown for array of timestamp") {
    sql("drop table if exists complex1")
    sql("create table complex1 (arr array<timestamp>) stored as carbondata")
    sql("insert into complex1 " +
        "select array('2017-01-01 00:00:00','2018-01-01 00:00:00') union all " +
        "select array('2019-01-01 00:00:00') union all " +
        "select array('2018-01-01 00:00:00') ")
    checkExistence(
      sql("explain select * from complex1 " +
          "where array_contains(arr,cast('2018-01-01 00:00:00' as timestamp))"),
      true,
      "PushedFilters: [arr = '2018-01-01 00:00:00']")

    checkExistence(
      sql("explain select count(*) from complex1 " +
          "where array_contains(arr,cast('2018-01-01 00:00:00' as timestamp))"),
      true,
      "PushedFilters: [arr = '2018-01-01 00:00:00']")

    checkAnswer(
      sql("select * from complex1 " +
          "where array_contains(arr,cast('2018-01-01 00:00:00' as timestamp))"),
      Seq(Row(mutable.WrappedArray.make(Array(Timestamp.valueOf("2017-01-01 00:00:00.0"),
        Timestamp.valueOf("2018-01-01 00:00:00.0")))),
        Row(mutable.WrappedArray.make(Array(Timestamp.valueOf("2018-01-01 00:00:00.0"))))))

    checkAnswer(
      sql("select count(*) from complex1 " +
          "where array_contains(arr,cast('2018-01-01 00:00:00' as timestamp))"),
      Seq(Row(2)))
    sql("drop table complex1")
  }

  test("test array contains pushdown for array of date") {
    sql("drop table if exists complex1")
    sql("create table complex1 (arr array<date>) stored as carbondata")
    sql("insert into complex1 select array('2017-01-01','2018-01-01') union all " +
        "select array('2019-01-01') union all " +
        "select array('2018-01-01') ")

    checkExistence(
      sql("explain select * from complex1 where array_contains(arr,cast('2018-01-01' as date))"),
      true,
      "PushedFilters: [arr = '2018-01-01']")

    checkExistence(
      sql("explain select count(*) from complex1 " +
          "where array_contains(arr,cast('2018-01-01' as date))"),
      true,
      "PushedFilters: [arr = '2018-01-01']")

    checkAnswer(sql(" select * from complex1 where array_contains(arr,cast('2018-01-01' as date))"),
      Seq(Row(mutable.WrappedArray.make(
        Array(Date.valueOf("2017-01-01"), Date.valueOf("2018-01-01")))),
        Row(mutable.WrappedArray.make(Array(Date.valueOf("2018-01-01"))))))

    checkAnswer(
      sql("select count(*) from complex1 where array_contains(arr,cast('2018-01-01' as date))"),
      Seq(Row(2)))
    sql("drop table complex1")
  }
}
