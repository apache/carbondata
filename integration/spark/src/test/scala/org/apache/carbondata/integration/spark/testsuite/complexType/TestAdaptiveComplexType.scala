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

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

trait TestAdaptiveComplexType extends QueryTest with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }

  test("test INT with struct and array, Encoding INT-->BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:int,name:string,marks:array<int>>) " +
      "STORED AS carbondata")
    sql(
      s"load data inpath '$resourcesPath/adap.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(20, 30, 40)))),
        Row(2, Row(600, "abc", mutable.WrappedArray.make(Array(20, 30, 40)))),
        Row(3, Row(600, "abc", mutable.WrappedArray.make(Array(20, 30, 40))))))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:int,name:string,marks:array<int>>) " +
      "STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 500, 'name', 'abc', 'marks', array(20,30,40)))")
    sql("insert into adaptive values(2,named_struct('id', 600, 'name', 'abc', 'marks', array(20,30,40)))")
    sql("insert into adaptive values(3,named_struct('id', 600, 'name', 'abc', 'marks', array(20,30,40)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(20, 30, 40)))),
        Row(2, Row(600, "abc", mutable.WrappedArray.make(Array(20, 30, 40)))),
        Row(3, Row(600, "abc", mutable.WrappedArray.make(Array(20, 30, 40))))))
  }

  test("test INT with struct and array, Encoding INT-->SHORT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:int,name:string,marks:array<int>>) " +
      "STORED AS carbondata")
    sql(
      s"load data inpath '$resourcesPath/adap_int1.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(700, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(3, Row(800, "abc", mutable.WrappedArray.make(Array(200, 300, 400))))))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:int,name:string,marks:array<int>>) " +
      "STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 500, 'name', 'abc', 'marks', array(200,300,400)))")
    sql("insert into adaptive values(2,named_struct('id', 700, 'name', 'abc', 'marks', array(200,300,400)))")
    sql("insert into adaptive values(3,named_struct('id', 800, 'name', 'abc', 'marks', array(200,300,400)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(700, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(3, Row(800, "abc", mutable.WrappedArray.make(Array(200, 300, 400))))))
  }

  test("test INT with struct and array, Encoding INT-->SHORT INT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:int,name:string,marks:array<int>>) " +
      "STORED AS carbondata")
    sql(
      s"load data inpath '$resourcesPath/adap_int2.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(50000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(2, Row(70000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(3, Row(100000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000))))))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:int,name:string,marks:array<int>>) " +
      "STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 50000, 'name', 'abc', 'marks', array(2000000,3000000,4000000)))")
    sql("insert into adaptive values(2,named_struct('id', 70000, 'name', 'abc', 'marks', array(2000000,3000000,4000000)))")
    sql("insert into adaptive values(3,named_struct('id', 100000, 'name', 'abc', 'marks', array(2000000,3000000,4000000)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(50000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(2, Row(70000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(3, Row(100000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000))))))
  }

  test("test INT with struct and array, Encoding INT-->INT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:int,name:string,marks:array<int>>) " +
      "STORED AS carbondata")
    sql(
      s"load data inpath '$resourcesPath/adap_int3.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(2, Row(7000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(3, Row(10000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000))))))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:int,name:string,marks:array<int>>) " +
      "STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 500000, 'name', 'abc', 'marks', array(200,300,52000000)))")
    sql("insert into adaptive values(2,named_struct('id', 700000, 'name', 'abc', 'marks', array(200,300,52000000)))")
    sql("insert into adaptive values(3,named_struct('id', 10000000, 'name', 'abc', 'marks', array(200,300,52000000)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(2, Row(700000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(3, Row(10000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000))))))
  }


  test("test SMALLINT with struct and array SMALLINT --> BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:smallint,name:string," +
      "marks:array<smallint>>) STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 100, 'name', 'abc', 'marks', array(20,30,40)))")
    sql("insert into adaptive values(2,named_struct('id', 200, 'name', 'abc', 'marks', array(30,40,50)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(100, "abc", mutable.WrappedArray.make(Array(20, 30, 40)))),
        Row(2, Row(200, "abc", mutable.WrappedArray.make(Array(30, 40, 50))))))
  }

  test("test SMALLINT with struct and array SMALLINT --> SHORT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:smallint,name:string," +
      "marks:array<smallint>>) STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 500, 'name', 'abc', 'marks', array(200,300,400)))")
    sql("insert into adaptive values(2,named_struct('id', 8000, 'name', 'abc', 'marks', array(300,400,500)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(8000, "abc", mutable.WrappedArray.make(Array(300, 400, 500))))))
  }

  test("test BigInt with struct and array BIGINT --> BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:bigint,name:string," +
      "marks:array<bigint>>) STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 1, 'name', 'abc', 'marks', array(20,30,40)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(1, "abc", mutable.WrappedArray.make(Array(20, 30, 40))))))
  }

  test("test BigInt with struct and array BIGINT --> SHORT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:bigint,name:string," +
      "marks:array<bigint>>) STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 500, 'name', 'abc', 'marks', array(200,300,400)))")
    sql("insert into adaptive values(2,named_struct('id', 8000, 'name', 'abc', 'marks', array(300,400,500)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(8000, "abc", mutable.WrappedArray.make(Array(300, 400, 500))))))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:BIGINT,name:string,marks:array<BIGINT>>)" +
      " " +
      "STORED AS carbondata")
    sql(
      s"load data inpath '$resourcesPath/adap_int1.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(700, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(3, Row(800, "abc", mutable.WrappedArray.make(Array(200, 300, 400))))))
  }

  test("test BigInt with struct and array BIGINT --> SHORT INT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:bigint,name:string," +
      "marks:array<bigint>>) STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 50000, 'name', 'abc', 'marks', array(2000000,3000000,4000000)))")
    sql("insert into adaptive values(2,named_struct('id', 70000, 'name', 'abc', 'marks', array(2000000,3000000,4000000)))")
    sql("insert into adaptive values(3,named_struct('id', 100000, 'name', 'abc', 'marks', array(2000000,3000000,4000000)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(50000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(2, Row(70000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(3, Row(100000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000))))))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:BIGINT,name:string,marks:array<BIGINT>>)" +
      " " +
      "STORED AS carbondata")
    sql(
      s"load data inpath '$resourcesPath/adap_int2.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(50000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(2, Row(70000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(3, Row(100000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000))))))
  }

  test("test BIGINT with struct and array, Encoding INT-->INT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:BIGINT,name:string,marks:array<BIGINT>>)" +
      " " +
      "STORED AS carbondata")
    sql(
      s"load data inpath '$resourcesPath/adap_int3.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(2, Row(7000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(3, Row(10000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000))))))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:BIGINT,name:string,marks:array<BIGINT>>)" +
      " " +
      "STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 500000, 'name', 'abc', 'marks', array(200,300,52000000)))")
    sql("insert into adaptive values(2,named_struct('id', 700000, 'name', 'abc', 'marks', array(200,300,52000000)))")
    sql("insert into adaptive values(3,named_struct('id', 10000000, 'name', 'abc', 'marks', array(200,300,52000000)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(2, Row(700000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(3, Row(10000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000))))))
  }

  test("test Double with Struct and Array DOUBLE --> BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:double,name:string," +
      "marks:array<double>>) STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 1.323, 'name', 'abc', 'marks', array(2.2,3.3,4.4)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(2.2, 3.3, 4.4))))))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:double,name:string,marks:array<double>>)" +
      " " +
      "STORED AS carbondata")
    sql(
      s"load data inpath '$resourcesPath/adap_double1.csv' into table adaptive options('delimiter'='," +
      "'," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(2.2, 3.3, 4.4)))),
        Row(2, Row(1.323, "abc", mutable.WrappedArray.make(Array(2.2, 3.3, 4.4)))),
        Row(3, Row(1.323, "abc", mutable.WrappedArray.make(Array(2.2, 3.3, 4.4))))))
  }

  test("test Double with Struct and Array DOUBLE --> SHORT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:double,name:string," +
      "marks:array<double>>) STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 1.323, 'name', 'abc', 'marks', array(20.2,30.3,40.4)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 40.4))))))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:double,name:string,marks:array<double>>)" +
      " " +
      "STORED AS carbondata")
    sql(
      s"load data inpath '$resourcesPath/adap_double2.csv' into table adaptive options('delimiter'='," +
      "'," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 40.4)))),
        Row(2, Row(2.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 40.4)))),
        Row(3, Row(4.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 40.4))))))
  }

  test("test Double with Struct and Array DOUBLE --> SHORT INT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:double,name:string," +
      "marks:array<double>>) STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 10.323, 'name', 'abc', 'marks', array(20.2,30.3,500.423)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(10.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 500.423))))))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:double,name:string,marks:array<double>>)" +
      " " +
      "STORED AS carbondata")
    sql(
      s"load data inpath '$resourcesPath/adap_double3.csv' into table adaptive options('delimiter'='," +
      "'," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 500.423)))),
        Row(2, Row(2.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 500.423)))),
        Row(3, Row(50.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 500.423))))))
  }

  test("test Double with Struct and Array DOUBLE --> INT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:double,name:string," +
      "marks:array<double>>) STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 1000.323, 'name', 'abc', 'marks', array(20.2,30.3,50000.423)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(1000.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 50000.423))))))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:double,name:string,marks:array<double>>)" +
      " " +
      "STORED AS carbondata")
    sql(
      s"load data inpath '$resourcesPath/adap_double4.csv' into table adaptive options('delimiter'='," +
      "'," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 50000.423)))),
        Row(2, Row(2.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 50000.423)))),
        Row(3, Row(50000.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 50000.423))))))
  }

  test("test Double with Struct and Array DOUBLE --> DOUBLE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:double,name:string," +
      "marks:array<double>>) STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 1.797693134862315, 'name', 'abc', 'marks', array(2.2,30.3,1.797693134862315)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1,
        Row(1.797693134862315,
          "abc",
          mutable.WrappedArray.make(Array(2.2, 30.3, 1.797693134862315))))))

  }

  test("test Decimal with Struct") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:decimal(3,2),name:string>) " +
      "STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 3.2, 'name', 'abc'))")
    sql("select * from adaptive").show(false)
  }

  test("test Decimal with Array") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<name:string," +
      "marks:array<decimal>>) STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('name', 'abc', 'marks', array(20.2,30.3,40.4)))")
    sql("select * from adaptive").show(false)
  }

  test("test Timestamp with Struct") {
    sql("Drop table if exists adaptive")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(
      "create table adaptive(roll int, student struct<id:timestamp,name:string>) " +
      "STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', '2017-01-01 00:00:00', 'name', 'abc'))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(Timestamp.valueOf("2017-01-01 00:00:00.0"), "abc"))))
  }

  test("test Timestamp with Array") {
    sql("Drop table if exists adaptive")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(
      "create table adaptive(roll int, student struct<name:string," +
      "marks:array<timestamp>>) STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('name', 'abc', 'marks', array('2017-01-01 00:00:00','2018-01-01 00:00:00')))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1,
        Row("abc",
          mutable.WrappedArray
            .make(Array(Timestamp.valueOf("2017-01-01 00:00:00.0"),
              Timestamp.valueOf("2018-01-01 00:00:00.0")))))))
  }

  test("test DATE with Array") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<name:string," +
      "marks:array<date>>) STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('name', 'abc', 'marks', array('2017/01/01')))")
    sql("select * from adaptive").show(false)
  }

  test("test LONG with Array and Struct Encoding LONG --> BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:long,name:string,marks:array<long>>) " +
      "STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 11111, 'name', 'abc', 'marks', array(20,30,40)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(11111, "abc", mutable.WrappedArray.make(Array(20, 30, 40))))))
  }

  test("test LONG with Array and Struct Encoding LONG --> SHORT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:long,name:string,marks:array<long>>) " +
      "STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 11111, 'name', 'abc', 'marks', array(200,300,400)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(11111, "abc", mutable.WrappedArray.make(Array(200, 300, 400))))))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:LONG,name:string,marks:array<LONG>>) " +
      "STORED AS carbondata")
    sql(
      s"load data inpath '$resourcesPath/adap_int1.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(700, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(3, Row(800, "abc", mutable.WrappedArray.make(Array(200, 300, 400))))))
  }

  test("test LONG with struct and array, Encoding LONG-->SHORT INT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:LONG,name:string,marks:array<LONG>>) " +
      "STORED AS carbondata")
    sql(
      s"load data inpath '$resourcesPath/adap_int2.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(50000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(2, Row(70000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(3, Row(100000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000))))))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:LONG,name:string,marks:array<LONG>>) " +
      "STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 50000, 'name', 'abc', 'marks', array(2000000,3000000,4000000)))")
    sql("insert into adaptive values(2,named_struct('id', 70000, 'name', 'abc', 'marks', array(2000000,3000000,4000000)))")
    sql("insert into adaptive values(3,named_struct('id', 100000, 'name', 'abc', 'marks', array(2000000,3000000,4000000)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(50000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(2, Row(70000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(3, Row(100000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000))))))
  }

  test("test LONG with struct and array, Encoding LONG-->INT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:LONG,name:string,marks:array<LONG>>) " +
      "STORED AS carbondata")
    sql(
      s"load data inpath '$resourcesPath/adap_int3.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(2, Row(7000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(3, Row(10000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000))))))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:LONG,name:string,marks:array<LONG>>) " +
      "STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 500000, 'name', 'abc', 'marks', array(200, 300, 52000000)))")
    sql("insert into adaptive values(2,named_struct('id', 700000, 'name', 'abc', 'marks', array(200, 300, 52000000)))")
    sql("insert into adaptive values(3,named_struct('id', 10000000,'name', 'abc', 'marks', array(200, 300, 52000000)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(2, Row(700000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(3, Row(10000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000))))))
  }

  test("test LONG with struct and array, Encoding LONG-->LONG") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:LONG,name:string,marks:array<LONG>>) " +
      "STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 500000, 'name', 'abc', 'marks', array(200, 300, 52000000000)))")
    sql("insert into adaptive values(2,named_struct('id', 700000, 'name', 'abc', 'marks', array(200, 300, 52000000000)))")
    sql("insert into adaptive values(3,named_struct('id', 10000000,'name', 'abc', 'marks', array(200, 300, 52000000000)))")
    sql("select * from adaptive").show(false)
  }

  test("test SHORT with Array and Struct Encoding SHORT -->BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:short,name:string,marks:array<short>>) " +
      "STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 11, 'name', 'abc', 'marks', array(20, 30, 40)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(11, "abc", mutable.WrappedArray.make(Array(20, 30, 40))))))
  }

  test("test SHORT with Array and Struct Encoding SHORT --> SHORT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:SHORT,name:string,marks:array<SHORT>>) " +
      "STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', 11111, 'name', 'abc', 'marks', array(200, 300, 400)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(11111, "abc", mutable.WrappedArray.make(Array(200, 300, 400))))))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:SHORT,name:string,marks:array<SHORT>>) " +
      "STORED AS carbondata")
    sql(
      s"load data inpath '$resourcesPath/adap_int1.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(700, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(3, Row(800, "abc", mutable.WrappedArray.make(Array(200, 300, 400))))))
  }

  test("test Boolean with Struct and Array") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:boolean,name:string," +
      "marks:array<boolean>>) " +
      "STORED AS carbondata")
    sql("insert into adaptive values(1,named_struct('id', true, 'name', 'abc', 'marks', array(false, true, false)))")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(true, "abc", mutable.WrappedArray.make(Array(false, true, false))))))
  }

  test("test Double with large decimalcount") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(array1 struct<double1:double,double2:double,double3:double>) " +
      "STORED AS carbondata")
    sql("insert into adaptive values(named_struct('double1', 10000000.0, 'double2', 300000.0, 'double3', 3000.0))")
    sql("select * from adaptive").show(false)
    checkExistence(sql("select * from adaptive"), true, "1.0E7,300000.0,3000.0")
    sql("Drop table if exists adaptive")
    sql("create table adaptive(struct_arr struct<array_db1:array<double>>) STORED AS carbondata")
    sql("insert into adaptive values(named_struct('array_db1', array(5555555.9559, 12345678991234567, 3444.999)))")
    checkExistence(sql("select * from adaptive"),
      true,
      "5555555.9559, 1.2345678991234568E16, 3444.999")
  }

}
