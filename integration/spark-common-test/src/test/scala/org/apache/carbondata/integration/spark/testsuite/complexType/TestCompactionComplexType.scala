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

class TestCompactionComplexType extends QueryTest with BeforeAndAfterAll {

  private val compactionThreshold = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
      CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)

  override protected def beforeAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2,3")
  }

  override protected def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS compactComplex")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, compactionThreshold)
  }

  test("test INT with struct and array, Encoding INT-->BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:int,name:string,marks:array<int>>) " +
      "stored by 'carbondata'")
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
      "stored by 'carbondata'")
    sql("insert into adaptive values(1,'500\001abc\00120\00230\00240')")
    sql("insert into adaptive values(2,'600\001abc\00130\00230\00240')")
    sql("insert into adaptive values(3,'700\001abc\00140\00230\00240')")
    sql("insert into adaptive values(4,'800\001abc\00150\00230\00240')")
    sql("alter table adaptive compact 'major'").show(200,false)
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(20, 30, 40)))),
        Row(2, Row(600, "abc", mutable.WrappedArray.make(Array(30, 30, 40)))),
        Row(3, Row(700, "abc", mutable.WrappedArray.make(Array(40, 30, 40)))),
        Row(4, Row(800, "abc", mutable.WrappedArray.make(Array(50, 30, 40))))))
  }

  test("test INT with struct and array, Encoding INT-->SHORT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:int,name:string,marks:array<int>>) " +
      "stored by 'carbondata'")
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
      "stored by 'carbondata'")
    sql("insert into adaptive values(1,'500\001abc\001200\002300\002400')")
    sql("insert into adaptive values(2,'600\001abc\001300\002300\002400')")
    sql("insert into adaptive values(3,'700\001abc\001400\002300\002400')")
    sql("insert into adaptive values(4,'800\001abc\001500\002300\002400')")
    sql("alter table adaptive compact 'major'").show(200,false)
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(600, "abc", mutable.WrappedArray.make(Array(300, 300, 400)))),
        Row(3, Row(700, "abc", mutable.WrappedArray.make(Array(400, 300, 400)))),
        Row(4, Row(800, "abc", mutable.WrappedArray.make(Array(500, 300, 400))))))
  }

  test("test INT with struct and array, Encoding INT-->SHORT INT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:int,name:string,marks:array<int>>) " +
      "stored by 'carbondata'")
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
      "stored by 'carbondata'")
    sql("insert into adaptive values(1,'50000\001abc\0012000000\0023000000\0024000000')")
    sql("insert into adaptive values(2,'70000\001abc\0012000000\0024000000\0024000000')")
    sql("insert into adaptive values(3,'100000\001abc\0012000000\0025000000\0024000000')")
    sql("insert into adaptive values(4,'200000\001abc\0012000000\0026000000\0024000000')")
    sql("alter table adaptive compact 'major'").show(200,false)
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(50000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(2, Row(70000, "abc", mutable.WrappedArray.make(Array(2000000, 4000000, 4000000)))),
        Row(3, Row(100000, "abc", mutable.WrappedArray.make(Array(2000000, 5000000, 4000000)))),
        Row(4, Row(200000, "abc", mutable.WrappedArray.make(Array(2000000, 6000000, 4000000))))))
  }

  test("test INT with struct and array, Encoding INT-->INT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:int,name:string,marks:array<int>>) " +
      "stored by 'carbondata'")
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
      "stored by 'carbondata'")
    sql("insert into adaptive values(1,'500000\001abc\001200\002300\00252000000')")
    sql("insert into adaptive values(2,'700000\001abc\001210\002350\00252000000')")
    sql("insert into adaptive values(3,'10000000\001abc\001200\002300\00252000000')")
    sql("insert into adaptive values(4,'10000001\001abd\001250\002450\00262000000')")
    sql("alter table adaptive compact 'major'").show(200,false)
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(2, Row(700000, "abc", mutable.WrappedArray.make(Array(210, 350, 52000000)))),
        Row(3, Row(10000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(4, Row(10000001, "abd", mutable.WrappedArray.make(Array(250, 450, 62000000))))))
  }


  test("test SMALLINT with struct and array SMALLINT --> BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:smallint,name:string," +
      "marks:array<smallint>>) stored by 'carbondata'")
    sql("insert into adaptive values(1,'100\001abc\00120\00230\00240')")
    sql("insert into adaptive values(2,'200\001abc\00130\00240\00250')")
    sql("insert into adaptive values(3,'300\001abd\00130\00241\00255')")
    sql("insert into adaptive values(4,'400\001abe\00130\00242\00256')")
    sql("alter table adaptive compact 'major'").show(200,false)
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(100, "abc", mutable.WrappedArray.make(Array(20, 30, 40)))),
        Row(2, Row(200, "abc", mutable.WrappedArray.make(Array(30, 40, 50)))),
        Row(3, Row(300, "abd", mutable.WrappedArray.make(Array(30, 41, 55)))),
        Row(4, Row(400, "abe", mutable.WrappedArray.make(Array(30, 42, 56))))))
  }

  test("test SMALLINT with struct and array SMALLINT --> SHORT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:smallint,name:string," +
      "marks:array<smallint>>) stored by 'carbondata'")
    sql("insert into adaptive values(1,'500\001abc\001200\002300\002400')")
    sql("insert into adaptive values(2,'8000\001abc\001300\002410\002500')")
    sql("insert into adaptive values(3,'9000\001abee\001310\002420\002400')")
    sql("insert into adaptive values(4,'9900\001abfffffffffffffff\001320\002430\002500')")
    sql("alter table adaptive compact 'major'").show(200,false)
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(8000, "abc", mutable.WrappedArray.make(Array(300, 410, 500)))),
        Row(3, Row(9000, "abee", mutable.WrappedArray.make(Array(310, 420, 400)))),
        Row(4, Row(9900, "abfffffffffffffff", mutable.WrappedArray.make(Array(320, 430, 500))))))
    sql("insert into adaptive values(5,'500\001abc\001200\002310\002400')")
    sql("insert into adaptive values(6,'8000\001abc\001300\002310\002500')")
    sql("insert into adaptive values(7,'9000\001abee\001310\002320\002400')")
    sql("insert into adaptive values(8,'9900\001abfffffffffffffffeeee\001320\002330\002500')")
    sql("alter table adaptive compact 'major'").show(200,false)
    sql("SHOW SEGMENTS FOR TABLE adaptive").show(200,false)
    sql("clean files for table adaptive").show(200,false)
    sql("SHOW SEGMENTS FOR TABLE adaptive").show(200,false)
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(8000, "abc", mutable.WrappedArray.make(Array(300, 410, 500)))),
        Row(3, Row(9000, "abee", mutable.WrappedArray.make(Array(310, 420, 400)))),
        Row(4, Row(9900, "abfffffffffffffff", mutable.WrappedArray.make(Array(320, 430, 500)))),
      Row(5, Row(500, "abc", mutable.WrappedArray.make(Array(200, 310, 400)))),
      Row(6, Row(8000, "abc", mutable.WrappedArray.make(Array(300, 310, 500)))),
      Row(7, Row(9000, "abee", mutable.WrappedArray.make(Array(310, 320, 400)))),
      Row(8, Row(9900, "abfffffffffffffffeeee", mutable.WrappedArray.make(Array(320, 330, 500))))))
  }

  test("test BigInt with struct and array BIGINT --> BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:bigint,name:string," +
      "marks:array<bigint>>) stored by 'carbondata'")
    sql("insert into adaptive values(11,'1\001abc\00121\00230\00240')")
    sql("insert into adaptive values(12,'1\001ab1\00122\00230\00240')")
    sql("insert into adaptive values(13,'1\001ab2\00123\00230\00240')")
    sql("insert into adaptive values(14,'1\001ab3\00124\00230\00240')")
    sql("insert into adaptive values(15,'1\001ab4\00125\00230\00240')")
    sql("insert into adaptive values(16,'1\001ab5\00126\00230\00240')")
    sql("insert into adaptive values(17,'1\001ab6\00127\00230\00240')")
    sql("insert into adaptive values(18,'1\001ab7\00128\00230\00240')")
    sql("insert into adaptive values(19,'1\001ab8\00129\00230\00240')")
    sql("insert into adaptive values(20,'1\001ab9\00130\00230\00240')")
    sql("insert into adaptive values(21,'1\001ab10\00131\00230\00240')")
    sql("insert into adaptive values(22,'1\001ab11\00132\00230\00240')")
    sql("alter table adaptive compact 'major'").show(200,false)
    sql("SHOW SEGMENTS FOR TABLE adaptive").show(200,false)
    sql("clean files for table adaptive").show(200,false)
    sql("SHOW SEGMENTS FOR TABLE adaptive").show(200,false)

    checkAnswer(sql("select * from adaptive"),
      Seq(Row(11, Row(1, "abc", mutable.WrappedArray.make(Array(21, 30, 40)))),
        Row(12, Row(1, "ab1", mutable.WrappedArray.make(Array(22, 30, 40)))),
        Row(13, Row(1, "ab2", mutable.WrappedArray.make(Array(23, 30, 40)))),
        Row(14, Row(1, "ab3", mutable.WrappedArray.make(Array(24, 30, 40)))),
        Row(15, Row(1, "ab4", mutable.WrappedArray.make(Array(25, 30, 40)))),
        Row(16, Row(1, "ab5", mutable.WrappedArray.make(Array(26, 30, 40)))),
        Row(17, Row(1, "ab6", mutable.WrappedArray.make(Array(27, 30, 40)))),
        Row(18, Row(1, "ab7", mutable.WrappedArray.make(Array(28, 30, 40)))),
        Row(19, Row(1, "ab8", mutable.WrappedArray.make(Array(29, 30, 40)))),
        Row(20, Row(1, "ab9", mutable.WrappedArray.make(Array(30, 30, 40)))),
        Row(21, Row(1, "ab10", mutable.WrappedArray.make(Array(31, 30, 40)))),
        Row(22, Row(1, "ab11", mutable.WrappedArray.make(Array(32, 30, 40))))
      ))
  }

  test("test BigInt with struct and array BIGINT --> SHORT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:bigint,name:string," +
      "marks:array<bigint>>) stored by 'carbondata'")
    sql("insert into adaptive values(1,'500\001abc\001200\002300\002400')")
    sql("insert into adaptive values(2,'8000\001abc\001300\002400\002500')")
    sql("insert into adaptive values(3,'9000\001abc\001300\002400\002500')")
    sql("insert into adaptive values(4,'10000\001abc\001300\002400\002500')")
    sql("alter table adaptive compact'major'")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(8000, "abc", mutable.WrappedArray.make(Array(300, 400, 500)))),
        Row(3, Row(9000, "abc", mutable.WrappedArray.make(Array(300, 400, 500)))),
        Row(4, Row(10000, "abc", mutable.WrappedArray.make(Array(300, 400, 500))))))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:BIGINT,name:string,marks:array<BIGINT>>)" +
      " " +
      "stored by 'carbondata'")
    sql(
      s"load data inpath '$resourcesPath/adap_int1.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_int1.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_int1.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_int1.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql("alter table adaptive compact'major'")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(700, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(3, Row(800, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(700, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(3, Row(800, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(700, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(3, Row(800, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(700, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(3, Row(800, "abc", mutable.WrappedArray.make(Array(200, 300, 400))))
      ))
  }

  test("test BigInt with struct and array BIGINT --> SHORT INT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:bigint,name:string," +
      "marks:array<bigint>>) stored by 'carbondata'")
    sql("insert into adaptive values(1,'50000\001abc\0012000000\0023000000\0024000000')")
    sql("insert into adaptive values(2,'70000\001abc\0012000000\0023000000\0024000000')")
    sql("insert into adaptive values(3,'100000\001abc\0012000000\0023000000\0024000000')")
    sql("insert into adaptive values(1,'50000\001abc\0012000000\0023000000\0024000000')")
    sql("insert into adaptive values(2,'70000\001abc\0012000000\0023000000\0024000000')")
    sql("insert into adaptive values(3,'100000\001abc\0012000000\0023000000\0024000000')")
    sql("insert into adaptive values(1,'50000\001abc\0012000000\0023000000\0024000000')")
    sql("insert into adaptive values(2,'70000\001abc\0012000000\0023000000\0024000000')")
    sql("insert into adaptive values(3,'100000\001abc\0012000000\0023000000\0024000000')")
    sql("insert into adaptive values(1,'50000\001abc\0012000000\0023000000\0024000000')")
    sql("insert into adaptive values(2,'70000\001abc\0012000000\0023000000\0024000000')")
    sql("insert into adaptive values(3,'100000\001abc\0012000000\0023000000\0024000000')")
    sql("alter table adaptive compact'major'")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(50000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(2, Row(70000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(3, Row(100000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(1, Row(50000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(2, Row(70000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(3, Row(100000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(1, Row(50000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(2, Row(70000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(3, Row(100000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(1, Row(50000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(2, Row(70000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(3, Row(100000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000))))
      ))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:BIGINT,name:string,marks:array<BIGINT>>)" +
      " " +
      "stored by 'carbondata'")
    sql(
      s"load data inpath '$resourcesPath/adap_int2.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_int2.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_int2.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_int2.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql("alter table adaptive compact'major'")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(50000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(2, Row(70000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(3, Row(100000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(1, Row(50000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(2, Row(70000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(3, Row(100000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(1, Row(50000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(2, Row(70000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(3, Row(100000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(1, Row(50000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(2, Row(70000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(3, Row(100000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000))))
      ))
  }

  test("test BIGINT with struct and array, Encoding INT-->INT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:BIGINT,name:string,marks:array<BIGINT>>)" +
      " " +
      "stored by 'carbondata'")
    sql(
      s"load data inpath '$resourcesPath/adap_int3.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_int3.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_int3.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_int3.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql("alter table adaptive compact'major'")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(2, Row(7000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(3, Row(10000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(1, Row(500000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(2, Row(7000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(3, Row(10000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(1, Row(500000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(2, Row(7000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(3, Row(10000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(1, Row(500000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(2, Row(7000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(3, Row(10000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000))))
      ))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:BIGINT,name:string,marks:array<BIGINT>>)" +
      " " +
      "stored by 'carbondata'")
    sql("insert into adaptive values(1,'500000\001abc\001200\002300\00252000000')")
    sql("insert into adaptive values(2,'700000\001abc\001200\002300\00252000000')")
    sql("insert into adaptive values(3,'10000000\001abc\001200\002300\00252000000')")
    sql("insert into adaptive values(1,'500000\001abc\001200\002300\00252000000')")
    sql("insert into adaptive values(2,'700000\001abc\001200\002300\00252000000')")
    sql("insert into adaptive values(3,'10000000\001abc\001200\002300\00252000000')")
    sql("insert into adaptive values(1,'500000\001abc\001200\002300\00252000000')")
    sql("insert into adaptive values(2,'700000\001abc\001200\002300\00252000000')")
    sql("insert into adaptive values(3,'10000000\001abc\001200\002300\00252000000')")
    sql("insert into adaptive values(1,'500000\001abc\001200\002300\00252000000')")
    sql("insert into adaptive values(2,'700000\001abc\001200\002300\00252000000')")
    sql("insert into adaptive values(3,'10000000\001abc\001200\002300\00252000000')")
    sql("alter table adaptive compact 'major' ")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(2, Row(700000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(3, Row(10000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(1, Row(500000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(2, Row(700000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(3, Row(10000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(1, Row(500000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(2, Row(700000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(3, Row(10000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(1, Row(500000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(2, Row(700000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(3, Row(10000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000))))
      ))
  }

  test("test Double with Struct and Array DOUBLE --> BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:double,name:string," +
      "marks:array<double>>) stored by 'carbondata'")
    sql("insert into adaptive values(1,'1.323\001abc\0012.2\0023.3\0024.4')")
    sql("insert into adaptive values(2,'1.324\001abc\0012.2\0023.3\0024.4')")
    sql("insert into adaptive values(3,'1.325\001abc\0012.2\0023.3\0024.4')")
    sql("insert into adaptive values(4,'1.326\001abc\0012.2\0023.3\0024.4')")
    sql("alter table adaptive compact 'major' ")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(2.2, 3.3, 4.4)))),
        Row(2, Row(1.324, "abc", mutable.WrappedArray.make(Array(2.2, 3.3, 4.4)))),
        Row(3, Row(1.325, "abc", mutable.WrappedArray.make(Array(2.2, 3.3, 4.4)))),
        Row(4, Row(1.326, "abc", mutable.WrappedArray.make(Array(2.2, 3.3, 4.4))))))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:double,name:string,marks:array<double>>)" +
      " " +
      "stored by 'carbondata'")
    sql(
      s"load data inpath '$resourcesPath/adap_double1.csv' into table adaptive options('delimiter'='," +
      "'," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_double1.csv' into table adaptive options('delimiter'='," +
      "'," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_double1.csv' into table adaptive options('delimiter'='," +
      "'," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_double1.csv' into table adaptive options('delimiter'='," +
      "'," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql("alter table adaptive compact 'major' ")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(2.2, 3.3, 4.4)))),
        Row(2, Row(1.323, "abc", mutable.WrappedArray.make(Array(2.2, 3.3, 4.4)))),
        Row(3, Row(1.323, "abc", mutable.WrappedArray.make(Array(2.2, 3.3, 4.4)))),
        Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(2.2, 3.3, 4.4)))),
        Row(2, Row(1.323, "abc", mutable.WrappedArray.make(Array(2.2, 3.3, 4.4)))),
        Row(3, Row(1.323, "abc", mutable.WrappedArray.make(Array(2.2, 3.3, 4.4)))),
        Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(2.2, 3.3, 4.4)))),
        Row(2, Row(1.323, "abc", mutable.WrappedArray.make(Array(2.2, 3.3, 4.4)))),
        Row(3, Row(1.323, "abc", mutable.WrappedArray.make(Array(2.2, 3.3, 4.4)))),
        Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(2.2, 3.3, 4.4)))),
        Row(2, Row(1.323, "abc", mutable.WrappedArray.make(Array(2.2, 3.3, 4.4)))),
        Row(3, Row(1.323, "abc", mutable.WrappedArray.make(Array(2.2, 3.3, 4.4))))
      ))
  }

  test("test Double with Struct and Array DOUBLE --> SHORT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:double,name:string," +
      "marks:array<double>>) stored by 'carbondata'")
    sql("insert into adaptive values(1,'1.323\001abc\00120.2\00230.3\00240.4')")
    sql("insert into adaptive values(2,'1.324\001abc\00120.2\00230.3\00240.5')")
    sql("insert into adaptive values(3,'1.325\001abc\00120.2\00230.3\00240.6')")
    sql("insert into adaptive values(4,'1.326\001abc\00120.2\00230.3\00240.7')")
    sql("alter table adaptive compact 'major' ")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 40.4)))),
        Row(2, Row(1.324, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 40.5)))),
        Row(3, Row(1.325, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 40.6)))),
        Row(4, Row(1.326, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 40.7))))
      ))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:double,name:string,marks:array<double>>)" +
      " " +
      "stored by 'carbondata'")
    sql(
      s"load data inpath '$resourcesPath/adap_double2.csv' into table adaptive options('delimiter'='," +
      "'," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_double2.csv' into table adaptive options('delimiter'='," +
      "'," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_double2.csv' into table adaptive options('delimiter'='," +
      "'," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_double2.csv' into table adaptive options('delimiter'='," +
      "'," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql("alter table adaptive compact 'major' ")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 40.4)))),
        Row(2, Row(2.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 40.4)))),
        Row(3, Row(4.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 40.4)))),
        Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 40.4)))),
        Row(2, Row(2.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 40.4)))),
        Row(3, Row(4.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 40.4)))),
        Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 40.4)))),
        Row(2, Row(2.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 40.4)))),
        Row(3, Row(4.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 40.4)))),
        Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 40.4)))),
        Row(2, Row(2.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 40.4)))),
        Row(3, Row(4.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 40.4))))
      ))
  }

  test("test Double with Struct and Array DOUBLE --> SHORT INT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:double,name:string," +
      "marks:array<double>>) stored by 'carbondata'")
    sql("insert into adaptive values(1,'10.323\001abc\00120.2\00230.3\002501.423')")
    sql("insert into adaptive values(2,'10.323\001abc\00120.2\00230.3\002502.421')")
    sql("insert into adaptive values(3,'10.323\001abc\00120.2\00230.3\002503.422')")
    sql("insert into adaptive values(4,'10.323\001abc\00120.2\00230.3\002504.424')")
    sql("alter table adaptive compact 'major' ")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(10.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 501.423)))),
        Row(2, Row(10.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 502.421)))),
        Row(3, Row(10.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 503.422)))),
        Row(4, Row(10.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 504.424))))      ))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:double,name:string,marks:array<double>>)" +
      " " +
      "stored by 'carbondata'")
    sql(
      s"load data inpath '$resourcesPath/adap_double3.csv' into table adaptive options('delimiter'='," +
      "'," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_double3.csv' into table adaptive options('delimiter'='," +
      "'," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_double3.csv' into table adaptive options('delimiter'='," +
      "'," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_double3.csv' into table adaptive options('delimiter'='," +
      "'," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql("alter table adaptive compact 'major' ")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 500.423)))),
        Row(2, Row(2.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 500.423)))),
        Row(3, Row(50.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 500.423)))),
        Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 500.423)))),
        Row(2, Row(2.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 500.423)))),
        Row(3, Row(50.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 500.423)))),
        Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 500.423)))),
        Row(2, Row(2.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 500.423)))),
        Row(3, Row(50.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 500.423)))),
        Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 500.423)))),
        Row(2, Row(2.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 500.423)))),
        Row(3, Row(50.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 500.423))))
      ))
  }

  test("test Double with Struct and Array DOUBLE --> INT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:double,name:string," +
      "marks:array<double>>) stored by 'carbondata'")
    sql("insert into adaptive values(1,'1000.323\001abc\00120.2\00230.3\00260000.423')")
    sql("insert into adaptive values(2,'1000.324\001abc\00120.2\00230.3\00270000.424')")
    sql("insert into adaptive values(3,'1000.325\001abc\00120.2\00230.3\00280000.425')")
    sql("insert into adaptive values(4,'1000.326\001abc\00120.2\00230.3\00290000.426')")
    sql("alter table adaptive compact 'major' ")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(1000.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 60000.423)))),
        Row(2, Row(1000.324, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 70000.424)))),
        Row(3, Row(1000.325, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 80000.425)))),
        Row(4, Row(1000.326, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 90000.426))))
      ))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:double,name:string,marks:array<double>>)" +
      " " +
      "stored by 'carbondata'")
    sql(
      s"load data inpath '$resourcesPath/adap_double4.csv' into table adaptive options('delimiter'='," +
      "'," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_double4.csv' into table adaptive options('delimiter'='," +
      "'," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_double4.csv' into table adaptive options('delimiter'='," +
      "'," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_double4.csv' into table adaptive options('delimiter'='," +
      "'," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql("alter table adaptive compact 'major' ")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 50000.423)))),
        Row(2, Row(2.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 50000.423)))),
        Row(3, Row(50000.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 50000.423)))),
        Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 50000.423)))),
        Row(2, Row(2.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 50000.423)))),
        Row(3, Row(50000.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 50000.423)))),
        Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 50000.423)))),
        Row(2, Row(2.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 50000.423)))),
        Row(3, Row(50000.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 50000.423)))),
        Row(1, Row(1.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 50000.423)))),
        Row(2, Row(2.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 50000.423)))),
        Row(3, Row(50000.323, "abc", mutable.WrappedArray.make(Array(20.2, 30.3, 50000.423))))
      ))
  }

  test("test Double with Struct and Array DOUBLE --> DOUBLE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:double,name:string," +
      "marks:array<double>>) stored by 'carbondata'")
    sql("insert into adaptive values(1,'1.797693134862315\001abc\0012.2\00230.3\0021.797693134862315')")
    sql("insert into adaptive values(2,'1.797693134862316\001abc\0012.2\00230.3\0021.797693134862316')")
    sql("insert into adaptive values(3,'1.797693134862317\001abc\0012.2\00230.3\0021.797693134862317')")
    sql("insert into adaptive values(4,'1.797693134862318\001abc\0012.2\00230.3\0021.797693134862318')")
    sql("alter table adaptive compact 'major' ")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1,
        Row(1.797693134862315,
          "abc",
          mutable.WrappedArray.make(Array(2.2, 30.3, 1.797693134862315)))),
        Row(2,
          Row(1.797693134862316,
            "abc",
            mutable.WrappedArray.make(Array(2.2, 30.3, 1.797693134862316)))),
        Row(3,
          Row(1.797693134862317,
            "abc",
            mutable.WrappedArray.make(Array(2.2, 30.3, 1.797693134862317)))),
        Row(4,
          Row(1.797693134862318,
            "abc",
            mutable.WrappedArray.make(Array(2.2, 30.3, 1.797693134862318))))
      ))

  }

  test("test Decimal with Struct") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:decimal(3,2),name:string>) stored by " +
      "'carbondata'")
    sql("insert into adaptive values(1,'3.2\001abc')")
    sql("select * from adaptive").show(false)
  }

  test("test Decimal with Array") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<name:string," +
      "marks:array<decimal>>) stored by 'carbondata'")
    sql("insert into adaptive values(1,'abc\00120.2\00230.3\00240.4')")
    sql("select * from adaptive").show(false)
  }

  test("test Timestamp with Struct") {
    sql("Drop table if exists adaptive")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(
      "create table adaptive(roll int, student struct<id:timestamp,name:string>) stored by " +
      "'carbondata'")
    sql("insert into adaptive values(1,'2017/01/01 00:00:00\001abc')")
    sql("insert into adaptive values(2,'2017/01/02 00:00:00\001abc')")
    sql("insert into adaptive values(3,'2017/01/03 00:00:00\001abc')")
    sql("insert into adaptive values(4,'2017/01/04 00:00:00\001abc')")
    sql("alter table adaptive compact 'major' ")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(Timestamp.valueOf("2017-01-01 00:00:00.0"), "abc")),
        Row(2, Row(Timestamp.valueOf("2017-01-02 00:00:00.0"), "abc")),
        Row(3, Row(Timestamp.valueOf("2017-01-03 00:00:00.0"), "abc")),
        Row(4, Row(Timestamp.valueOf("2017-01-04 00:00:00.0"), "abc"))
      ))
  }

  test("test Timestamp with Array") {
    sql("Drop table if exists adaptive")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(
      "create table adaptive(roll int, student struct<name:string," +
      "marks:array<timestamp>>) stored by 'carbondata'")
    sql("insert into adaptive values(1,'abc1\0012017/01/01\0022018/01/01')")
    sql("insert into adaptive values(2,'abc2\0012017/01/02\0022018/01/03')")
    sql("insert into adaptive values(3,'abc3\0012017/01/04\0022018/01/05')")
    sql("insert into adaptive values(4,'abc4\0012017/01/06\0022018/01/07')")
    sql("alter table adaptive compact 'major' ")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1,
        Row("abc1",
          mutable.WrappedArray
            .make(Array(Timestamp.valueOf("2017-01-01 00:00:00.0"),
              Timestamp.valueOf("2018-01-01 00:00:00.0"))))),
        Row(2,
          Row("abc2",
            mutable.WrappedArray
              .make(Array(Timestamp.valueOf("2017-01-02 00:00:00.0"),
                Timestamp.valueOf("2018-01-03 00:00:00.0"))))),
        Row(3,
          Row("abc3",
            mutable.WrappedArray
              .make(Array(Timestamp.valueOf("2017-01-04 00:00:00.0"),
                Timestamp.valueOf("2018-01-05 00:00:00.0"))))),
        Row(4,
          Row("abc4",
            mutable.WrappedArray
              .make(Array(Timestamp.valueOf("2017-01-06 00:00:00.0"),
                Timestamp.valueOf("2018-01-07 00:00:00.0")))))
      ))
  }

  test("test DATE with Array") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<name:string," +
      "marks:array<date>>) stored by 'carbondata'")
    sql("insert into adaptive values(1,'abc\0012017-01-01')")
    sql("select * from adaptive").show(false)
  }

  test("test LONG with Array and Struct Encoding LONG --> BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:long,name:string,marks:array<long>>) " +
      "stored by 'carbondata'")
    sql("insert into adaptive values(1,'11111\001abc\00120\00230\00240')")
    sql("insert into adaptive values(2,'11111\001abc\00155\00265\00275')")
    sql("insert into adaptive values(3,'11111\001abc\00188\00298\0028')")
    sql("insert into adaptive values(4,'11111\001abc\00199\0029\00219')")
    sql("alter table adaptive compact 'major' ")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(11111, "abc", mutable.WrappedArray.make(Array(20, 30, 40)))),
        Row(2, Row(11111, "abc", mutable.WrappedArray.make(Array(55, 65, 75)))),
        Row(3, Row(11111, "abc", mutable.WrappedArray.make(Array(88, 98, 8)))),
        Row(4, Row(11111, "abc", mutable.WrappedArray.make(Array(99, 9, 19))))
      ))
  }

  test("test LONG with Array and Struct Encoding LONG --> SHORT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:long,name:string,marks:array<long>>) " +
      "stored by 'carbondata'")
    sql("insert into adaptive values(1,'11111\001abc\001200\002300\002400')")
    sql("insert into adaptive values(2,'11111\001abc\001201\002301\002401')")
    sql("insert into adaptive values(3,'11111\001abc\001202\002302\002402')")
    sql("insert into adaptive values(4,'11111\001abc\001203\002303\002403')")
    sql("alter table adaptive compact 'major' ")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(11111, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(11111, "abc", mutable.WrappedArray.make(Array(201, 301, 401)))),
        Row(3, Row(11111, "abc", mutable.WrappedArray.make(Array(202, 302, 402)))),
        Row(4, Row(11111, "abc", mutable.WrappedArray.make(Array(203, 303, 403))))
      ))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:LONG,name:string,marks:array<LONG>>) " +
      "stored by 'carbondata'")
    sql(
      s"load data inpath '$resourcesPath/adap_int1.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_int1.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_int1.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_int1.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql("alter table adaptive compact 'major' ")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(700, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(3, Row(800, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(700, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(3, Row(800, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(700, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(3, Row(800, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(700, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(3, Row(800, "abc", mutable.WrappedArray.make(Array(200, 300, 400))))
      ))
  }

  test("test LONG with struct and array, Encoding LONG-->SHORT INT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:LONG,name:string,marks:array<LONG>>) " +
      "stored by 'carbondata'")
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
      "stored by 'carbondata'")
    sql("insert into adaptive values(1,'50000\001abc\0012000000\0023000000\0024000000')")
    sql("insert into adaptive values(2,'70000\001abc\0012000000\0023000000\0024000000')")
    sql("insert into adaptive values(3,'100000\001abc\0012000000\0023000000\0024000000')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(50000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(2, Row(70000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000)))),
        Row(3, Row(100000, "abc", mutable.WrappedArray.make(Array(2000000, 3000000, 4000000))))))
  }

  test("test LONG with struct and array, Encoding LONG-->INT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:LONG,name:string,marks:array<LONG>>) " +
      "stored by 'carbondata'")
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
      "stored by 'carbondata'")
    sql("insert into adaptive values(1,'500000\001abc\001200\002300\00252000000')")
    sql("insert into adaptive values(2,'700000\001abc\001200\002300\00252000000')")
    sql("insert into adaptive values(3,'10000000\001abc\001200\002300\00252000000')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(2, Row(700000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000)))),
        Row(3, Row(10000000, "abc", mutable.WrappedArray.make(Array(200, 300, 52000000))))))
  }

  test("test LONG with struct and array, Encoding LONG-->LONG") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:LONG,name:string,marks:array<LONG>>) " +
      "stored by 'carbondata'")
    sql("insert into adaptive values(1,'500000\001abc\001200\002300\00252000000000')")
    sql("insert into adaptive values(2,'700000\001abc\001200\002300\00252000000000')")
    sql("insert into adaptive values(3,'10000000\001abc\001200\002300\00252000000000')")
    sql("select * from adaptive").show(false)
  }

  test("test SHORT with Array and Struct Encoding SHORT -->BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:short,name:string,marks:array<short>>) " +
      "stored by 'carbondata'")
    sql("insert into adaptive values(1,'11\001abc\00120\00230\00240')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(11, "abc", mutable.WrappedArray.make(Array(20, 30, 40))))))
  }

  test("test SHORT with Array and Struct Encoding SHORT --> SHORT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:SHORT,name:string,marks:array<SHORT>>) " +
      "stored by 'carbondata'")
    sql("insert into adaptive values(1,'11111\001abc\001200\002300\002400')")
    sql("insert into adaptive values(1,'11111\001abc\001200\002300\002401')")
    sql("insert into adaptive values(1,'11111\001abc\001200\002300\002402')")
    sql("insert into adaptive values(1,'11111\001abc\001200\002300\002403')")
    sql("alter table adaptive compact 'major' ")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(11111, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(1, Row(11111, "abc", mutable.WrappedArray.make(Array(200, 300, 401)))),
        Row(1, Row(11111, "abc", mutable.WrappedArray.make(Array(200, 300, 402)))),
        Row(1, Row(11111, "abc", mutable.WrappedArray.make(Array(200, 300, 403))))
      ))
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:SHORT,name:string,marks:array<SHORT>>) " +
      "stored by 'carbondata'")
    sql(
      s"load data inpath '$resourcesPath/adap_int1.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_int1.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_int1.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql(
      s"load data inpath '$resourcesPath/adap_int1.csv' into table adaptive options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='roll,student','complex_delimiter_level_1'='$'," +
      "'complex_delimiter_level_2'=':')")
    sql("alter table adaptive compact 'major' ")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(700, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(3, Row(800, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(700, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(3, Row(800, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(700, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(3, Row(800, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(700, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(3, Row(800, "abc", mutable.WrappedArray.make(Array(200, 300, 400))))
      ))
  }

  test("test Boolean with Struct and Array") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:boolean,name:string," +
      "marks:array<boolean>>) " +
      "stored by 'carbondata'")
    sql("insert into adaptive values(1,'true\001abc\001false\002true\002false')")
    sql("insert into adaptive values(1,'true\001abc\001false\002true\002true')")
    sql("insert into adaptive values(1,'true\001abc\001false\002true\002true')")
    sql("insert into adaptive values(1,'true\001abc\001false\002true\002false')")
    sql("alter table adaptive compact 'major' ")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(true, "abc", mutable.WrappedArray.make(Array(false, true, false)))),
        Row(1, Row(true, "abc", mutable.WrappedArray.make(Array(false, true, true)))),
        Row(1, Row(true, "abc", mutable.WrappedArray.make(Array(false, true, true)))),
        Row(1, Row(true, "abc", mutable.WrappedArray.make(Array(false, true, false))))
      ))
  }

  test("complex type compaction") {
    sql("drop table if exists complexcarbontable")
    sql("create table complexcarbontable(deviceInformationId int, channelsId string," +
        "ROMSize string, purchasedate string, mobile struct<imei:string, imsi:string>," +
        "MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, " +
        "ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>," +
        "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId " +
        "double,contractNumber double) " +
        "STORED BY 'org.apache.carbondata.format' " +
        "TBLPROPERTIES ('DICTIONARY_INCLUDE'='deviceInformationId')"
    )
    sql(
      s"LOAD DATA local inpath '$resourcesPath/complexdata.csv' INTO table " +
      "complexcarbontable " +
      "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId," +
      "ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber'," +
      "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')"
    )
    sql(
      s"LOAD DATA local inpath '$resourcesPath/complexdata.csv' INTO table " +
      "complexcarbontable " +
      "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId," +
      "ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber'," +
      "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')"
    )
    sql("alter table complexcarbontable compact 'minor'")
    sql(
      "select locationinfo,proddate from complexcarbontable where deviceInformationId=1 limit 1")
      .show(false)
    checkAnswer(sql(
      "select locationinfo,proddate from complexcarbontable where deviceInformationId=1 limit 1"),
      Seq(Row(mutable
        .WrappedArray
        .make(Array(Row(7, "Chinese", "Hubei Province", "yichang", "yichang", "yichang"),
          Row(7, "India", "New Delhi", "delhi", "delhi", "delhi"))),
        Row("29-11-2015", mutable
          .WrappedArray.make(Array("29-11-2015", "29-11-2015"))))))
    sql("drop table if exists complexcarbontable")
  }

  test("test minor compaction with all complex types") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:SHORT,name:string,marks:array<SHORT>>, " +
      "mapField map<int, string>) " +
      "stored by 'carbondata'")
    sql("insert into adaptive values(1,'11111\001abc\001200\002300\002400','1\002Nalla\0012" +
        "\002Singh\0013\002Gupta\0014\002Kumar')")
    sql("insert into adaptive values(1,'11111\001abc\001200\002300\002401','11\002Nalla\00112" +
        "\002Singh\00113\002Gupta\00114\002Kumar')")
    sql("insert into adaptive values(1,'11111\001abc\001200\002300\002402','21\002Nalla\00122" +
        "\002Singh\00123\002Gupta\00124\002Kumar')")
    sql("insert into adaptive values(1,'11111\001abc\001200\002300\002403','31\002Nalla\00132" +
        "\002Singh\00133\002Gupta\00134\002Kumar')")
    sql("alter table adaptive compact 'minor' ")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(11111, "abc", mutable.WrappedArray.make(Array(200, 300, 400))), Map(1 -> "Nalla", 2 -> "Singh", 3 -> "Gupta", 4 -> "Kumar")),
        Row(1, Row(11111, "abc", mutable.WrappedArray.make(Array(200, 300, 401))), Map(11 -> "Nalla", 12 -> "Singh", 13 -> "Gupta", 14 -> "Kumar")),
        Row(1, Row(11111, "abc", mutable.WrappedArray.make(Array(200, 300, 402))), Map(21 -> "Nalla", 22 -> "Singh", 23 -> "Gupta", 24 -> "Kumar")),
        Row(1, Row(11111, "abc", mutable.WrappedArray.make(Array(200, 300, 403))), Map(31 -> "Nalla", 32 -> "Singh", 33 -> "Gupta", 34 -> "Kumar"))
      ))
    sql("Drop table if exists adaptive")
  }

  test("Test major compaction with dictionary include for struct of array type") {
    sql("DROP TABLE IF EXISTS compactComplex")
    sql(
      "CREATE TABLE compactComplex(CUST_ID string,YEAR int, MONTH int, AGE int, GENDER string,EDUCATED " +
      "string,IS_MARRIED " +
      "string," +
      "STRUCT_OF_ARRAY struct<ID:int,CHECK_DATE:string,SNo:array<int>,sal1:array<double>," +
      "state:array<string>," +
      "date1:array<string>>,CARD_COUNT int,DEBIT_COUNT int,CREDIT_COUNT int, DEPOSIT double, " +
      "HQ_DEPOSIT double) STORED BY 'carbondata'" +
      "TBLPROPERTIES('DICTIONARY_INCLUDE'='STRUCT_OF_ARRAY,DEPOSIT,HQ_DEPOSIT')")
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/structofarray.csv' INTO TABLE compactComplex OPTIONS" +
      s"('DELIMITER'=',','QUOTECHAR'='\'," +
      "'FILEHEADER'='CUST_ID,YEAR,MONTH,AGE, GENDER,EDUCATED,IS_MARRIED,STRUCT_OF_ARRAY," +
      "CARD_COUNT," +
      "DEBIT_COUNT,CREDIT_COUNT, DEPOSIT,HQ_DEPOSIT','COMPLEX_DELIMITER_LEVEL_1'='$', " +
      "'COMPLEX_DELIMITER_LEVEL_2'='&')")
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/structofarray.csv' INTO TABLE compactComplex OPTIONS" +
      s"('DELIMITER'=',','QUOTECHAR'='\'," +
      "'FILEHEADER'='CUST_ID,YEAR,MONTH,AGE, GENDER,EDUCATED,IS_MARRIED,STRUCT_OF_ARRAY," +
      "CARD_COUNT," +
      "DEBIT_COUNT,CREDIT_COUNT, DEPOSIT,HQ_DEPOSIT','COMPLEX_DELIMITER_LEVEL_1'='$', " +
      "'COMPLEX_DELIMITER_LEVEL_2'='&')")
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/structofarray.csv' INTO TABLE compactComplex OPTIONS" +
      s"('DELIMITER'=',','QUOTECHAR'='\'," +
      "'FILEHEADER'='CUST_ID,YEAR,MONTH,AGE,GENDER,EDUCATED,IS_MARRIED,STRUCT_OF_ARRAY," +
      "CARD_COUNT," +
      "DEBIT_COUNT,CREDIT_COUNT, DEPOSIT,HQ_DEPOSIT','COMPLEX_DELIMITER_LEVEL_1'='$', " +
      "'COMPLEX_DELIMITER_LEVEL_2'='&')")
    sql("ALTER TABLE compactComplex COMPACT 'major'")
    checkAnswer(sql("Select count(*) from compactComplex"), Row(30))
  }

  test("Test Compaction for complex types with table restructured") {
    sql("drop table if exists compactComplex")
    sql(
      """
        | create table compactComplex (
        | name string,
        | age int,
        | number string,
        | structfield struct<a:array<int> ,b:int>
        | )
        | stored by 'carbondata'
        | TBLPROPERTIES(
        | 'DICTIONARY_INCLUDE'='name,age,number,structfield'
        | )
      """.stripMargin)
    sql("INSERT into compactComplex values('man',25,'222','1000\0022000\0011')")
    sql("INSERT into compactComplex values('can',24,'333','1000\0022000\0012')")
    sql("INSERT into compactComplex values('dan',25,'222','1000\0022000\0013')")
    sql("ALTER TABLE compactComplex drop columns(age)")
    sql("ALTER TABLE compactComplex COMPACT 'major'")
    checkAnswer(sql("SELECT * FROM compactComplex"),
      Seq(Row("man", "222", Row(mutable.WrappedArray.make(Array(1000, 2000)), 1)),
        Row("can", "333", Row(mutable.WrappedArray.make(Array(1000, 2000)), 2)),
        Row("dan", "222", Row(mutable.WrappedArray.make(Array(1000, 2000)), 3))
      ))

  }

}
