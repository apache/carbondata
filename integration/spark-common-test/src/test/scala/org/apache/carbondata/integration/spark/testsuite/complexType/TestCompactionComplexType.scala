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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestCompactionComplexType extends QueryTest {

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
    sql("insert into adaptive values(1,'500$abc$20:30:40')")
    sql("insert into adaptive values(2,'600$abc$30:30:40')")
    sql("insert into adaptive values(3,'700$abc$40:30:40')")
    sql("insert into adaptive values(4,'800$abc$50:30:40')")
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
    sql("insert into adaptive values(1,'500$abc$200:300:400')")
    sql("insert into adaptive values(2,'600$abc$300:300:400')")
    sql("insert into adaptive values(3,'700$abc$400:300:400')")
    sql("insert into adaptive values(4,'800$abc$500:300:400')")
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
    sql("insert into adaptive values(1,'50000$abc$2000000:3000000:4000000')")
    sql("insert into adaptive values(2,'70000$abc$2000000:4000000:4000000')")
    sql("insert into adaptive values(3,'100000$abc$2000000:5000000:4000000')")
    sql("insert into adaptive values(4,'200000$abc$2000000:6000000:4000000')")
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
    sql("insert into adaptive values(1,'500000$abc$200:300:52000000')")
    sql("insert into adaptive values(2,'700000$abc$210:350:52000000')")
    sql("insert into adaptive values(3,'10000000$abc$200:300:52000000')")
    sql("insert into adaptive values(4,'10000001$abd$250:450:62000000')")
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
    sql("insert into adaptive values(1,'100$abc$20:30:40')")
    sql("insert into adaptive values(2,'200$abc$30:40:50')")
    sql("insert into adaptive values(3,'300$abd$30:41:55')")
    sql("insert into adaptive values(4,'400$abe$30:42:56')")
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
    sql("insert into adaptive values(1,'500$abc$200:300:400')")
    sql("insert into adaptive values(2,'8000$abc$300:410:500')")
    sql("insert into adaptive values(3,'9000$abee$310:420:400')")
    sql("insert into adaptive values(4,'9900$abfffffffffffffff$320:430:500')")
    sql("alter table adaptive compact 'major'").show(200,false)
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(500, "abc", mutable.WrappedArray.make(Array(200, 300, 400)))),
        Row(2, Row(8000, "abc", mutable.WrappedArray.make(Array(300, 410, 500)))),
        Row(3, Row(9000, "abee", mutable.WrappedArray.make(Array(310, 420, 400)))),
        Row(4, Row(9900, "abfffffffffffffff", mutable.WrappedArray.make(Array(320, 430, 500))))))
    sql("insert into adaptive values(5,'500$abc$200:310:400')")
    sql("insert into adaptive values(6,'8000$abc$300:310:500')")
    sql("insert into adaptive values(7,'9000$abee$310:320:400')")
    sql("insert into adaptive values(8,'9900$abfffffffffffffffeeee$320:330:500')")
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
    sql("insert into adaptive values(11,'1$abc$21:30:40')")
    sql("insert into adaptive values(12,'1$ab1$22:30:40')")
    sql("insert into adaptive values(13,'1$ab2$23:30:40')")
    sql("insert into adaptive values(14,'1$ab3$24:30:40')")
    sql("insert into adaptive values(15,'1$ab4$25:30:40')")
    sql("insert into adaptive values(16,'1$ab5$26:30:40')")
    sql("insert into adaptive values(17,'1$ab6$27:30:40')")
    sql("insert into adaptive values(18,'1$ab7$28:30:40')")
    sql("insert into adaptive values(19,'1$ab8$29:30:40')")
    sql("insert into adaptive values(20,'1$ab9$30:30:40')")
    sql("insert into adaptive values(21,'1$ab10$31:30:40')")
    sql("insert into adaptive values(22,'1$ab11$32:30:40')")
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
    sql("insert into adaptive values(1,'500$abc$200:300:400')")
    sql("insert into adaptive values(2,'8000$abc$300:400:500')")
    sql("insert into adaptive values(3,'9000$abc$300:400:500')")
    sql("insert into adaptive values(4,'10000$abc$300:400:500')")
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
    sql("insert into adaptive values(1,'50000$abc$2000000:3000000:4000000')")
    sql("insert into adaptive values(2,'70000$abc$2000000:3000000:4000000')")
    sql("insert into adaptive values(3,'100000$abc$2000000:3000000:4000000')")
    sql("insert into adaptive values(1,'50000$abc$2000000:3000000:4000000')")
    sql("insert into adaptive values(2,'70000$abc$2000000:3000000:4000000')")
    sql("insert into adaptive values(3,'100000$abc$2000000:3000000:4000000')")
    sql("insert into adaptive values(1,'50000$abc$2000000:3000000:4000000')")
    sql("insert into adaptive values(2,'70000$abc$2000000:3000000:4000000')")
    sql("insert into adaptive values(3,'100000$abc$2000000:3000000:4000000')")
    sql("insert into adaptive values(1,'50000$abc$2000000:3000000:4000000')")
    sql("insert into adaptive values(2,'70000$abc$2000000:3000000:4000000')")
    sql("insert into adaptive values(3,'100000$abc$2000000:3000000:4000000')")
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
    sql("insert into adaptive values(1,'500000$abc$200:300:52000000')")
    sql("insert into adaptive values(2,'700000$abc$200:300:52000000')")
    sql("insert into adaptive values(3,'10000000$abc$200:300:52000000')")
    sql("insert into adaptive values(1,'500000$abc$200:300:52000000')")
    sql("insert into adaptive values(2,'700000$abc$200:300:52000000')")
    sql("insert into adaptive values(3,'10000000$abc$200:300:52000000')")
    sql("insert into adaptive values(1,'500000$abc$200:300:52000000')")
    sql("insert into adaptive values(2,'700000$abc$200:300:52000000')")
    sql("insert into adaptive values(3,'10000000$abc$200:300:52000000')")
    sql("insert into adaptive values(1,'500000$abc$200:300:52000000')")
    sql("insert into adaptive values(2,'700000$abc$200:300:52000000')")
    sql("insert into adaptive values(3,'10000000$abc$200:300:52000000')")
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
    sql("insert into adaptive values(1,'1.323$abc$2.2:3.3:4.4')")
    sql("insert into adaptive values(2,'1.324$abc$2.2:3.3:4.4')")
    sql("insert into adaptive values(3,'1.325$abc$2.2:3.3:4.4')")
    sql("insert into adaptive values(4,'1.326$abc$2.2:3.3:4.4')")
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
    sql("insert into adaptive values(1,'1.323$abc$20.2:30.3:40.4')")
    sql("insert into adaptive values(2,'1.324$abc$20.2:30.3:40.5')")
    sql("insert into adaptive values(3,'1.325$abc$20.2:30.3:40.6')")
    sql("insert into adaptive values(4,'1.326$abc$20.2:30.3:40.7')")
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
    sql("insert into adaptive values(1,'10.323$abc$20.2:30.3:501.423')")
    sql("insert into adaptive values(2,'10.323$abc$20.2:30.3:502.421')")
    sql("insert into adaptive values(3,'10.323$abc$20.2:30.3:503.422')")
    sql("insert into adaptive values(4,'10.323$abc$20.2:30.3:504.424')")
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
    sql("insert into adaptive values(1,'1000.323$abc$20.2:30.3:60000.423')")
    sql("insert into adaptive values(2,'1000.324$abc$20.2:30.3:70000.424')")
    sql("insert into adaptive values(3,'1000.325$abc$20.2:30.3:80000.425')")
    sql("insert into adaptive values(4,'1000.326$abc$20.2:30.3:90000.426')")
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
    sql("insert into adaptive values(1,'1.797693134862315$abc$2.2:30.3:1.797693134862315')")
    sql("insert into adaptive values(2,'1.797693134862316$abc$2.2:30.3:1.797693134862316')")
    sql("insert into adaptive values(3,'1.797693134862317$abc$2.2:30.3:1.797693134862317')")
    sql("insert into adaptive values(4,'1.797693134862318$abc$2.2:30.3:1.797693134862318')")
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
    sql("insert into adaptive values(1,'3.2$abc')")
    sql("select * from adaptive").show(false)
  }

  test("test Decimal with Array") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<name:string," +
      "marks:array<decimal>>) stored by 'carbondata'")
    sql("insert into adaptive values(1,'abc$20.2:30.3:40.4')")
    sql("select * from adaptive").show(false)
  }

  test("test Timestamp with Struct") {
    sql("Drop table if exists adaptive")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(
      "create table adaptive(roll int, student struct<id:timestamp,name:string>) stored by " +
      "'carbondata'")
    sql("insert into adaptive values(1,'2017/01/01 00:00:00$abc')")
    sql("insert into adaptive values(2,'2017/01/02 00:00:00$abc')")
    sql("insert into adaptive values(3,'2017/01/03 00:00:00$abc')")
    sql("insert into adaptive values(4,'2017/01/04 00:00:00$abc')")
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
    sql("insert into adaptive values(1,'abc1$2017/01/01:2018/01/01')")
    sql("insert into adaptive values(2,'abc2$2017/01/02:2018/01/03')")
    sql("insert into adaptive values(3,'abc3$2017/01/04:2018/01/05')")
    sql("insert into adaptive values(4,'abc4$2017/01/06:2018/01/07')")
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
    sql("insert into adaptive values(1,'abc$2017-01-01')")
    sql("select * from adaptive").show(false)
  }

  test("test LONG with Array and Struct Encoding LONG --> BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:long,name:string,marks:array<long>>) " +
      "stored by 'carbondata'")
    sql("insert into adaptive values(1,'11111$abc$20:30:40')")
    sql("insert into adaptive values(2,'11111$abc$55:65:75')")
    sql("insert into adaptive values(3,'11111$abc$88:98:8')")
    sql("insert into adaptive values(4,'11111$abc$99:9:19')")
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
    sql("insert into adaptive values(1,'11111$abc$200:300:400')")
    sql("insert into adaptive values(2,'11111$abc$201:301:401')")
    sql("insert into adaptive values(3,'11111$abc$202:302:402')")
    sql("insert into adaptive values(4,'11111$abc$203:303:403')")
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
    sql("insert into adaptive values(1,'50000$abc$2000000:3000000:4000000')")
    sql("insert into adaptive values(2,'70000$abc$2000000:3000000:4000000')")
    sql("insert into adaptive values(3,'100000$abc$2000000:3000000:4000000')")
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
    sql("insert into adaptive values(1,'500000$abc$200:300:52000000')")
    sql("insert into adaptive values(2,'700000$abc$200:300:52000000')")
    sql("insert into adaptive values(3,'10000000$abc$200:300:52000000')")
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
    sql("insert into adaptive values(1,'500000$abc$200:300:52000000000')")
    sql("insert into adaptive values(2,'700000$abc$200:300:52000000000')")
    sql("insert into adaptive values(3,'10000000$abc$200:300:52000000000')")
    sql("select * from adaptive").show(false)
  }

  test("test SHORT with Array and Struct Encoding SHORT -->BYTE") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:short,name:string,marks:array<short>>) " +
      "stored by 'carbondata'")
    sql("insert into adaptive values(1,'11$abc$20:30:40')")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(11, "abc", mutable.WrappedArray.make(Array(20, 30, 40))))))
  }

  test("test SHORT with Array and Struct Encoding SHORT --> SHORT") {
    sql("Drop table if exists adaptive")
    sql(
      "create table adaptive(roll int, student struct<id:SHORT,name:string,marks:array<SHORT>>) " +
      "stored by 'carbondata'")
    sql("insert into adaptive values(1,'11111$abc$200:300:400')")
    sql("insert into adaptive values(1,'11111$abc$200:300:401')")
    sql("insert into adaptive values(1,'11111$abc$200:300:402')")
    sql("insert into adaptive values(1,'11111$abc$200:300:403')")
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
    sql("insert into adaptive values(1,'true$abc$false:true:false')")
    sql("insert into adaptive values(1,'true$abc$false:true:true')")
    sql("insert into adaptive values(1,'true$abc$false:true:true')")
    sql("insert into adaptive values(1,'true$abc$false:true:false')")
    sql("alter table adaptive compact 'major' ")
    checkAnswer(sql("select * from adaptive"),
      Seq(Row(1, Row(true, "abc", mutable.WrappedArray.make(Array(false, true, false)))),
        Row(1, Row(true, "abc", mutable.WrappedArray.make(Array(false, true, true)))),
        Row(1, Row(true, "abc", mutable.WrappedArray.make(Array(false, true, true)))),
        Row(1, Row(true, "abc", mutable.WrappedArray.make(Array(false, true, false))))
      ))
  }

}
