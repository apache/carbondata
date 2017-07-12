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

package org.apache.spark.carbondata.restructure.vectorreader

import java.math.BigDecimal

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.Spark2QueryTest
import org.scalatest.BeforeAndAfterAll

class ChangeDataTypeTestCases extends Spark2QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS changedatatypetest")
    sql("drop table if exists hivetable")
  }

  test("test change datatype on existing column and load data, insert into hive table") {
    beforeAll
    sql(
      "CREATE TABLE changedatatypetest(intField int,stringField string,charField string," +
      "timestampField timestamp,decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
        s"changedatatypetest options('FILEHEADER'='intField,stringField,charField,timestampField," +
        s"decimalField')")
    sql("Alter table changedatatypetest change intField intfield bigint")
    sql(
      "CREATE TABLE hivetable(intField bigint,stringField string,charField string,timestampField " +
      "timestamp,decimalField decimal(6,2)) stored as parquet")
    sql("insert into table hivetable select * from changedatatypetest")
    afterAll
  }

  test("test datatype change and filter") {
    beforeAll
    sql(
      "CREATE TABLE changedatatypetest(intField int,stringField string,charField string," +
      "timestampField timestamp,decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
        s"changedatatypetest options('FILEHEADER'='intField,stringField,charField,timestampField," +
        s"decimalField')")
    sql("Alter table changedatatypetest change intField intfield bigint")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
        s"changedatatypetest options('FILEHEADER'='intField,stringField,charField,timestampField," +
        s"decimalField')")
    checkAnswer(sql("select charField from changedatatypetest where intField > 99"),
      Seq(Row("abc"), Row("abc")))
    checkAnswer(sql("select charField from changedatatypetest where intField < 99"), Seq())
    checkAnswer(sql("select charField from changedatatypetest where intField = 100"),
      Seq(Row("abc"), Row("abc")))
    afterAll
  }


  test("test change int datatype and load data") {
    beforeAll
    sql(
      "CREATE TABLE changedatatypetest(intField int,stringField string,charField string," +
      "timestampField timestamp,decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
        s"changedatatypetest options('FILEHEADER'='intField,stringField,charField,timestampField," +
        s"decimalField')")
    sql("Alter table changedatatypetest change intField intfield bigint")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
        s"changedatatypetest options('FILEHEADER'='intField,stringField,charField,timestampField," +
        s"decimalField')")
    checkAnswer(sql("select sum(intField) from changedatatypetest"), Row(200))
    afterAll
  }

  test("test change decimal datatype and compaction") {
    beforeAll
    sql(
      "CREATE TABLE changedatatypetest(intField int,stringField string,charField string," +
      "timestampField timestamp,decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
        s"changedatatypetest options('FILEHEADER'='intField,stringField,charField,timestampField," +
        s"decimalField')")
    sql("Alter table changedatatypetest change decimalField decimalField decimal(9,5)")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
        s"changedatatypetest options('FILEHEADER'='intField,stringField,charField,timestampField," +
        s"decimalField')")
    checkAnswer(sql("select decimalField from changedatatypetest"),
      Seq(Row(new BigDecimal("21.23").setScale(5)), Row(new BigDecimal("21.23").setScale(5))))
    sql("alter table changedatatypetest compact 'major'")
    checkExistence(sql("show segments for table changedatatypetest"), true, "0Compacted")
    checkExistence(sql("show segments for table changedatatypetest"), true, "1Compacted")
    checkExistence(sql("show segments for table changedatatypetest"), true, "0.1Success")
    afterAll
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS changedatatypetest")
    sql("drop table if exists hivetable")
    sqlContext.setConf("carbon.enable.vector.reader", "false")
  }
}
