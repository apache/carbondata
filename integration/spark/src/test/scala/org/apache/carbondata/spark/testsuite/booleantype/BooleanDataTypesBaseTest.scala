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
package org.apache.carbondata.spark.testsuite.booleantype

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class BooleanDataTypesBaseTest extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {

  override def beforeEach(): Unit = {
    sql("drop table if exists carbon_table")
    sql("drop table if exists boolean_table")
  }

  override def afterEach(): Unit = {
    sql("drop table if exists carbon_table")
    sql("drop table if exists boolean_table")
  }

  test("Creating table: boolean one column, should support") {
    try {
      sql("CREATE TABLE if not exists boolean_table(cc BOOLEAN) STORED AS carbondata")
      assert(true)
    } catch {
      case _: Exception => assert(false)
    }
  }

  test("Creating table: boolean and other table, should support") {
    try {
      sql(
        s"""
           |CREATE TABLE if not exists boolean_table(
           |aa INT, bb STRING, cc BOOLEAN
           |) STORED AS carbondata""".stripMargin)
      assert(true)
    } catch {
      case _: Exception => assert(false)
    }
  }

  test("Describing table: boolean data type, should support") {
    sql(
      s"""
         |CREATE TABLE if not exists carbon_table(
         |cc BOOLEAN
         |) STORED AS carbondata""".stripMargin)
    checkExistence(sql("describe formatted carbon_table"), true, "boolean")
  }

  test("Describing table: support boolean data type format and other table ") {
    sql(
      s"""
         | CREATE TABLE carbon_table(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>,
         | booleanField2 BOOLEAN
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('sort_columns'='')
       """.stripMargin)
    checkExistence(sql("describe formatted carbon_table"), true, "boolean")
  }

  test("Altering table and add column: add boolean type column") {
    sql(
      s"""
         |CREATE TABLE if not exists carbon_table(
         |aa INT, bb STRING
         |) STORED AS carbondata""".stripMargin)
    sql("alter table carbon_table add columns (dd BOOLEAN)")
    checkExistence(sql("describe formatted carbon_table"), true, "boolean")
    checkExistence(sql("describe formatted carbon_table"), true, "dd")
  }

  test("Altering table and add column: exists boolean column, add boolean type column") {
    sql(
      s"""
         |CREATE TABLE if not exists carbon_table(
         |aa INT, bb STRING, cc BOOLEAN
         |) STORED AS carbondata""".stripMargin)
    sql("alter table carbon_table add columns (dd BOOLEAN)")
    checkExistence(sql("describe formatted carbon_table"), true, "boolean")
    checkExistence(sql("describe formatted carbon_table"), true, "dd")
  }

  test("Altering table and add column: exists boolean column, add not boolean type column") {
    sql(
      s"""
         |CREATE TABLE if not exists carbon_table(
         |aa INT, bb STRING, cc BOOLEAN
         |) STORED AS carbondata""".stripMargin)
    sql("alter table carbon_table add columns (dd STRING)")
    checkExistence(sql("describe formatted carbon_table"), true, "boolean")
    checkExistence(sql("describe formatted carbon_table"), true, "dd")
  }

  test("Altering table and add column and insert values: exists boolean column, add boolean type column") {
    sql(
      s"""
         |CREATE TABLE if not exists carbon_table(
         |aa STRING, bb INT, cc BOOLEAN
         |) STORED AS carbondata""".stripMargin)
    sql("alter table carbon_table add columns (dd BOOLEAN)")
    sql("insert into carbon_table values('adam',11,true,false)")
    checkAnswer(sql("select * from carbon_table"), Seq(Row("adam", 11, true, false)))
  }

  test("Altering table and drop column and insert values: exists boolean column, add boolean type column") {
    sql(
      s"""
         |CREATE TABLE if not exists carbon_table(
         |aa STRING, bb INT, cc BOOLEAN, dd BOOLEAN
         |) STORED AS carbondata""".stripMargin)
    sql("alter table carbon_table drop columns (dd)")
    sql("insert into carbon_table values('adam',11,true)")
    checkAnswer(sql("select * from carbon_table"), Seq(Row("adam", 11, true)))
  }

  test("Deleting table and drop column and insert values: exists boolean column, add boolean type column") {
    sql(
      s"""
         |CREATE TABLE if not exists carbon_table(
         |aa STRING, bb INT, cc BOOLEAN, dd BOOLEAN
         |) STORED AS carbondata""".stripMargin)
    sql("alter table carbon_table drop columns (dd)")
    sql("insert into carbon_table values('adam',11,true)")
    checkAnswer(sql("select * from carbon_table"), Seq(Row("adam", 11, true)))
    sql("delete from carbon_table where cc=true")
    checkAnswer(sql("select COUNT(*) from carbon_table"), Row(0))
  }

  test("test boolean as dictionary include column and codegen=false"){
    sql("drop table if exists carbon_table")
    sql("create table carbon_table(a1 boolean,a2 string,a3 int) STORED AS carbondata ")
    sql("insert into carbon_table select false,'a',1")
    sql("set spark.sql.codegen.wholestage=false")
    checkAnswer(sql("select a1 from carbon_table"), Seq(Row(false)))
    sql("set spark.sql.codegen.wholestage=true")
    checkAnswer(sql("select a1 from carbon_table"), Seq(Row(false)))
    sql("insert into carbon_table select true,'a',1")
    sql("set spark.sql.codegen.wholestage=false")
    checkAnswer(sql("select a1 from carbon_table"), Seq(Row(false), Row(true)))
    sql("set spark.sql.codegen.wholestage=true")
    checkAnswer(sql("select a1 from carbon_table"), Seq(Row(false), Row(true)))
    sql("drop table if exists carbon_table")
    CarbonEnv.getInstance(sqlContext.sparkSession).carbonSessionInfo.getSessionParams.clear()
  }
}
