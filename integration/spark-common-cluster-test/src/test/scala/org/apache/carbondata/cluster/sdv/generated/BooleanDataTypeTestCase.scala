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

package org.apache.carbondata.cluster.sdv.generated

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.{Include, QueryTest}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class BooleanDataTypeTestCase extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {

  override def beforeEach() = {
    sql("drop table if exists boolean_table")
  }

  override def afterEach() = {
    sql("drop table if exists boolean_table")
  }

  test("test create table with booolean data types", Include) {
    Try {
      sql("CREATE TABLE if not exists boolean_table(aa INT, bb STRING, cc BOOLEAN) STORED BY 'carbondata'").show()
    } match {
      case Success(_) => assert(true)
      case Failure(_) => assert(false)
    }
    checkExistence(sql("describe formatted boolean_table"), true, "boolean")
  }

  test("Altering table and add column: add boolean type column") {
    sql(
      s"""
         |CREATE TABLE if not exists boolean_table(
         |aa INT, bb STRING
         |) STORED BY 'carbondata'""".stripMargin)
    sql("alter table boolean_table add columns (dd BOOLEAN)")
    checkExistence(sql("describe formatted boolean_table"), true, "boolean")
    checkExistence(sql("describe formatted boolean_table"), true, "dd")
  }

  test("Create table, insert values and alter table: exists boolean column, add boolean type column") {
    sql(
      s"""
         |CREATE TABLE if not exists boolean_table(
         |aa STRING, bb INT, cc BOOLEAN
         |) STORED BY 'carbondata'""".stripMargin)
    sql("insert into boolean_table values('adam',11,true)")
    sql("alter table boolean_table add columns (dd BOOLEAN)")
    checkAnswer(sql("select * from boolean_table"), Seq(Row("adam", 11, true, null)))
  }

  test("Alter table to add column and insert values: exists boolean column, add boolean type column") {

    sql(
      s"""
         |CREATE TABLE if not exists boolean_table(
         |aa STRING, bb INT, cc BOOLEAN
         |) STORED BY 'carbondata'""".stripMargin)
    sql("alter table boolean_table add columns (dd BOOLEAN)")
    sql("insert into boolean_table values('adam',11,true,false)")
    checkAnswer(sql("select * from boolean_table"), Seq(Row("adam", 11, true, false)))
  }

  test("Alter table to drop boolean column and insert values: exists boolean column, add boolean type column") {
    sql(
      s"""
         |CREATE TABLE if not exists boolean_table(
         |aa STRING, bb INT, cc BOOLEAN, dd BOOLEAN
         |) STORED BY 'carbondata'""".stripMargin)
    sql("alter table boolean_table drop columns (dd)")
    sql("insert into boolean_table values('adam',11,true)")
    checkAnswer(sql("select * from boolean_table"), Seq(Row("adam", 11, true)))
  }


  test("test filter on boolean data value") {
    sql(
      s"""
         |CREATE TABLE if not exists boolean_table(
         |aa STRING, bb INT, cc BOOLEAN, dd BOOLEAN
         |) STORED BY 'carbondata'""".stripMargin)
    sql("insert into boolean_table values('adam',11,true,true)")
    sql("insert into boolean_table values('james',12,false,false)")
    sql("insert into boolean_table values('smith',13,true,true)")
    checkAnswer(sql("select * from boolean_table where cc=false"), Seq(Row("james", 12, false, false)))
  }

  test("test filter on boolean data value with boolean column as Sort column") {
    sql(
      s"""
         |CREATE TABLE if not exists boolean_table(
         |aa STRING, bb INT, cc BOOLEAN, dd BOOLEAN
         |) STORED BY 'carbondata' TBLPROPERTIES ('SORT_COLUMNS'='cc')""".stripMargin)
    sql("insert into boolean_table values('adam',11,true,true)")
    sql("insert into boolean_table values('james',12,false,false)")
    sql("insert into boolean_table values('smith',13,true,true)")
    sql("insert into boolean_table values('sangeeta',13,false,true)")
    checkAnswer(sql("select * from boolean_table where cc=false"), Seq(Row("james", 12, false, false)
      , Row("sangeeta", 13, false, true)))
  }

  test("insert from hive table to carbon table with boolean data type") {
    sql("drop table if exists boolean_table_hive")
    sql(
      s"""
         |CREATE TABLE if not exists boolean_table_hive(
         |aa STRING, bb INT, cc BOOLEAN, dd BOOLEAN
         |)""".stripMargin)

    sql("insert into boolean_table_hive values('adam',11,true,true)")
    sql("insert into boolean_table_hive values('james',12,false,false)")
    sql("insert into boolean_table_hive values('smith',13,true,true)")

    sql(
      s"""
         |CREATE TABLE if not exists boolean_table(
         |aa STRING, bb INT, cc BOOLEAN, dd BOOLEAN
         |) STORED BY 'carbondata' TBLPROPERTIES ('SORT_COLUMNS'='cc')""".stripMargin)

    sql("insert into boolean_table select * from boolean_table_hive")

    checkAnswer(sql("select * from boolean_table"), Seq(Row("james", 12, false, false),
      Row("adam", 11, true, true), Row("smith",13, true, true)))
  }


  test("test insert overwrite on table with boolean column ") {
    sql(
      s"""
         |CREATE TABLE if not exists boolean_table(
         |bb INT, cc BOOLEAN
         |) STORED BY 'carbondata' TBLPROPERTIES ('SORT_COLUMNS'='cc')""".stripMargin)

    sql("insert overwrite table boolean_table values(1,true)")
    checkAnswer(
      sql("select * from boolean_table"),
      Seq(Row(1, true))
    )

    sql("insert overwrite table boolean_table values(1,false)")
    checkAnswer(
      sql("select * from boolean_table"),
      Seq(Row(1, false))
    )
  }

}
