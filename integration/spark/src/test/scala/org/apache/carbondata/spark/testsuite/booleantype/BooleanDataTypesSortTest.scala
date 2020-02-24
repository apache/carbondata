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

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class BooleanDataTypesSortTest extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  val rootPath = new File(this.getClass.getResource("/").getPath
    + "../../../..").getCanonicalPath

  override def beforeEach(): Unit = {
    sql("drop table if exists boolean_table")
    sql("drop table if exists boolean_table2")
  }

  override def afterAll(): Unit = {
    sql("drop table if exists boolean_table")
    sql("drop table if exists boolean_table2")
  }

  test("Sort_columns: sort one boolean column") {
    sql(
      s"""
         | CREATE TABLE boolean_table(
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
         | complexData ARRAY<STRING>
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('sort_columns'='booleanField')
       """.stripMargin)

    sql(
      s"""
         | CREATE TABLE boolean_table2(
         | shortField SHORT,
         | booleanField BOOLEAN
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('sort_columns'='booleanField')
       """.stripMargin)

    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanTwoBooleanColumns.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData')
           """.stripMargin)

    sql("insert into boolean_table2 select shortField,booleanField from boolean_table where shortField = 1 and booleanField = true")

    checkAnswer(
      sql("select shortField,booleanField from boolean_table"),
      Seq(Row(5, false), Row(1, false), Row(2, false), Row(1, false), Row(4, false), Row(1, false), Row(1, true), Row(1, true), Row(1, true), Row(3, true))
    )

    checkAnswer(
      sql("select shortField,booleanField from boolean_table2"),
      Seq(Row(1, true), Row(1, true), Row(1, true))
    )
  }

  test("Sort_columns: sort two boolean columns and other data type column") {
    sql(
      s"""
         | CREATE TABLE boolean_table(
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
         | TBLPROPERTIES('sort_columns'='shortField,booleanField,booleanField2')
       """.stripMargin)

    sql(
      s"""
         | CREATE TABLE boolean_table2(
         | shortField SHORT,
         | booleanField BOOLEAN,
         | booleanField2 BOOLEAN
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('sort_columns'='shortField,booleanField,booleanField2')
       """.stripMargin)

    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanTwoBooleanColumns.csv"
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${storeLocation}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)

    sql("insert into boolean_table2 select shortField,booleanField,booleanField2 from boolean_table where shortField = 1 and booleanField = true")

    checkAnswer(
      sql("select shortField,booleanField,booleanField2 from boolean_table"),
      Seq(Row(5, false, true), Row(1, false, false), Row(2, false, false), Row(1, false, false), Row(4, false, false),
        Row(1, false, true), Row(1, true, true), Row(1, true, true), Row(1, true, true), Row(3, true, false))
    )

    checkAnswer(
      sql("select shortField,booleanField,booleanField2 from boolean_table2"),
      Seq(Row(1, true, true), Row(1, true, true), Row(1, true, true))
    )
  }
}
