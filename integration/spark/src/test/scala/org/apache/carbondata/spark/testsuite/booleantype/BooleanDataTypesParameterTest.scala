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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class BooleanDataTypesParameterTest extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  val filePath: String = s"$resourcesPath/globalsort"
  val file1: String = resourcesPath + "/globalsort/sample1.csv"
  val file2: String = resourcesPath + "/globalsort/sample2.csv"
  val file3: String = resourcesPath + "/globalsort/sample3.csv"

  override def beforeEach(): Unit = {
    sql("drop table if exists boolean_one_column")
    sql("drop table if exists boolean_table")
    sql(
      s"""CREATE TABLE if not exists boolean_one_column(
         |booleanField BOOLEAN)
         |STORED AS carbondata
         |""".stripMargin)
  }

  val rootPath = new File(this.getClass.getResource("/").getPath
    + "../../../..").getCanonicalPath

  override def beforeAll(): Unit = {
    CarbonProperties.getInstance().
      addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
  }

  override def afterAll(): Unit = {
    sql("drop table if exists boolean_one_column")
    sql("drop table if exists boolean_table")
  }

  test("ENABLE_AUTO_LOAD_MERGE: false, and Inserting and selecting table: one column boolean and many rows, should support") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
    sql("insert into boolean_one_column values(true)")
    sql("insert into boolean_one_column values(True)")
    sql("insert into boolean_one_column values(TRUE)")
    sql("insert into boolean_one_column values('true')")
    sql("insert into boolean_one_column values(False)")
    sql("insert into boolean_one_column values(false)")
    sql("insert into boolean_one_column values(FALSE)")
    sql("insert into boolean_one_column values('false')")
    sql("insert into boolean_one_column values('tr')")
    sql("insert into boolean_one_column values(null)")
    sql("insert into boolean_one_column values('truEe')")
    sql("insert into boolean_one_column values('falsEe')")
    sql("insert into boolean_one_column values('t')")
    sql("insert into boolean_one_column values('f')")

    checkAnswer(
      sql("select * from boolean_one_column"),
      Seq(Row(true), Row(true), Row(true), Row(true),
        Row(false), Row(false), Row(false), Row(false),
        Row(true), Row(false), Row(null), Row(null), Row(null), Row(null))
    )

    val segments = sql("SHOW SEGMENTS FOR TABLE boolean_one_column")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(!SegmentSequenceIds.contains("0.1"))
    assert(SegmentSequenceIds.length == 14)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE,
      CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE)
  }

  test("ENABLE_AUTO_LOAD_MERGE: true, and Inserting and selecting table: one column boolean and many rows, should support") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    sql("insert into boolean_one_column values(true)")
    sql("insert into boolean_one_column values(True)")
    sql("insert into boolean_one_column values(TRUE)")
    sql("insert into boolean_one_column values('true')")
    sql("insert into boolean_one_column values(False)")
    sql("insert into boolean_one_column values(false)")
    sql("insert into boolean_one_column values(FALSE)")
    sql("insert into boolean_one_column values('false')")
    sql("insert into boolean_one_column values('tr')")
    sql("insert into boolean_one_column values(null)")
    sql("insert into boolean_one_column values('truEe')")
    sql("insert into boolean_one_column values('falsEe')")
    sql("insert into boolean_one_column values('t')")
    sql("insert into boolean_one_column values('f')")

    checkAnswer(
      sql("select * from boolean_one_column"),
      Seq(Row(true), Row(true), Row(true), Row(true),
        Row(false), Row(false), Row(false), Row(false),
        Row(true), Row(false), Row(null), Row(null), Row(null), Row(null))
    )

    val segments = sql("SHOW SEGMENTS FOR TABLE boolean_one_column")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(SegmentSequenceIds.length == 18)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE,
      CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE)
  }

  test("ENABLE_AUTO_LOAD_MERGE: false, and Loading table: support boolean and other data type") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
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
         | TBLPROPERTIES('sort_columns'='')
       """.stripMargin)

    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBoolean.csv"
    for (i <- 0 until 4) {
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '${storeLocation}'
           | INTO TABLE boolean_table
           | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)
    }

    checkAnswer(
      sql("select count(*) from boolean_table"),
      Seq(Row(40))
    )
    val segments = sql("SHOW SEGMENTS FOR TABLE boolean_table")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(!SegmentSequenceIds.contains("0.1"))
    assert(SegmentSequenceIds.length == 4)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE,
      CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE)
  }

  test("ENABLE_AUTO_LOAD_MERGE: true, and Loading table: support boolean and other data type") {
    //unfinish
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
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
         | booleanField2 BOOLEAN
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('sort_columns'='')
       """.stripMargin)

    val storeLocation = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanTwoBooleanColumns.csv"
    for (i <- 0 until 4) {
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '${storeLocation}'
           | INTO TABLE boolean_table
           | options('FILEHEADER'='shortField,booleanField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData,booleanField2')
           """.stripMargin)
    }

    checkAnswer(
      sql("select count(*) from boolean_table"),
      Seq(Row(40))
    )
    val segments = sql("SHOW SEGMENTS FOR TABLE boolean_table")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(SegmentSequenceIds.length == 5)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE,
      CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE)
  }

  test("ENABLE_AUTO_LOAD_MERGE: false, and sort_columns is boolean") {
    sql("drop table if exists boolean_one_column")
    sql(
      s"""CREATE TABLE if not exists boolean_one_column(
         |booleanField BOOLEAN)
         |STORED AS carbondata
         |TBLPROPERTIES('sort_columns'='booleanField','SORT_SCOPE'='GLOBAL_SORT')
         |""".stripMargin)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
    sql("insert into boolean_one_column values(true)")
    sql("insert into boolean_one_column values(True)")
    sql("insert into boolean_one_column values(TRUE)")
    sql("insert into boolean_one_column values('true')")
    sql("insert into boolean_one_column values(False)")
    sql("insert into boolean_one_column values(false)")
    sql("insert into boolean_one_column values(FALSE)")
    sql("insert into boolean_one_column values('false')")
    sql("insert into boolean_one_column values('tr')")
    sql("insert into boolean_one_column values(null)")
    sql("insert into boolean_one_column values('truEe')")
    sql("insert into boolean_one_column values('falsEe')")
    sql("insert into boolean_one_column values('t')")
    sql("insert into boolean_one_column values('f')")

    checkAnswer(
      sql("select * from boolean_one_column"),
      Seq(Row(true), Row(true), Row(true), Row(true),
        Row(false), Row(false), Row(false), Row(false),
        Row(true), Row(false), Row(null), Row(null), Row(null), Row(null))
    )

    val segments = sql("SHOW SEGMENTS FOR TABLE boolean_one_column")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(!SegmentSequenceIds.contains("0.1"))
    assert(SegmentSequenceIds.length == 14)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE,
      CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE)
  }

  test("ENABLE_AUTO_LOAD_MERGE: true, and sort_columns is boolean") {
    sql("drop table if exists boolean_one_column")
    sql(
      s"""CREATE TABLE if not exists boolean_one_column(
         |booleanField BOOLEAN)
         |STORED AS carbondata
         |TBLPROPERTIES('sort_columns'='booleanField','SORT_SCOPE'='GLOBAL_SORT')
         |""".stripMargin)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    sql("insert into boolean_one_column values(true)")
    sql("insert into boolean_one_column values(True)")
    sql("insert into boolean_one_column values(TRUE)")
    sql("insert into boolean_one_column values('true')")
    sql("insert into boolean_one_column values(False)")
    sql("insert into boolean_one_column values(false)")
    sql("insert into boolean_one_column values(FALSE)")
    sql("insert into boolean_one_column values('false')")
    sql("insert into boolean_one_column values('tr')")
    sql("insert into boolean_one_column values(null)")
    sql("insert into boolean_one_column values('truEe')")
    sql("insert into boolean_one_column values('falsEe')")
    sql("insert into boolean_one_column values('t')")
    sql("insert into boolean_one_column values('f')")

    checkAnswer(
      sql("select * from boolean_one_column"),
      Seq(Row(true), Row(true), Row(true), Row(true),
        Row(false), Row(false), Row(false), Row(false),
        Row(true), Row(false), Row(null), Row(null), Row(null), Row(null))
    )

    val segments = sql("SHOW SEGMENTS FOR TABLE boolean_one_column")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
    assert(SegmentSequenceIds.length == 18)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE,
      CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE)
  }
}
