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

package org.apache.carbondata.datamap.lucene

import java.io.{File, PrintWriter}

import scala.util.Random

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException

class LuceneFineGrainDataMapSuite extends QueryTest with BeforeAndAfterAll {

  val file2 = resourcesPath + "/datamap_input.csv"

  override protected def beforeAll(): Unit = {
    //n should be about 5000000 of reset if size is default 1024
    val n = 15000
    LuceneFineGrainDataMapSuite.createFile(file2)
    sql("DROP TABLE IF EXISTS normal_test")
    sql(
      """
        | CREATE TABLE normal_test(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE normal_test OPTIONS('header'='false')")

    sql("DROP TABLE IF EXISTS datamap_test")
    sql(
      """
        | CREATE TABLE datamap_test(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql("DROP TABLE IF EXISTS datamap_main")
    sql("DROP TABLE IF EXISTS datamap_streaming")
    sql("DROP TABLE IF EXISTS datamap_streaming_alter")
  }

  test("test-001 validate TEXT_COLUMNS DataMap property") {
    // require TEXT_COLUMNS
    var exception = intercept[MalformedDataMapCommandException](sql(
      s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test
         | USING 'LuceneFG'
      """.stripMargin))

    assertResult("Lucene DataMap require proper TEXT_COLUMNS property.")(exception.getMessage)

    // illegal argumnet.
    exception = intercept[MalformedDataMapCommandException](sql(
      s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test
         | USING 'LuceneFG'
         | DMProperties('text_COLUMNS'='name, ')
      """.stripMargin))

    assertResult("TEXT_COLUMNS contains illegal argument.")(exception.getMessage)

    // not exists
    exception = intercept[MalformedDataMapCommandException](sql(
      s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test
         | USING 'LuceneFG'
         | DMProperties('text_COLUMNS'='city,school')
    """.stripMargin))

    assertResult(
      "TEXT_COLUMNS: school does not exist in table. Please check create DataMap statement.")(
      exception.getMessage)

    // only support String DataType
    exception = intercept[MalformedDataMapCommandException](sql(
      s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test
         | USING 'LuceneFG'
         | DMProperties('text_COLUMNS'='city,id')
      """.stripMargin))

    assertResult("TEXT_COLUMNS only supports String column. Unsupported column: id, DataType: INT")(
      exception.getMessage)
  }

  test("test-002 lucene fine grain data map") {
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test
         | USING 'LuceneFG'
         | DMProperties('TEXT_COLUMNS'='Name , cIty')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test OPTIONS('header'='false')")
    checkAnswer(sql("SELECT * FROM datamap_test WHERE TEXT_MATCH('name:n10')"),
      sql(s"select * from datamap_test where name='n10'"))
    checkAnswer(sql("SELECT * FROM datamap_test WHERE TEXT_MATCH('city:c020')"),
      sql(s"SELECT * FROM datamap_test WHERE city='c020'"))
  }

  test("test-003 lucene fine grain data map for create datamap with Duplicate Columns") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    val exception_duplicate_column: Exception = intercept[Exception] {
      sql(
        s"""
           | CREATE DATAMAP dm ON TABLE datamap_test_table
           | USING 'LuceneFG'
           | DMProperties('TEXT_COLUMNS'='name')
      """.stripMargin)
      sql(
        s"""
           | CREATE DATAMAP dm1 ON TABLE datamap_test_table
           | USING 'LuceneFG'
           | DMProperties('TEXT_COLUMNS'='name')
      """.stripMargin)
    }
    assert(exception_duplicate_column.getMessage
      .contains("Create lucene datamap dm1 failed, datamap already exists on column(s) name"))
    sql("drop datamap if exists dm on table datamap_test_table")
  }

  test("test-004 lucene fine grain data map for show datamap") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test_table
         | USING 'LuceneFG'
         | DMProperties('TEXT_COLUMNS'='name')
      """.stripMargin)
    checkExistence(sql("show datamap on table datamap_test_table"), true, "dm")
    sql(
      s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test_table
         | USING 'LuceneFG'
         | DMProperties('TEXT_COLUMNS'='city')
      """.stripMargin)
    checkExistence(sql("show datamap on table datamap_test_table"), true, "dm1")
    sql("drop datamap if exists dm on table datamap_test_table")
    sql("drop datamap if exists dm1 on table datamap_test_table")
  }

  test("test-005 lucene fine grain data map with wildcard matching ") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test_table
         | USING 'LuceneFG'
         | DMProperties('TEXT_COLUMNS'='Name , cIty')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    checkAnswer(sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n99*')"),
      sql("select * from datamap_test_table where name like 'n99%'"))
    checkAnswer(sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n*9')"),
      sql(s"select * from datamap_test_table where name like 'n%9'"))
    sql("drop datamap if exists dm on table datamap_test_table")
  }

  test("test-006 lucene fine grain data map with TEXT_MATCH 'AND' Filter ") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test_table
         | USING 'LuceneFG'
         | DMProperties('TEXT_COLUMNS'='Name , cIty')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    checkAnswer(sql(
      "SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n0*') AND TEXT_MATCH(' city:c0*')"),
      sql("select * from datamap_test_table where name like 'n0%' and city like 'c0%'"))
    sql("drop datamap if exists dm on table datamap_test_table")
  }

  test("test-007 lucene fine grain data map with TEXT_MATCH 'OR' Filter ") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test_table
         | USING 'LuceneFG'
         | DMProperties('TEXT_COLUMNS'='Name , cIty')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    checkAnswer(sql(
      "SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n1*') or TEXT_MATCH('city:c01*')"),
      sql("select * from datamap_test_table where name like 'n1%' or city like 'c01%'"))
    sql("drop datamap if exists dm on table datamap_test_table")
  }

  test("test-008 lucene fine grain data map with TEXT_MATCH 'AND' and 'OR' Filter ") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test_table
         | USING 'LuceneFG'
         | DMProperties('TEXT_COLUMNS'='Name , cIty')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    checkAnswer(sql(
      "SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n1*') OR TEXT_MATCH ('city:c01*') " +
      "AND TEXT_MATCH('CITY:C02*')"),
      sql(
        "select * from datamap_test_table where name like 'n1%' OR city like 'c01%' and city like" +
        " 'c02%'"))
    sql("drop datamap if exists dm on table datamap_test_table")
  }

  test("test-009 lucene fine grain data map with compaction-Major ") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test_table
         | USING 'LuceneFG'
         | DMProperties('TEXT_COLUMNS'='Name , cIty')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    checkAnswer(sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n10')"),
      sql("select * from datamap_test_table where name='n10'"))
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    sql("alter table datamap_test_table compact 'major'")
    checkAnswer(sql("SELECT COUNT(*) FROM datamap_test_table WHERE TEXT_MATCH('name:n10')"),
      sql("select COUNT(*) from datamap_test_table where name='n10'"))
    sql("drop datamap if exists dm on table datamap_test_table")
  }

  test("test-010 lucene fine grain data map with compaction-Minor ") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test_table
         | USING 'LuceneFG'
         | DMProperties('TEXT_COLUMNS'='Name , cIty')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    checkAnswer(sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n10')"),
      sql("select * from datamap_test_table where name='n10'"))
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    sql("alter table datamap_test_table compact 'minor'")
    checkAnswer(sql("SELECT COUNT(*) FROM datamap_test_table WHERE TEXT_MATCH('name:n10')"),
      sql("select count(*) from datamap_test_table where name='n10'"))
    sql("drop datamap if exists dm on table datamap_test_table")
  }

  test("test-011 lucene fine grain data map with GLOBAL_SORT_SCOPE ") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test_table
         | USING 'LuceneFG'
         | DMProperties('TEXT_COLUMNS'='Name , cIty')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    checkAnswer(sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n10')"),
      sql("select * from datamap_test_table where name='n10'"))
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    checkAnswer(sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n10')"),
      sql("select * from datamap_test_table where name='n10'"))
    sql("drop datamap if exists dm on table datamap_test_table")
  }

  test("test-012 lucene fine grain data map with Partition table") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """create table datamap_test_table (c1 string,c2 int,c3 string) PARTITIONED BY(c5 string)
        |STORED
        | BY 'org.apache.carbondata.format'""".stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test_table
         | USING 'LuceneFG'
         | DMProperties('TEXT_COLUMNS'='c1 , c3')
      """.stripMargin)
    sql("insert into datamap_test_table select 'abc',1,'aa','bb")
    sql("insert into datamap_test_table select 'def',2,'dd','ee")
    checkAnswer(sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('c1:abc')"),
      sql("select * from datamap_test_table where c1='abc'"))
  }

  test("test-013 lucene fine grain data map with ALTER ADD and DROP Table COLUMN on Lucene DataMap")
  {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm2 ON TABLE datamap_test_table
         | USING 'LuceneFG'
         | DMProperties('TEXT_COLUMNS'='Name')
      """.stripMargin)
    val exception_add_column: Exception = intercept[Exception] {
      sql("alter table dm2 add columns(city1 string)")
    }
    assert(exception_add_column.getMessage
      .contains("Unsupported alter operation on hive table"))
    val exception_drop_column: Exception = intercept[Exception] {
      sql("alter table dm2 drop columns(Name)")
    }
    assert(exception_drop_column.getMessage
      .contains("Unsupported alter operation on hive table"))
    sql("drop datamap if exists dm2 on table datamap_test_table")
  }

  test("test-014 Drop Lucene Datamap") {
    sql("DROP TABLE IF EXISTS datamap_testt")
    sql(
      """
        | CREATE TABLE datamap_testt(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name','SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_testt
         | USING 'LuceneFG'
         | DMProperties('TEXT_COLUMNS'='Name')
      """.stripMargin)
    sql("drop datamap if exists dm on table datamap_testt")
    checkExistence(sql("show datamap on table datamap_testt"), false, "dm")
  }

  test("test-015 Clean Files and check Lucene DataMap") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm2 ON TABLE datamap_test_table
         | USING 'LuceneFG'
         | DMProperties('TEXT_COLUMNS'='Name')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    checkAnswer(sql("SELECT count(*) FROM datamap_test_table WHERE TEXT_MATCH('name:n99*')"),
      sql("select count(*) from datamap_test_table where name like 'n99%'"))
    sql("delete from table datamap_test_table where SEGMENT.ID in (0) ")
    checkAnswer(sql("SELECT count(*) FROM datamap_test_table WHERE TEXT_MATCH('name:n99*')"),
      sql("select count(*) from datamap_test_table where name like 'n99%'"))
    sql("clean files for table datamap_test_table")
    sql("drop datamap if exists dm2 on table datamap_test_table")
  }

  test("test-016 Alter table Rename main Table") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm2 ON TABLE datamap_test_table
         | USING 'LuceneFG'
         | DMProperties('TEXT_COLUMNS'='Name')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    val exception_rename_column: Exception = intercept[Exception] {
      sql("alter table datamap_test_table rename to dt")
    }
    assert(exception_rename_column.getMessage
      .contains("Rename operation is not supported for table with Lucene DataMap"))
    sql("drop datamap if exists dm2 on table datamap_test_table")
  }

  test("test-017 Alter table Add column to main Table") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm2 ON TABLE datamap_test_table
         | USING 'LuceneFG'
         | DMProperties('TEXT_COLUMNS'='Name')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    val exception_add_column: Exception = intercept[Exception] {
      sql("alter table datamap_test_table add columns(date string)")
    }
    assert(exception_add_column.getMessage
      .contains("Add column operation is not supported for table with Lucene DataMap"))
    sql("drop datamap if exists dm2 on table datamap_test_table")
  }

  test("test-018 Alter table Drop column in main Table") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm2 ON TABLE datamap_test_table
         | USING 'LuceneFG'
         | DMProperties('TEXT_COLUMNS'='Name')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    val exception_drop_column: Exception = intercept[Exception] {
      sql("alter table datamap_test_table drop columns(id)")
    }
    assert(exception_drop_column.getMessage
      .contains("Drop column operation is not supported for table with Lucene DataMap"))
    sql("drop datamap if exists dm2 on table datamap_test_table")
  }

  test("test-019 Alter table Change DataType for column in main Table") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm2 ON TABLE datamap_test_table
         | USING 'LuceneFG'
         | DMProperties('TEXT_COLUMNS'='Name')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    val exception_changetype_column: Exception = intercept[Exception] {
      sql("alter table datamap_test_table change id id BIGINT")
    }
    assert(exception_changetype_column.getMessage
      .contains("Change DataType operation is not supported for table with Lucene DataMap"))
    sql("drop datamap if exists dm2 on table datamap_test_table")
  }

  test("test-020 Streaming") {
    sql("DROP TABLE IF EXISTS datamap_test_streaming")
    sql("DROP TABLE IF EXISTS datamap_test_streaming_alter")
    val exception_streaming: Exception = intercept[Exception] {
      sql(
        """
          | CREATE TABLE datamap_test_streaming(id INT, name STRING, city STRING, age INT)
          | STORED BY 'carbondata'
          | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT','streaming'='true')
        """.stripMargin)
      sql(
        s"""
           | CREATE DATAMAP dm2 ON TABLE datamap_test_streaming
           | USING 'LuceneFG'
           | DMProperties('TEXT_COLUMNS'='Name')
      """.stripMargin)
    }
    assert(exception_streaming.getMessage
      .contains("Streaming table does not support creating LuceneFG"))
    sql(
      """
        | CREATE TABLE datamap_test_streaming_alter(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP datamap ON TABLE datamap_test_streaming_alter
         | USING 'LuceneFG'
         | DMProperties('TEXT_COLUMNS'='Name')
      """.stripMargin)
    val exception_streaming_alter: Exception = intercept[Exception] {
      sql("alter table datamap_test_streaming_alter SET TBLPROPERTIES('STREAMING'='TRUE')")
    }
    assert(exception_streaming_alter.getMessage
      .contains("The table has 'Lucene' DataMap, it doesn't support streaming"))
    sql("drop datamap if exists dm2 on table datamap_test_streaming")
    sql("drop datamap if exists datamap on table datamap_test_streaming_alter")
  }

  test("test-021 lucene fine grain data map with TEXT_MATCH 'NOT' Filter ") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test_table
         | USING 'LuceneFG'
         | DMProperties('TEXT_COLUMNS'='Name , cIty')
          """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    //check NOT filter with TEXTMATCH term-search
    checkAnswer(sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n0 NOT n1')"),
      sql("select *from datamap_test_table where NAME='n0' AND not NAME='n1'"))
    //check NOT filter with TEXTMATCH wildcard-search
    checkAnswer(sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n1* NOT n2*')"),
      sql("select *from datamap_test_table where NAME like'n1%' AND not NAME like 'n2%'"))
    //check NOT filter with TEXTMATCH wildcard-search using AND on different columns
    checkAnswer(sql(
      "select *from datamap_test_table where TEXT_MATCH('name:n1*')AND TEXT_MATCH('city:c01* NOT " +
      "c02*')"),
      sql("select *from datamap_test_table where NAME like'n1%' AND not city='c02%'"))
    sql("drop datamap if exists dm on table datamap_test_table")
  }

  test("test-022 lucene fine grain data map for show datamaps with Preaggregate and Lucene") {
    sql("DROP TABLE IF EXISTS datamap_main")
    sql("create table datamap_main (a string, b string, c string) stored by 'carbondata'")
    sql(
      s"""
         | CREATE DATAMAP dm_lucene ON TABLE datamap_main
         | USING 'LuceneFG'
         | DMProperties('TEXT_COLUMNS'='c')
      """.stripMargin)
    sql(
      "create datamap dm_pre on table datamap_main USING 'preaggregate' as select a,sum(b) " +
      "from datamap_main group by a")
    checkExistence(sql("show datamap on table datamap_main"), true, "dm_pre")
    checkExistence(sql("show datamap on table datamap_main"), true, "dm_lucene")
    sql("drop datamap if exists dm_pre on table datamap_main")
    sql("drop datamap if exists dm_lucene on table datamap_main")
  }

  override protected def afterAll(): Unit = {
    LuceneFineGrainDataMapSuite.deleteFile(file2)
    sql("DROP TABLE IF EXISTS normal_test")
    sql("DROP TABLE IF EXISTS datamap_test")
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql("DROP TABLE IF EXISTS datamap_main")
    sql("DROP TABLE IF EXISTS datamap_streaming")
    sql("DROP TABLE IF EXISTS datamap_streaming_alter")
  }
}

object LuceneFineGrainDataMapSuite {
  def createFile(fileName: String, line: Int = 10000, start: Int = 0) = {
    val write = new PrintWriter(new File(fileName))
    for (i <- start until (start + line)) {
      write.println(i + "," + "n" + i + "," + "c0" + i + "," + Random.nextInt(80))
    }
    write.close()
  }

  def deleteFile(fileName: String): Unit = {
    val file = new File(fileName)
    if (file.exists()) {
      file.delete()
    }
  }
}