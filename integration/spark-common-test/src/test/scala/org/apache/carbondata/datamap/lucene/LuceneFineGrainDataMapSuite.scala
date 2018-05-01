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

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedDataMapCommandException}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.datamap.DataMapStoreManager

class LuceneFineGrainDataMapSuite extends QueryTest with BeforeAndAfterAll {

  val file2 = resourcesPath + "/datamap_input.csv"

  override protected def beforeAll(): Unit = {
    LuceneFineGrainDataMapSuite.createFile(file2)
    sql("create database if not exists lucene")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SYSTEM_FOLDER_LOCATION,
        CarbonEnv.getDatabaseLocation("lucene", sqlContext.sparkSession))
    sql("use lucene")
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

    sql("DROP TABLE IF EXISTS datamap_test4")

    sql(
      """
        | CREATE TABLE datamap_test4(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT', 'autorefreshdatamap' = 'false')
      """.stripMargin)
  }

  test("validate TEXT_COLUMNS DataMap property") {
    // require TEXT_COLUMNS
    var exception = intercept[MalformedDataMapCommandException](sql(
      s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test
         | USING 'lucene'
      """.stripMargin))

    assertResult("Lucene DataMap require proper TEXT_COLUMNS property.")(exception.getMessage)

    // illegal argumnet.
    exception = intercept[MalformedDataMapCommandException](sql(
      s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('text_COLUMNS'='name, ')
      """.stripMargin))

    assertResult("TEXT_COLUMNS contains illegal argument.")(exception.getMessage)

    // not exists
    exception = intercept[MalformedDataMapCommandException](sql(
      s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('text_COLUMNS'='city,school')
    """.stripMargin))

    assertResult("TEXT_COLUMNS: school does not exist in table. Please check create DataMap statement.")(exception.getMessage)

    // duplicate columns
    exception = intercept[MalformedDataMapCommandException](sql(
      s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('text_COLUMNS'='name,city,name')
      """.stripMargin))

    assertResult("TEXT_COLUMNS has duplicate columns :name")(exception.getMessage)

    // only support String DataType
    exception = intercept[MalformedDataMapCommandException](sql(
    s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('text_COLUMNS'='city,id')
      """.stripMargin))

    assertResult("TEXT_COLUMNS only supports String column. Unsupported column: id, DataType: INT")(exception.getMessage)
  }

  test("test lucene fine grain data map") {
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('TEXT_COLUMNS'='Name , cIty')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test OPTIONS('header'='false')")
    checkAnswer(sql("SELECT * FROM datamap_test WHERE TEXT_MATCH('name:n10')"), sql(s"select * from datamap_test where name='n10'"))
    checkAnswer(sql("SELECT * FROM datamap_test WHERE TEXT_MATCH('city:c020')"), sql(s"SELECT * FROM datamap_test WHERE city='c020'"))

    sql("drop datamap dm on table datamap_test")
  }

  test("test lucene refresh data map") {

    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test4 OPTIONS('header'='false')")

    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test4 OPTIONS('header'='false')")

    sql(
      s"""
         | CREATE DATAMAP dm4 ON TABLE datamap_test4
         | USING 'lucene'
         | DMProperties('TEXT_COLUMNS'='Name , cIty')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test4 OPTIONS('header'='false')")

    sql("refresh datamap dm4 ON TABLE datamap_test4")

    checkAnswer(sql("SELECT * FROM datamap_test4 WHERE TEXT_MATCH('name:n10')"), sql(s"select * from datamap_test4 where name='n10'"))
    checkAnswer(sql("SELECT * FROM datamap_test4 WHERE TEXT_MATCH('city:c020')"), sql(s"SELECT * FROM datamap_test4 WHERE city='c020'"))

    sql("drop datamap dm4 on table datamap_test4")

  }


  test("test lucene fine grain data map drop") {
    sql("DROP TABLE IF EXISTS datamap_test1")
    sql(
      """
        | CREATE TABLE datamap_test1(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm12 ON TABLE datamap_test1
         | USING 'org.apache.carbondata.datamap.lucene.LuceneFineGrainDataMapFactory'
         | DMProperties('TEXT_COLUMNS'='Name , cIty')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test1 OPTIONS('header'='false')")

    checkAnswer(sql("SELECT * FROM datamap_test1 WHERE TEXT_MATCH('name:n10')"), sql(s"select * from datamap_test1 where name='n10'"))

    intercept[Exception] {
      sql("drop datamap dm12")
    }
    val schema = DataMapStoreManager.getInstance().getDataMapSchema("dm12")
    sql("drop datamap dm12 on table datamap_test1")
    intercept[Exception] {
      val schema = DataMapStoreManager.getInstance().getDataMapSchema("dm12")
    }
    sql("DROP TABLE IF EXISTS datamap_test1")
  }

  test("test lucene fine grain data map show") {
    sql("DROP TABLE IF EXISTS datamap_test2")
    sql("DROP TABLE IF EXISTS datamap_test3")
    sql(
      """
        | CREATE TABLE datamap_test2(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm122 ON TABLE datamap_test2
         | USING 'org.apache.carbondata.datamap.lucene.LuceneFineGrainDataMapFactory'
         | DMProperties('TEXT_COLUMNS'='Name , cIty')
      """.stripMargin)

    sql(
      """
        | CREATE TABLE datamap_test3(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm123 ON TABLE datamap_test3
         | USING 'org.apache.carbondata.datamap.lucene.LuceneFineGrainDataMapFactory'
         | DMProperties('TEXT_COLUMNS'='Name , cIty')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test2 OPTIONS('header'='false')")

    checkAnswer(sql("SELECT * FROM datamap_test2 WHERE TEXT_MATCH('name:n10')"), sql(s"select * from datamap_test2 where name='n10'"))

    assert(sql("show datamap on table datamap_test2").count() == 1)
    assert(sql("show datamap").count() == 2)
    sql("DROP TABLE IF EXISTS datamap_test2")
    sql("DROP TABLE IF EXISTS datamap_test3")
  }

  test("test lucene fine grain data map for create datamap with Duplicate Columns") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    val exception_duplicate_column: Exception = intercept[MalformedDataMapCommandException] {
      sql(
        s"""
           | CREATE DATAMAP dm ON TABLE datamap_test_table
           | USING 'lucene'
           | DMProperties('TEXT_COLUMNS'='name')
      """.stripMargin)
      sql(
        s"""
           | CREATE DATAMAP dm1 ON TABLE datamap_test_table
           | USING 'lucene'
           | DMProperties('TEXT_COLUMNS'='name')
      """.stripMargin)
    }
    assert(exception_duplicate_column.getMessage
      .contains("datamap already exists on column(s)"))
    sql("drop datamap if exists dm on table datamap_test_table")
  }

  test("test lucene fine grain data map with wildcard matching ") {
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
         | USING 'lucene'
         | DMProperties('TEXT_COLUMNS'='name , city')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    checkAnswer(sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n99*')"),
      sql("select * from datamap_test_table where name like 'n99%'"))
    checkAnswer(sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n*9')"),
      sql(s"select * from datamap_test_table where name like 'n%9'"))
    sql("drop datamap if exists dm on table datamap_test_table")
  }

  test("test lucene fine grain data map with TEXT_MATCH 'AND' Filter ") {
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
         | USING 'lucene'
         | DMProperties('TEXT_COLUMNS'='name , city')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    checkAnswer(sql(
      "SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n0*') AND TEXT_MATCH(' city:c0*')"),
      sql("select * from datamap_test_table where name like 'n0%' and city like 'c0%'"))
    sql("drop datamap if exists dm on table datamap_test_table")
  }

  test("test lucene fine grain data map with TEXT_MATCH 'OR' Filter ") {
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
         | USING 'lucene'
         | DMProperties('TEXT_COLUMNS'='name , city')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    checkAnswer(sql(
      "SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n1*') or TEXT_MATCH('city:c01*')"),
      sql("select * from datamap_test_table where name like 'n1%' or city like 'c01%'"))
    sql("drop datamap if exists dm on table datamap_test_table")
  }

  test("test lucene fine grain data map with TEXT_MATCH 'AND' and 'OR' Filter ") {
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
         | USING 'lucene'
         | DMProperties('TEXT_COLUMNS'='name , city')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    checkAnswer(sql(
      "SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n1*') OR TEXT_MATCH ('city:c01*') " +
      "AND TEXT_MATCH('city:C02*')"),
      sql(
        "select * from datamap_test_table where name like 'n1%' OR city like 'c01%' and city like" +
        " 'c02%'"))
    sql("drop datamap if exists dm on table datamap_test_table")
  }

  test("test lucene fine grain data map with compaction-Major ") {
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
         | USING 'lucene'
         | DMProperties('TEXT_COLUMNS'='name , city')
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

  test("test lucene fine grain data map with compaction-Minor ") {
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
         | USING 'lucene'
         | DMProperties('TEXT_COLUMNS'='name , city')
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

  test("test lucene fine grain data map with GLOBAL_SORT_SCOPE ") {
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
         | USING 'lucene'
         | DMProperties('TEXT_COLUMNS'='name , city')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false','GLOBAL_SORT_PARTITIONS'='2')")
    checkAnswer(sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n10')"),
      sql("select * from datamap_test_table where name='n10'"))
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false','GLOBAL_SORT_PARTITIONS'='2')")
    checkAnswer(sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n10')"),
      sql("select * from datamap_test_table where name='n10'"))
    sql("drop datamap if exists dm on table datamap_test_table")
  }

  test("test lucene fine grain data map with ALTER ADD and DROP Table COLUMN on Lucene DataMap") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm2 ON TABLE datamap_test_table
         | USING 'lucene'
         | DMProperties('TEXT_COLUMNS'='name , city')
      """.stripMargin)
    val exception_add_column: Exception = intercept[MalformedCarbonCommandException] {
      sql("alter table dm2 add columns(city1 string)")
    }
    assert(exception_add_column.getMessage
      .contains("Unsupported alter operation on hive table"))
    val exception_drop_column: Exception = intercept[MalformedCarbonCommandException] {
      sql("alter table dm2 drop columns(name)")
    }
    assert(exception_drop_column.getMessage
      .contains("Unsupported alter operation on hive table"))
    sql("drop datamap if exists dm2 on table datamap_test_table")
  }
  test("test Clean Files and check Lucene DataMap") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm2 ON TABLE datamap_test_table
         | USING 'lucene'
         | DMProperties('TEXT_COLUMNS'='name , city')
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

  test("test lucene fine grain data map with TEXT_MATCH 'NOT' Filter ") {
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
         | USING 'lucene'
         | DMProperties('TEXT_COLUMNS'='name , city')
          """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    //check NOT filter with TEXTMATCH term-search
    checkAnswer(sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n0 NOT n1')"),
      sql("select *from datamap_test_table where name='n0' AND not name='n1'"))
    //check NOT filter with TEXTMATCH wildcard-search
    checkAnswer(sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n1* NOT n2*')"),
      sql("select *from datamap_test_table where name like'n1%' AND not name like 'n2%'"))
    //check NOT filter with TEXTMATCH wildcard-search using AND on different columns
    checkAnswer(sql(
      "select *from datamap_test_table where TEXT_MATCH('name:n1*')AND TEXT_MATCH('city:c01* NOT " +
      "c02*')"),
      sql("select *from datamap_test_table where name like'n1%' AND not city='c02%'"))
    sql("drop datamap if exists dm on table datamap_test_table")
  }

  test("test lucene fine grain data map for show datamaps with Preaggregate and Lucene") {
    sql("DROP TABLE IF EXISTS datamap_main")
    sql("create table datamap_main (a string, b string, c string) stored by 'carbondata'")
    sql(
      s"""
         | CREATE DATAMAP dm_lucene ON TABLE datamap_main
         | USING 'lucene'
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

  test("test lucene fine grain data map with CTAS") {
    sql("DROP TABLE IF EXISTS source_table")
    sql("DROP TABLE IF EXISTS target_table")
    sql(
      """
        | CREATE TABLE source_table(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE source_table
         | USING 'lucene'
         | DMProperties('TEXT_COLUMNS'='name,city')
          """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE source_table OPTIONS('header'='false')")
    sql(
      """
        | CREATE TABLE target_table
        | STORED BY 'carbondata'
        | AS
        | Select * from source_table where TEXT_MATCH('name:n1*')
      """.stripMargin)
    checkAnswer(sql("SELECT count(*) FROM target_table"),
      sql("select count(*) from source_table where name like 'n1%'"))
    sql("DROP TABLE IF EXISTS source_table")
    sql("DROP TABLE IF EXISTS target_table")
  }

  test("test lucene fine grain data map with text-match limit") {
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('TEXT_COLUMNS'='name , city')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test OPTIONS('header'='false')")
    checkAnswer(sql("select count(*) from datamap_test where TEXT_MATCH_WITH_LIMIT('name:n10*',10)"),Seq(Row(10)))
    checkAnswer(sql("select count(*) from datamap_test where TEXT_MATCH_WITH_LIMIT('name:n10*',50)"),Seq(Row(50)))
    sql("drop datamap dm on table datamap_test")
  }

  test("test lucene fine grain data map with InsertOverwrite") {
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('TEXT_COLUMNS'='name , city')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test OPTIONS('header'='false')")
    sql(
      """
        | CREATE TABLE table1(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql("INSERT OVERWRITE TABLE table1 select *from datamap_test where TEXT_MATCH('name:n*')")
    checkAnswer(sql("select count(*) from table1"),Seq(Row(10000)))
    sql("drop datamap dm on table datamap_test")
  }

  test("explain query with lucene datamap") {
    sql("drop table if exists main")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.BLOCKLET_SIZE, "8")
    sql(
      """
        | CREATE TABLE main(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE main
         | USING 'lucene'
         | DMProperties('TEXT_COLUMNS'='name , city')
      """.stripMargin)

    val file1 = resourcesPath + "/main.csv"
    LuceneFineGrainDataMapSuite.createFile(file1, 1000000)

    sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE main OPTIONS('header'='false')")

    sql("EXPLAIN SELECT * FROM main WHERE TEXT_MATCH('name:bob')").show(false)
    val rows = sql("EXPLAIN SELECT * FROM main WHERE TEXT_MATCH('name:bob')").collect()

    assertResult(
      """== CarbonData Profiler ==
        |Table Scan on main
        | - total blocklets: 1
        | - filter: TEXT_MATCH('name:bob')
        | - pruned by Main DataMap
        |    - skipped blocklets: 0
        | - pruned by FG DataMap
        |    - name: dm
        |    - provider: lucene
        |    - skipped blocklets: 1
        |""".stripMargin)(rows(0).getString(0))

    LuceneFineGrainDataMapSuite.deleteFile(file1)
    sql("drop datamap dm on table main")
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.BLOCKLET_SIZE, CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL)
  }

  test("test lucene datamap creation for blocked features") {
    sql("DROP TABLE IF EXISTS datamap_test7")
    sql(
      """
        | CREATE TABLE datamap_test7(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm124 ON TABLE datamap_test7
         | USING 'org.apache.carbondata.datamap.lucene.LuceneFineGrainDataMapFactory'
         | DMProperties('TEXT_COLUMNS'='Name , cIty')
      """.stripMargin)

    val ex1 = intercept[MalformedCarbonCommandException] {
      sql("alter table datamap_test7 rename to datamap_test5")
    }
    assert(ex1.getMessage.contains("alter rename is not supported"))

    val ex2 = intercept[MalformedCarbonCommandException] {
      sql("alter table datamap_test7 add columns(address string)")
    }
    assert(ex2.getMessage.contains("alter table add column is not supported"))

    val ex3 = intercept[MalformedCarbonCommandException] {
      sql("alter table datamap_test7 change id id BIGINT")
    }
    assert(ex3.getMessage.contains("alter table change datatype is not supported"))

    val ex4 = intercept[MalformedCarbonCommandException] {
      sql("alter table datamap_test7 drop columns(name)")
    }
    assert(ex4.getMessage.contains("alter table drop column is not supported"))

    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test7 OPTIONS('header'='false')")
    val ex5 = intercept[MalformedCarbonCommandException] {
      sql("UPDATE datamap_test7 d set(d.city)=('luc') where d.name='n10'").show()
    }
    assert(ex5.getMessage.contains("update operation is not supported"))

    val ex6 = intercept[MalformedCarbonCommandException] {
      sql("delete from datamap_test7 where name = 'n10'").show()
    }
    assert(ex6.getMessage.contains("delete operation is not supported"))
  }

  test("test lucene fine grain multiple data map on table") {
    sql("DROP TABLE IF EXISTS datamap_test5")
    sql(
      """
        | CREATE TABLE datamap_test5(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm2 ON TABLE datamap_test5
         | USING 'lucene'
         | DMProperties('TEXT_COLUMNS'='city')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test5
         | USING 'lucene'
         | DMProperties('TEXT_COLUMNS'='Name')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test5 OPTIONS('header'='false')")
    checkAnswer(sql("SELECT * FROM datamap_test5 WHERE TEXT_MATCH('name:n10')"),
      sql(s"select * from datamap_test5 where name='n10'"))
    checkAnswer(sql("SELECT * FROM datamap_test5 WHERE TEXT_MATCH('city:c020')"),
      sql(s"SELECT * FROM datamap_test5 WHERE city='c020'"))

  }

  override protected def afterAll(): Unit = {
    LuceneFineGrainDataMapSuite.deleteFile(file2)
    sql("DROP TABLE IF EXISTS normal_test")
    sql("DROP TABLE IF EXISTS datamap_test")
    sql("DROP TABLE IF EXISTS source_table")
    sql("DROP TABLE IF EXISTS target_table")
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql("DROP TABLE IF EXISTS datamap_test1")
    sql("DROP TABLE IF EXISTS datamap_test2")
    sql("DROP TABLE IF EXISTS datamap_test3")
    sql("DROP TABLE IF EXISTS datamap_test4")
    sql("DROP TABLE IF EXISTS datamap_test5")
    sql("DROP TABLE IF EXISTS datamap_test7")
    sql("DROP TABLE IF EXISTS datamap_main")
    sql("use default")
    sql("drop database if exists lucene cascade")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SYSTEM_FOLDER_LOCATION,
          CarbonProperties.getStorePath)
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
