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

import org.apache.spark.SparkException
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedDataMapCommandException}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datamap.status.DataMapStatusManager

class LuceneFineGrainDataMapSuite extends QueryTest with BeforeAndAfterAll {

  val originDistributedDatamapStatus = CarbonProperties.getInstance().getProperty(
    CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP,
    CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP_DEFAULT
  )
  val file2 = resourcesPath + "/datamap_input.csv"

  override protected def beforeAll(): Unit = {
    sql("drop database if exists lucene cascade")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "true")
    new File(CarbonProperties.getInstance().getSystemFolderLocation).delete()
    LuceneFineGrainDataMapSuite.createFile(file2)
    sql("create database if not exists lucene")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP, "true")
    sql("use lucene")
    sql("DROP TABLE IF EXISTS normal_test")
    sql(
      """
        | CREATE TABLE normal_test(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE normal_test OPTIONS('header'='false')")

    sql("DROP TABLE IF EXISTS datamap_test")
    sql(
      """
        | CREATE TABLE datamap_test(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test OPTIONS('header'='false')")
  }

  test("validate INDEX_COLUMNS DataMap property") {
    // require INDEX_COLUMNS
    var exception = intercept[MalformedDataMapCommandException](sql(
      s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test
         | USING 'lucene'
      """.stripMargin))

    assert(exception.getMessage.contains("INDEX_COLUMNS DMPROPERTY is required"))

    // illegal argumnet.
    exception = intercept[MalformedDataMapCommandException](sql(
      s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name, ')
      """.stripMargin))

    assertResult("column '' does not exist in table. Please check create DataMap statement.")(exception.getMessage)

    // not exists
    exception = intercept[MalformedDataMapCommandException](sql(
      s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='city,school')
    """.stripMargin))

    assertResult("column 'school' does not exist in table. Please check create DataMap statement.")(exception.getMessage)

    // duplicate columns
    exception = intercept[MalformedDataMapCommandException](sql(
      s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name,city,name')
      """.stripMargin))

    assertResult("INDEX_COLUMNS has duplicate column")(exception.getMessage)

    // only support String DataType
    exception = intercept[MalformedDataMapCommandException](sql(
    s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='city,id')
      """.stripMargin))

    assertResult("Only String column is supported, column 'id' is INT type. ")(exception.getMessage)
  }

  test("test lucene fine grain data map") {
    sql("drop datamap if exists dm on table datamap_test")
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='Name , cIty')
      """.stripMargin)
    checkAnswer(sql("SELECT * FROM datamap_test WHERE TEXT_MATCH('name:n10')"), sql(s"select * from datamap_test where name='n10'"))
    checkAnswer(sql("SELECT * FROM datamap_test WHERE TEXT_MATCH('city:c020')"), sql(s"SELECT * FROM datamap_test WHERE city='c020'"))

    sql("drop datamap dm on table datamap_test")
  }

  // for CARBONDATA-2820, we will first block deferred rebuild for lucene
  test("test block rebuild for lucene") {
    val deferredRebuildException = intercept[MalformedDataMapCommandException] {
      sql(
        s"""
           | CREATE DATAMAP dm ON TABLE datamap_test
           | USING 'lucene'
           | WITH DEFERRED REBUILD
           | DMProperties('INDEX_COLUMNS'='city')
      """.stripMargin)
    }
    assert(deferredRebuildException.getMessage.contains(
      s"DEFERRED REBUILD is not supported on this datamap dm with provider lucene"))

    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='city')
      """.stripMargin)
    val exception = intercept[MalformedDataMapCommandException] {
      sql(s"REBUILD DATAMAP dm ON TABLE datamap_test")
    }
    sql("drop datamap dm on table datamap_test")
    assert(exception.getMessage.contains("Non-lazy datamap dm does not support rebuild"))
  }

  ignore("test lucene rebuild data map") {
    sql("DROP TABLE IF EXISTS datamap_test4")
    sql(
      """
        | CREATE TABLE datamap_test4(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test4 OPTIONS('header'='false')")

    sql(
      s"""
         | CREATE DATAMAP dm4 ON TABLE datamap_test4
         | USING 'lucene'
         | WITH DEFERRED REBUILD
         | DMProperties('INDEX_COLUMNS'='name , city')
      """.stripMargin)

    sql("REBUILD DATAMAP dm4 ON TABLE datamap_test4")

    checkAnswer(sql("SELECT * FROM datamap_test4 WHERE TEXT_MATCH('name:n10')"), sql(s"select * from datamap_test where name='n10'"))
    checkAnswer(sql("SELECT * FROM datamap_test4 WHERE TEXT_MATCH('city:c020')"), sql(s"SELECT * FROM datamap_test4 WHERE city='c020'"))

    sql("drop table datamap_test4")
  }

  test("test lucene fine grain data map drop") {
    sql("DROP TABLE IF EXISTS datamap_test1")
    sql(
      """
        | CREATE TABLE datamap_test1(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm12 ON TABLE datamap_test1
         | USING 'org.apache.carbondata.datamap.lucene.LuceneFineGrainDataMapFactory'
         | DMProperties('INDEX_COLUMNS'='Name , cIty')
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
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm122 ON TABLE datamap_test2
         | USING 'org.apache.carbondata.datamap.lucene.LuceneFineGrainDataMapFactory'
         | DMProperties('INDEX_COLUMNS'='Name , cIty')
      """.stripMargin)

    sql(
      """
        | CREATE TABLE datamap_test3(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm123 ON TABLE datamap_test3
         | USING 'org.apache.carbondata.datamap.lucene.LuceneFineGrainDataMapFactory'
         | DMProperties('INDEX_COLUMNS'='Name , cIty')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test2 OPTIONS('header'='false')")

    checkAnswer(sql("SELECT * FROM datamap_test2 WHERE TEXT_MATCH('name:n10')"), sql(s"select * from datamap_test2 where name='n10'"))

    assert(sql("show datamap on table datamap_test2").count() == 1)
    // assert(sql("show datamap").count() == 2)
    sql("DROP TABLE IF EXISTS datamap_test2")
    sql("DROP TABLE IF EXISTS datamap_test3")
  }

  test("test lucene fine grain data map with wildcard matching ") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test_table
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name , city')
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
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test_table
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name , city')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    checkAnswer(sql(
      "SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n0* AND city:c0*')"),
      sql("select * from datamap_test_table where name like 'n0%' and city like 'c0%'"))
    sql("drop datamap if exists dm on table datamap_test_table")
  }

  test("test lucene fine grain data map with TEXT_MATCH 'OR' Filter ") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test_table
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name , city')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    checkAnswer(sql(
      "SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n1* OR city:c01*')"),
      sql("select * from datamap_test_table where name like 'n1%' or city like 'c01%'"))
    sql("drop datamap if exists dm on table datamap_test_table")
  }

  test("test lucene fine grain data map with TEXT_MATCH 'AND' and 'OR' Filter ") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test_table
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name , city')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    checkAnswer(sql(
      "SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n1* OR (city:c01* AND city:c02*)')"),
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
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test_table
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name , city')
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
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test_table
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name , city')
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
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT', 'CACHE_LEVEL'='BLOCKLET')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test_table
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name,city')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false','GLOBAL_SORT_PARTITIONS'='2')")
    checkAnswer(sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n10')"),
      sql("select * from datamap_test_table where name='n10'"))
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false','GLOBAL_SORT_PARTITIONS'='2')")
    checkAnswer(sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n10')"),
      sql("select * from datamap_test_table where name='n10'"))
    sql("DROP TABLE IF EXISTS datamap_test_table")
  }

  test("test Clean Files and check Lucene DataMap") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm2 ON TABLE datamap_test_table
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name , city')
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
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test_table
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name , city')
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
      "select *from datamap_test_table where TEXT_MATCH('name:n1* AND city:c01* NOT " +
      "c02*')"),
      sql("select *from datamap_test_table where name like'n1%' AND not city='c02%'"))
    sql("drop datamap if exists dm on table datamap_test_table")
  }

  test("test lucene fine grain data map with CTAS") {
    sql("DROP TABLE IF EXISTS source_table")
    sql("DROP TABLE IF EXISTS target_table")
    sql(
      """
        | CREATE TABLE source_table(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE source_table
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name,city')
          """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE source_table OPTIONS('header'='false')")
    sql(
      """
        | CREATE TABLE target_table
        | STORED AS carbondata
        | AS
        | Select * from source_table where TEXT_MATCH('name:n1*')
      """.stripMargin)
    checkAnswer(sql("SELECT count(*) FROM target_table"),
      sql("select count(*) from source_table where name like 'n1%'"))
    sql("DROP TABLE IF EXISTS source_table")
    sql("DROP TABLE IF EXISTS target_table")
  }

  test("test lucene fine grain data map with text-match limit") {
    sql("DROP TABLE IF EXISTS datamap_test_limit")
    sql(
      """
        | CREATE TABLE datamap_test_limit(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test_limit
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name , city')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_limit OPTIONS('header'='false')")
    checkAnswer(sql("select count(*) from datamap_test_limit where TEXT_MATCH_WITH_LIMIT('name:n10*',10)"),Seq(Row(10)))
    checkAnswer(sql("select count(*) from datamap_test_limit where TEXT_MATCH_WITH_LIMIT('name:n10*',50)"),Seq(Row(50)))
    sql("drop datamap dm on table datamap_test_limit")
  }

  test("test lucene fine grain data map with InsertOverwrite") {
    sql("DROP TABLE IF EXISTS datamap_test_overwrite")
    sql(
      """
        | CREATE TABLE datamap_test_overwrite(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test_overwrite
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name , city')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_overwrite OPTIONS('header'='false')")
    sql(
      """
        | CREATE TABLE table1(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql("INSERT OVERWRITE TABLE table1 select *from datamap_test_overwrite where TEXT_MATCH('name:n*')")
    checkAnswer(sql("select count(*) from table1"),Seq(Row(10000)))
    sql("drop datamap dm on table datamap_test_overwrite")
  }

  test("explain query with lucene datamap") {
    sql("drop table if exists main")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.BLOCKLET_SIZE, "8")
    sql(
      """
        | CREATE TABLE main(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'CACHE_LEVEL'='BLOCKLET')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE main
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name , city')
      """.stripMargin)

    val file1 = resourcesPath + "/main.csv"
    LuceneFineGrainDataMapSuite.createFile(file1, 1000000)

    sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE main OPTIONS('header'='false')")

    sql("EXPLAIN SELECT * FROM main WHERE TEXT_MATCH('name:bob')").show(false)
    val rows = sql("EXPLAIN SELECT * FROM main WHERE TEXT_MATCH('name:bob')").collect()
    // sometimes the plan comparison is failing even in case of both the plan being same.
    // once the failure happens the dropped datamap is not getting executed
    // and due to this other test cases also failing.
    try {
      assertResult(
        """== CarbonData Profiler ==
          |Table Scan on main
          | - total: 1 blocks, 1 blocklets
          | - filter: TEXT_MATCH('name:bob')
          | - pruned by Main DataMap
          |    - skipped: 0 blocks, 0 blocklets
          | - pruned by FG DataMap
          |    - name: dm
          |    - provider: lucene
          |    - skipped: 1 blocks, 1 blocklets
          |""".stripMargin)(rows(0).getString(0))
    } finally {
      LuceneFineGrainDataMapSuite.deleteFile(file1)
      sql("drop datamap dm on table main")
      CarbonProperties.getInstance().addProperty(
        CarbonCommonConstants.BLOCKLET_SIZE, CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL)
    }
  }

  test("test lucene datamap creation for blocked features") {
    sql("DROP TABLE IF EXISTS datamap_test7")
    sql(
      """
        | CREATE TABLE datamap_test7(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm124 ON TABLE datamap_test7
         | USING 'org.apache.carbondata.datamap.lucene.LuceneFineGrainDataMapFactory'
         | DMProperties('INDEX_COLUMNS'='name , city')
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
    val ex5 = intercept[UnsupportedOperationException] {
      sql("UPDATE datamap_test7 d set(d.city)=('luc') where d.name='n10'").show()
    }
    assert(ex5.getMessage.contains("Update operation is not supported"))

    val ex6 = intercept[UnsupportedOperationException] {
      sql("delete from datamap_test7 where name = 'n10'").show()
    }
    assert(ex6.getMessage.contains("Delete operation is not supported"))

    val ex7 = intercept[MalformedCarbonCommandException] {
      sql("alter table datamap_test7 change id test int")
    }
    assert(ex7.getMessage.contains("alter table column rename is not supported"))
  }

  ignore("test lucene fine grain multiple data map on table") {
    sql("DROP TABLE IF EXISTS datamap_test5")
    sql(
      """
        | CREATE TABLE datamap_test5(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT', 'CACHE_LEVEL'='BLOCKLET')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm_city ON TABLE datamap_test5
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='city')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm_name ON TABLE datamap_test5
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='Name')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test5 OPTIONS('header'='false')")
    checkAnswer(sql("SELECT * FROM datamap_test5 WHERE TEXT_MATCH('name:n10')"),
      sql(s"select * from datamap_test5 where name='n10'"))
    checkAnswer(sql("SELECT * FROM datamap_test5 WHERE TEXT_MATCH('city:c020')"),
      sql(s"SELECT * FROM datamap_test5 WHERE city='c020'"))

    var explainString = sql("explain select * from datamap_test5 where TEXT_MATCH('name:n10')").collect()
    assert(explainString(0).getString(0).contains(
      "pruned by FG DataMap\n    - name: dm_name\n    - provider: lucene"))

    sql("DROP TABLE IF EXISTS datamap_test5")
  }

  ignore("test lucene fine grain datamap rebuild") {
    sql("DROP TABLE IF EXISTS datamap_test5")
    sql(
      """
        | CREATE TABLE datamap_test5(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test5
         | USING 'lucene'
         | WITH DEFERRED REBUILD
         | DMProperties('INDEX_COLUMNS'='city')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test5 OPTIONS('header'='false')")
    val map = DataMapStatusManager.readDataMapStatusMap()
    assert(!map.get("dm").isEnabled)
    sql("REBUILD DATAMAP dm ON TABLE datamap_test5")
    checkAnswer(sql("SELECT * FROM datamap_test5 WHERE TEXT_MATCH('city:c020')"),
      sql(s"SELECT * FROM datamap_test5 WHERE city='c020'"))
    sql("DROP TABLE IF EXISTS datamap_test5")
  }

  ignore("test text_match on normal table") {
    sql("DROP TABLE IF EXISTS table1")
    sql(
      """
        | CREATE TABLE table1(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE table1 OPTIONS('header'='false')")
    val msg = intercept[SparkException] {
      sql("select * from table1 where TEXT_MATCH('name:n*')").show()
    }
    assert(msg.getCause.getMessage.contains("TEXT_MATCH is not supported on table"))
    sql("DROP TABLE table1")
  }

  test("test lucene with flush_cache as true") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm_flush ON TABLE datamap_test_table
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name , city', 'flush_cache'='true')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    checkAnswer(sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n99*')"),
      sql("select * from datamap_test_table where name like 'n99%'"))
    checkAnswer(sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n*9')"),
      sql(s"select * from datamap_test_table where name like 'n%9'"))
    sql("drop datamap if exists dm_flush on table datamap_test_table")
  }

  test("test lucene with split_blocklet as false ") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm_split_false ON TABLE datamap_test_table
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name , city', 'split_blocklet'='false')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    checkAnswer(sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n99*')"),
      sql("select * from datamap_test_table where name like 'n99%'"))
    checkAnswer(sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n*9')"),
      sql(s"select * from datamap_test_table where name like 'n%9'"))
    sql("drop datamap if exists dm_split_false on table datamap_test_table")
  }

  test("test text_match filters with more than one text_match udf ") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm_text ON TABLE datamap_test_table
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name , city')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_table OPTIONS('header'='false')")
    val msg = intercept[MalformedCarbonCommandException] {
      sql("SELECT * FROM datamap_test_table WHERE TEXT_MATCH('name:n0*') AND TEXT_MATCH" +
          "('city:c0*')").show()
    }
    assert(msg.getMessage
      .contains("Specify all search filters for Lucene within a single text_match UDF"))
    sql("drop datamap if exists dm_text on table datamap_test_table")
  }

  test("test lucene indexing english stop words") {
    sql("drop table if exists table_stop")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_LUCENE_INDEX_STOP_WORDS, "false")
    sql("create table table_stop(suggestion string,goal string) STORED AS carbondata TBLPROPERTIES('CACHE_LEVEL'='BLOCKLET')")
    sql(
      "create datamap stop_dm on table table_stop using 'lucene' DMPROPERTIES('index_columns'='suggestion')")
    sql("insert into table_stop select 'The is the stop word','abcde'")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_LUCENE_INDEX_STOP_WORDS, "true")
    sql("insert into table_stop select 'The is one more stop word','defg'")
    assert(
      sql("select * from table_stop where text_match('suggestion:*is*')").collect().length == 1)
  }

  test("test lucene data map on null values") {
    sql("DROP TABLE IF EXISTS datamap_test4")
    sql("DROP TABLE IF EXISTS datamap_copy")
    sql(
      """
        | CREATE TABLE datamap_test4(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT',
        | 'CACHE_LEVEL'='BLOCKLET')
      """.stripMargin)
    sql(
      """
        | CREATE TABLE datamap_copy(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT',
        | 'CACHE_LEVEL'='BLOCKLET')
      """.stripMargin)
    sql("insert into datamap_test4 select 1,'name','city',20")
    sql("insert into datamap_test4 select 2,'name1','city1',20")
    sql("insert into datamap_test4 select 25,cast(null as string),'city2',NULL")
    sql("insert into datamap_copy select * from datamap_test4")
    sql(
      s"""
         | CREATE DATAMAP dm4 ON TABLE datamap_test4
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name , city')
      """.stripMargin)
    checkAnswer(sql("SELECT * FROM datamap_test4 WHERE TEXT_MATCH('name:n*')"),
      sql(s"select * from datamap_copy where name like '%n%'"))
    sql("drop table datamap_test4")
    sql("drop table datamap_copy")
  }

  test("test create datamap: unable to create same index datamap for one column") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    val exception_duplicate_column: Exception = intercept[MalformedDataMapCommandException] {
      sql(
        s"""
           | CREATE DATAMAP dm ON TABLE datamap_test_table
           | USING 'lucene'
           | DMProperties('INDEX_COLUMNS'='name')
      """.stripMargin)
      sql(
        s"""
           | CREATE DATAMAP dm1 ON TABLE datamap_test_table
           | USING 'lucene'
           | DMProperties('INDEX_COLUMNS'='name')
      """.stripMargin)
    }
    assertResult("column 'name' already has lucene index datamap created")(exception_duplicate_column.getMessage)
    sql("drop table if exists datamap_test_table")
  }

  test("test create datamap: able to create different index datamap for one column") {
    sql("DROP TABLE IF EXISTS datamap_test_table")
    sql(
      """
        | CREATE TABLE datamap_test_table(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test_table
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test_table
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='name')
      """.stripMargin)
    sql("show datamap on table datamap_test_table").show(false)
    checkExistence(sql("show datamap on table datamap_test_table"), true, "dm", "dm1", "lucene", "bloomfilter")
    sql("drop table if exists datamap_test_table")
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
    sql("DROP TABLE IF EXISTS table_stop")
    sql("use default")
    sql("drop database if exists lucene cascade")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_LUCENE_INDEX_STOP_WORDS,
        CarbonCommonConstants.CARBON_LUCENE_INDEX_STOP_WORDS_DEFAULT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP,
        originDistributedDatamapStatus)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS,
        CarbonCommonConstants.ENABLE_QUERY_STATISTICS_DEFAULT)
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
