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

import org.apache.spark.sql.{CarbonEnv, CarbonSession, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.datamap.status.DataMapStatusManager

/**
  * Test lucene fine grain datamap with search mode
  */
class LuceneFineGrainDataMapWithSearchModeSuite extends QueryTest with BeforeAndAfterAll {

  val file2 = resourcesPath + "/datamap_input.csv"

  override protected def beforeAll(): Unit = {
    //n should be about 5000000 of reset if size is default 1024
    val n = 500000
    sqlContext.sparkSession.asInstanceOf[CarbonSession].startSearchMode()
    CarbonProperties
      .getInstance().addProperty(CarbonCommonConstants.CARBON_SEARCH_QUERY_TIMEOUT, "100s")
    LuceneFineGrainDataMapSuite.createFile(file2, n)
    sql("create database if not exists lucene")
    sql("use lucene")
    sql("DROP TABLE IF EXISTS datamap_test")
    sql(
      """
        | CREATE TABLE datamap_test(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
  }

  test("test lucene fine grain data map with search mode") {

    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='Name')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test OPTIONS('header'='false')")
    checkAnswer(sql("SELECT * FROM datamap_test WHERE TEXT_MATCH('name:n10')"),
      sql(s"select * from datamap_test where name='n10'"))

    sql("drop datamap dm on table datamap_test")
  }

  // TODO： optimize performance
  ignore("test lucene fine grain data map with TEXT_MATCH 'AND' Filter") {
    sql("drop datamap if exists dm on table datamap_test")
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test OPTIONS('header'='false')")
    checkAnswer(
      sql("SELECT count(*) FROM datamap_test WHERE TEXT_MATCH('name:n2*') " +
        "AND age=28 and id=200149"),
      sql("SELECT count(*) FROM datamap_test WHERE name like 'n2%' " +
        "AND age=28 and id=200149"))
    sql("drop datamap if exists dm on table datamap_test")
  }

  // TODO： optimize performance
  ignore("test lucene fine grain data map with TEXT_MATCH 'AND' and 'OR' Filter ") {
    sql("drop datamap if exists dm on table datamap_test")
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='name , city')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test OPTIONS('header'='false')")
    checkAnswer(sql("SELECT * FROM datamap_test WHERE TEXT_MATCH('name:n1*') OR TEXT_MATCH ('city:c01*') " +
      "AND TEXT_MATCH('city:C02*')"),
      sql("select * from datamap_test where name like 'n1%' OR city like 'c01%' and city like" +
        " 'c02%'"))
    sql("drop datamap if exists dm on table datamap_test")
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
    sql("DROP TABLE IF EXISTS datamap_test_table")
  }

  ignore("test lucene fine grain datamap rebuild") {
    sql("DROP TABLE IF EXISTS datamap_test5")
    sql(
      """
        | CREATE TABLE datamap_test5(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
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

  test("test lucene fine grain datamap rebuild with table block size") {
    sql("DROP TABLE IF EXISTS datamap_test5")
    sql(
      """
        | CREATE TABLE datamap_test5(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'TABLE_BLOCKSIZE'='1')
      """.stripMargin)
    sql(
    s"""
         | CREATE DATAMAP dm ON TABLE datamap_test5
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='Name , cIty')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test5 OPTIONS('header'='false')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test5 OPTIONS('header'='false')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test5 OPTIONS('header'='false')")

    checkAnswer(sql("SELECT * FROM datamap_test5 WHERE TEXT_MATCH('city:c00')"),
      sql(s"SELECT * FROM datamap_test5 WHERE city='c00'"))
    checkAnswer(sql("SELECT * FROM datamap_test5 WHERE TEXT_MATCH('city:c020')"),
      sql(s"SELECT * FROM datamap_test5 WHERE city='c020'"))
    checkAnswer(sql("SELECT * FROM datamap_test5 WHERE TEXT_MATCH('city:c0100085')"),
      sql(s"SELECT * FROM datamap_test5 WHERE city='c0100085'"))
    checkAnswer(sql("SELECT * FROM datamap_test5 WHERE TEXT_MATCH('city:c09560')"),
      sql(s"SELECT * FROM datamap_test5 WHERE city='c09560'"))
    sql("DROP TABLE IF EXISTS datamap_test5")
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
         | DMProperties('INDEX_COLUMNS'='city')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm1 ON TABLE datamap_test5
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='Name')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test5 OPTIONS('header'='false')")
    checkAnswer(sql("SELECT * FROM datamap_test5 WHERE TEXT_MATCH('name:n10')"),
      sql(s"select * from datamap_test5 where name='n10'"))
    checkAnswer(sql("SELECT * FROM datamap_test5 WHERE TEXT_MATCH('city:c020')"),
      sql(s"SELECT * FROM datamap_test5 WHERE city='c020'"))
    sql("DROP TABLE IF EXISTS datamap_test5")
  }

  // TODO： support it  in the future
  ignore("test lucene datamap and validate the visible and invisible status of datamap ") {
    val tableName = "datamap_test2"
    val dataMapName1 = "ggdatamap1";
    sql(s"DROP TABLE IF EXISTS $tableName")
    sql(
      s"""
         | CREATE TABLE $tableName(id INT, name STRING, city STRING, age INT)
         | STORED BY 'org.apache.carbondata.format'
         | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    // register datamap writer
    sql(
      s"""
         | CREATE DATAMAP ggdatamap1 ON TABLE $tableName
         | USING 'lucene'
         | DMPROPERTIES('index_columns'='name')
       """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE $tableName OPTIONS('header'='false')")

    val df1 = sql(s"EXPLAIN EXTENDED SELECT * FROM $tableName WHERE TEXT_MATCH('name:n502670')").collect()
    sql(s"SELECT * FROM $tableName WHERE TEXT_MATCH('name:n502670')").show()
    println(df1(0).getString(0))
    assertResult(
      s"""== CarbonData Profiler ==
         |Table Scan on datamap_test2
         | - total blocklets: 1
         | - filter: TEXT_MATCH('name:n502670')
         | - pruned by Main DataMap
         |    - skipped blocklets: 0
         | - pruned by FG DataMap
         |    - name: ggdatamap1
         |    - provider: lucene
         |    - skipped blocklets: 1
         |""".stripMargin)(df1(0).getString(0))

    sql(s"set ${CarbonCommonConstants.CARBON_DATAMAP_VISIBLE}default.$tableName.$dataMapName1 = false")

    val df2 = sql(s"EXPLAIN EXTENDED SELECT * FROM $tableName WHERE name='n502670'").collect()
    println(df2(0).getString(0))
    assertResult(
      s"""== CarbonData Profiler ==
         |Table Scan on $tableName
         | - total blocklets: 1
         | - filter: (name <> null and name = n502670)
         | - pruned by Main DataMap
         |    - skipped blocklets: 0
         |""".stripMargin)(df2(0).getString(0))

    checkAnswer(sql(s"SELECT * FROM $tableName WHERE name='n502670' AND city='c2670'"),
      sql(s"SELECT * FROM $tableName WHERE name='n502670' AND city='c2670'"))
    sql(s"DROP TABLE IF EXISTS $tableName")
  }

  ignore("test lucene fine grain datamap rebuild with table block size, rebuild") {
    sql("DROP TABLE IF EXISTS datamap_test5")
    sql(
      """
        | CREATE TABLE datamap_test5(id INT, name STRING, city STRING, age INT)
        | STORED BY 'carbondata'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'TABLE_BLOCKSIZE'='1')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test5
         | USING 'lucene'
         | WITH DEFERRED REBUILD
         | DMProperties('INDEX_COLUMNS'='Name , cIty')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test5 OPTIONS('header'='false')")
    sql("REBUILD DATAMAP dm ON TABLE datamap_test5")

    sqlContext.sparkSession.asInstanceOf[CarbonSession].stopSearchMode()
    sql("SELECT * FROM datamap_test5 WHERE TEXT_MATCH('city:c020')").show()
    sqlContext.sparkSession.asInstanceOf[CarbonSession].startSearchMode()
    sql("SELECT * FROM datamap_test5 WHERE TEXT_MATCH('city:c020')").show()
    checkAnswer(sql("SELECT * FROM datamap_test5 WHERE TEXT_MATCH('city:c020')"),
      sql(s"SELECT * FROM datamap_test5 WHERE city='c020'"))
    sql("DROP TABLE IF EXISTS datamap_test5")
  }

  override protected def afterAll(): Unit = {
    LuceneFineGrainDataMapSuite.deleteFile(file2)
    sql("DROP TABLE IF EXISTS datamap_test")
    sql("DROP TABLE IF EXISTS datamap_test5")
    sql("use default")
    sqlContext.sparkSession.asInstanceOf[CarbonSession].stopSearchMode()
  }

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
