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

package org.apache.carbondata.integration.spark.testsuite.preaggregate

import org.apache.spark.sql.Row
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.Spark2TestQueryExecutor
import org.apache.spark.util.SparkUtil4Test
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.util.CarbonSparkQueryTest

class TestPreAggregateLoad extends CarbonSparkQueryTest with BeforeAndAfterAll with BeforeAndAfterEach{

  val testData = s"$resourcesPath/sample.csv"
  val p1 = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
      CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)

  override def beforeAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)

    SparkUtil4Test.createTaskMockUp(sqlContext)
    sql("DROP TABLE IF EXISTS maintable")
  }

  override protected def afterAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE,
        CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE)
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, p1)
    sql("DROP TABLE IF EXISTS y ")
    sql("DROP TABLE IF EXISTS maintable")
    sql("DROP TABLE IF EXISTS maintbl")
    sql("DROP TABLE IF EXISTS main_table")
  }

  override protected def beforeEach(): Unit = {
    sql("DROP TABLE IF EXISTS main_table")
    sql("DROP TABLE IF EXISTS segmaintable")
  }

  private def createAllAggregateTables(parentTableName: String, columnName: String = "age"): Unit = {
    sql(
      s"""
         | create datamap preagg_sum
         | on table $parentTableName
         | using 'preaggregate'
         | as select id,sum($columnName)
         | from $parentTableName
         | group by id
       """.stripMargin)
    sql(
      s"""create datamap preagg_avg on table $parentTableName using 'preaggregate' as select id,avg($columnName) from $parentTableName group by id"""
        .stripMargin)
    sql(
      s"""create datamap preagg_count on table $parentTableName using 'preaggregate' as select id,count($columnName) from $parentTableName group by id"""
        .stripMargin)
    sql(
      s"""create datamap preagg_min on table $parentTableName using 'preaggregate' as select id,min($columnName) from $parentTableName group by id"""
        .stripMargin)
    sql(
      s"""create datamap preagg_max on table $parentTableName using 'preaggregate' as select id,max($columnName) from $parentTableName group by id"""
        .stripMargin)
  }

  test("test load into main table with pre-aggregate table") {
    sql("DROP TABLE IF EXISTS maintable")
    sql(
      """
        | CREATE TABLE maintable(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    createAllAggregateTables("maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    checkAnswer(sql(s"select * from maintable_preagg_sum"),
      Seq(Row(1, 31), Row(2, 27), Row(3, 70), Row(4, 55)))
    checkAnswer(sql(s"select * from maintable_preagg_avg"),
      Seq(Row(1, 31, 1), Row(2, 27, 1), Row(3, 70, 2), Row(4, 55, 2)))
    checkAnswer(sql(s"select * from maintable_preagg_count"),
      Seq(Row(1, 1), Row(2, 1), Row(3, 2), Row(4, 2)))
    checkAnswer(sql(s"select * from maintable_preagg_min"),
      Seq(Row(1, 31), Row(2, 27), Row(3, 35), Row(4, 26)))
    checkAnswer(sql(s"select * from maintable_preagg_max"),
      Seq(Row(1, 31), Row(2, 27), Row(3, 35), Row(4, 29)))
    sql("drop table if exists maintable")
  }

  test("test load into main table with pre-aggregate table with dictionary_include") {
    sql("drop table if exists maintable")
    sql(
      """
        | CREATE TABLE maintable(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('dictionary_include'='id')
      """.stripMargin)
    createAllAggregateTables("maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    checkAnswer(sql(s"select * from maintable_preagg_sum"),
      Seq(Row(1, 31), Row(2, 27), Row(3, 70), Row(4, 55)))
    checkAnswer(sql(s"select * from maintable_preagg_avg"),
      Seq(Row(1, 31, 1), Row(2, 27, 1), Row(3, 70, 2), Row(4, 55,2)))
    checkAnswer(sql(s"select * from maintable_preagg_count"),
      Seq(Row(1, 1), Row(2, 1), Row(3, 2), Row(4, 2)))
    checkAnswer(sql(s"select * from maintable_preagg_min"),
      Seq(Row(1, 31), Row(2, 27), Row(3, 35), Row(4, 26)))
    checkAnswer(sql(s"select * from maintable_preagg_max"),
      Seq(Row(1, 31), Row(2, 27), Row(3, 35), Row(4, 29)))
    sql("drop table if exists maintable")
  }

  test("test load into main table with pre-aggregate table with single_pass") {
    sql("drop table if exists maintable")
    sql(
      """
        | CREATE TABLE maintable(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('dictionary_include'='id')
      """.stripMargin)
    createAllAggregateTables("maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable options('single_pass'='true')")
    checkAnswer(sql(s"select * from maintable_preagg_sum"),
      Seq(Row(1, 31), Row(2, 27), Row(3, 70), Row(4, 55)))
    checkAnswer(sql(s"select * from maintable_preagg_avg"),
      Seq(Row(1, 31, 1), Row(2, 27, 1), Row(3, 70, 2), Row(4, 55,2)))
    checkAnswer(sql(s"select * from maintable_preagg_count"),
      Seq(Row(1, 1), Row(2, 1), Row(3, 2), Row(4, 2)))
    checkAnswer(sql(s"select * from maintable_preagg_min"),
      Seq(Row(1, 31), Row(2, 27), Row(3, 35), Row(4, 26)))
    checkAnswer(sql(s"select * from maintable_preagg_max"),
      Seq(Row(1, 31), Row(2, 27), Row(3, 35), Row(4, 29)))
    sql("drop table if exists maintable")
  }

  test("test load into main table with incremental load") {
    sql("drop table if exists maintable")
    sql(
      """
        | CREATE TABLE maintable(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('dictionary_include'='id')
      """.stripMargin)
    createAllAggregateTables("maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    checkAnswer(sql(s"select * from maintable_preagg_sum"),
      Seq(Row(1, 31),
        Row(2, 27),
        Row(3, 70),
        Row(4, 55),
        Row(1, 31),
        Row(2, 27),
        Row(3, 70),
        Row(4, 55)))
    checkAnswer(sql(s"select * from maintable_preagg_avg"),
      Seq(Row(1, 31, 1),
        Row(2, 27, 1),
        Row(3, 70, 2),
        Row(4, 55, 2),
        Row(1, 31, 1),
        Row(2, 27, 1),
        Row(3, 70, 2),
        Row(4, 55, 2)))
    checkAnswer(sql(s"select * from maintable_preagg_count"),
      Seq(Row(1, 1), Row(2, 1), Row(3, 2), Row(4, 2), Row(1, 1), Row(2, 1), Row(3, 2), Row(4, 2)))
    checkAnswer(sql(s"select * from maintable_preagg_min"),
      Seq(Row(1, 31),
        Row(2, 27),
        Row(3, 35),
        Row(4, 26),
        Row(1, 31),
        Row(2, 27),
        Row(3, 35),
        Row(4, 26)))
    checkAnswer(sql(s"select * from maintable_preagg_max"),
      Seq(Row(1, 31),
        Row(2, 27),
        Row(3, 35),
        Row(4, 29),
        Row(1, 31),
        Row(2, 27),
        Row(3, 35),
        Row(4, 29)))
  }

  test("test to check if exception is thrown for direct load on pre-aggregate table") {
    sql("drop table if exists maintable")
    sql(
      """
        | CREATE TABLE maintable(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('dictionary_include'='id')
      """.stripMargin)
    sql(
      s"""create datamap preagg_sum on table maintable using 'preaggregate' as select id,sum(age) from maintable group by id"""
        .stripMargin)
    assert(intercept[RuntimeException] {
      sql(s"insert into maintable_preagg_sum values(1, 30)")
    }.getMessage.equalsIgnoreCase("Cannot insert/load data directly into pre-aggregate/child table"))
  }

  test("test whether all segments are loaded into pre-aggregate table if segments are set on main table") {
    sql("DROP TABLE IF EXISTS maintable")
    sql(
      """
        | CREATE TABLE maintable(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"insert into maintable values(1, 'xyz', 'bengaluru', 26)")
    sql(s"insert into maintable values(1, 'xyz', 'bengaluru', 26)")
    sql("set carbon.input.segments.default.maintable=0")
    sql(
      s"""create datamap preagg_sum on table maintable using 'preaggregate' as select id, sum(age) from maintable group by id"""
        .stripMargin)
    sqlContext.sparkSession.catalog.clearCache()
    sql("reset")
    checkAnswer(sql("select * from maintable_preagg_sum"), Row(1, 52))
  }

  test("test if pre-aagregate is overwritten if main table is inserted with insert overwrite") {
    sql("DROP TABLE IF EXISTS maintable")
    sql(
      """
        | CREATE TABLE maintable(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      s"""create datamap preagg_sum on table maintable using 'preaggregate' as select id, sum(age) from maintable group by id"""
        .stripMargin)
    sql(s"insert into maintable values(1, 'xyz', 'bengaluru', 26)")
    sql(s"insert into maintable values(1, 'xyz', 'bengaluru', 26)")
    sql(s"insert overwrite table maintable values(1, 'xyz', 'delhi', 29)")
    checkAnswer(sql("select * from maintable_preagg_sum"), Row(1, 29))
  }

  test("test load in aggregate table with Measure col") {
    val originalBadRecordsAction = CarbonProperties.getInstance().getProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "FAIL")
    sql("drop table if exists y ")
    sql("create table y(year int,month int,name string,salary int) stored by 'carbondata'")
    sql("insert into y select 10,11,'babu',12")
    sql("create datamap y1_sum1 on table y using 'preaggregate' as select year,name,sum(salary) from y group by year,name")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, originalBadRecordsAction)
  }

  test("test partition load into main table with pre-aggregate table") {
    sql("DROP TABLE IF EXISTS maintable")
    sql(
      """
        | CREATE TABLE maintable(id int, city string, age int) partitioned by(name string)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    createAllAggregateTables("maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    checkAnswer(sql(s"select * from maintable_preagg_sum"),
      Seq(Row(1, 31), Row(2, 27), Row(3, 70), Row(4, 55)))
    checkAnswer(sql(s"select * from maintable_preagg_avg"),
      Seq(Row(1, 31, 1), Row(2, 27, 1), Row(3, 70, 2), Row(4, 55, 2)))
    checkAnswer(sql(s"select * from maintable_preagg_count"),
      Seq(Row(1, 1), Row(2, 1), Row(3, 2), Row(4, 2)))
    checkAnswer(sql(s"select * from maintable_preagg_min"),
      Seq(Row(1, 31), Row(2, 27), Row(3, 35), Row(4, 26)))
    checkAnswer(sql(s"select * from maintable_preagg_max"),
      Seq(Row(1, 31), Row(2, 27), Row(3, 35), Row(4, 29)))
    sql("drop table if exists maintable")
  }

  test("test load into preaggregate table having group by clause") {
    sql("DROP TABLE IF EXISTS maintable")
    sql(
      """
        | CREATE TABLE maintable(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"insert into maintable values(1, 'xyz', 'bengaluru', 26)")
    sql(s"insert into maintable values(1, 'xyz', 'bengaluru', 26)")
    sql("set carbon.input.segments.default.maintable=0")
    sql(
      s"""create datamap preagg_sum on table maintable using 'preaggregate' as select id, sum(age) from maintable group by id,name"""
        .stripMargin)
    sqlContext.sparkSession.catalog.clearCache()
    sql("reset")
    checkAnswer(sql("select * from maintable_preagg_sum"), Row(1, 52, "xyz"))
  }

  test("test pregarregate with spark adaptive execution ") {
    if (Spark2TestQueryExecutor.spark.version.startsWith("2.3")) {
      // enable adaptive execution
      Spark2TestQueryExecutor.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
    }
    sql("DROP TABLE IF EXISTS maintable")
    sql(
      """
        | CREATE TABLE maintable(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      s"""create datamap preagg_sum on table maintable using 'preaggregate' as select id, sum(age) from maintable group by id,name"""
        .stripMargin)
    sql(s"insert into maintable values(1, 'xyz', 'bengaluru', 20)")
    sql(s"insert into maintable values(1, 'xyz', 'bengaluru', 30)")

    checkAnswer(sql("select id, sum(age) from maintable group by id, name"), Row(1, 50))
    sql("drop datamap preagg_sum on table maintable")
    sql("drop table maintable")
    if (Spark2TestQueryExecutor.spark.version.startsWith("2.3")) {
      // disable adaptive execution
      Spark2TestQueryExecutor.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    }
  }


test("check load and select for avg double datatype") {
  sql("drop table if exists maintbl ")
  sql("create table maintbl(year int,month int,name string,salary double) stored by 'carbondata' tblproperties('sort_scope'='Global_sort','table_blocksize'='23','sort_columns'='month,year,name')")
  sql("insert into maintbl select 10,11,'babu',12.89")
  sql("insert into maintbl select 10,11,'babu',12.89")
  sql("create datamap maintbl_double on table maintbl using 'preaggregate' as select name,avg(salary) from maintbl group by name")
  checkAnswer(sql("select name,avg(salary) from maintbl group by name"), Row("babu", 12.89))
}

  test("check load and select for avg int datatype") {
    sql("drop table if exists maintbl ")
    sql("create table maintbl(year int,month int,name string,salary int) stored by 'carbondata' tblproperties('sort_scope'='Global_sort','table_blocksize'='23','sort_columns'='month,year,name')")
    sql("insert into maintbl select 10,11,'babu',12")
    sql("insert into maintbl select 10,11,'babu',12")
    sql("create datamap maintbl_double on table maintbl using 'preaggregate' as select name,avg(salary) from maintbl group by name")
    checkAnswer(sql("select name,avg(salary) from maintbl group by name"), Row("babu", 12.0))
  }

  test("check load and select for avg bigint datatype") {
    sql("drop table if exists maintbl ")
    sql("create table maintbl(year int,month int,name string,salary bigint) stored by 'carbondata' tblproperties('sort_scope'='Global_sort','table_blocksize'='23','sort_columns'='month,year,name')")
    sql("insert into maintbl select 10,11,'babu',12")
    sql("insert into maintbl select 10,11,'babu',12")
    sql("create datamap maintbl_double on table maintbl using 'preaggregate' as select name,avg(salary) from maintbl group by name")
    checkAnswer(sql("select name,avg(salary) from maintbl group by name"), Row("babu", 12.0))
  }

  test("check load and select for avg short datatype") {
    sql("drop table if exists maintbl ")
    sql("create table maintbl(year int,month int,name string,salary short) stored by 'carbondata' tblproperties('sort_scope'='Global_sort','table_blocksize'='23','sort_columns'='month,year,name')")
    sql("insert into maintbl select 10,11,'babu',12")
    sql("insert into maintbl select 10,11,'babu',12")
    sql("create datamap maintbl_double on table maintbl using 'preaggregate' as select name,avg(salary) from maintbl group by name")
    checkAnswer(sql("select name,avg(salary) from maintbl group by name"), Row("babu", 12.0))
  }

  test("check load and select for avg float datatype") {
    sql("drop table if exists maintbl ")
    sql("create table maintbl(year int,month int,name string,salary float) stored by 'carbondata' tblproperties('sort_scope'='Global_sort','table_blocksize'='23','sort_columns'='month,year,name')")
    sql("insert into maintbl select 10,11,'babu',12")
    sql("insert into maintbl select 10,11,'babu',12")
    val rows = sql("select name,avg(salary) from maintbl group by name").collect()
    sql("create datamap maintbl_double on table maintbl using 'preaggregate' as select name,avg(salary) from maintbl group by name")
    checkAnswer(sql("select name,avg(salary) from maintbl group by name"), rows)
  }

  test("create datamap with 'if not exists' after load data into mainTable and create datamap") {
    sql("DROP TABLE IF EXISTS maintable")
    sql(
      """
        | CREATE TABLE maintable(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(
      s"""
         | create datamap preagg_sum
         | on table maintable
         | using 'preaggregate'
         | as select id,sum(age) from maintable
         | group by id
       """.stripMargin)

    sql(
      s"""
         | create datamap if not exists preagg_sum
         | on table maintable
         | using 'preaggregate'
         | as select id,sum(age) from maintable
         | group by id
       """.stripMargin)

    checkAnswer(sql(s"select * from maintable_preagg_sum"),
      Seq(Row(1, 31), Row(2, 27), Row(3, 70), Row(4, 55)))
    sql("drop table if exists maintable")
  }

  test("create datamap with 'if not exists' after create datamap and load data into mainTable") {
    sql("DROP TABLE IF EXISTS maintable")
    sql(
      """
        | CREATE TABLE maintable(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(
      s"""
         | create datamap preagg_sum
         | on table maintable
         | using 'preaggregate'
         | as select id,sum(age) from maintable
         | group by id
       """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(
      s"""
         | create datamap if not exists preagg_sum
         | on table maintable
         | using 'preaggregate'
         | as select id,sum(age) from maintable
         | group by id
       """.stripMargin)

    checkAnswer(sql(s"select * from maintable_preagg_sum"),
      Seq(Row(1, 31), Row(2, 27), Row(3, 70), Row(4, 55)))
    sql("drop table if exists maintable")
  }

  test("create datamap without 'if not exists' after load data into mainTable and create datamap") {
    sql("DROP TABLE IF EXISTS maintable")
    sql(
      """
        | CREATE TABLE maintable(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(
      s"""
         | create datamap preagg_sum
         | on table maintable
         | using 'preaggregate'
         | as select id,sum(age) from maintable
         | group by id
       """.stripMargin)

    val e: Exception = intercept[MalformedDataMapCommandException] {
      sql(
        s"""
           | create datamap preagg_sum
           | on table maintable
           | using 'preaggregate'
           | as select id,sum(age) from maintable
           | group by id
       """.stripMargin)
    }
    assert(e.getMessage.contains("DataMap name 'preagg_sum' already exist"))
    checkAnswer(sql(s"select * from maintable_preagg_sum"),
      Seq(Row(1, 31), Row(2, 27), Row(3, 70), Row(4, 55)))
    sql("drop table if exists maintable")
  }

  test("check load and select for avg int datatype and group by") {
    sql("drop table if exists maintable ")
    sql("CREATE TABLE maintable(id int, city string, age int) stored by 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    val rows = sql("select age,avg(age) from maintable group by age").collect()
    sql("create datamap maintbl_double on table maintable using 'preaggregate' as select avg(age) from maintable group by age")
    checkAnswer(sql("select age,avg(age) from maintable group by age"), rows)
    sql("drop table if exists maintable ")
  }

  test("test load into main table with pre-aggregate table: string") {
    sql(
      """
        | CREATE TABLE main_table(
        |     id INT,
        |     name STRING,
        |     city STRING,
        |     age STRING)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    createAllAggregateTables("main_table")

    sql(s"LOAD DATA LOCAL INPATH '$testData' INTO TABLE main_table")
    checkAnswer(sql(s"SELECT * FROM main_table_preagg_sum"),
      Seq(Row(1, 31), Row(2, 27), Row(3, 70), Row(4, 55)))
    checkAnswer(sql(s"SELECT * FROM main_table_preagg_avg"),
      Seq(Row(1, 31, 1), Row(2, 27, 1), Row(3, 70, 2), Row(4, 55, 2)))
    checkAnswer(sql(s"SELECT * FROM main_table_preagg_count"),
      Seq(Row(1, 1), Row(2, 1), Row(3, 2), Row(4, 2)))
    checkAnswer(sql(s"SELECT * FROM main_table_preagg_min"),
      Seq(Row(1, "31"), Row(2, "27"), Row(3, "35"), Row(4, "26")))
    checkAnswer(sql(s"SELECT * FROM main_table_preagg_max"),
      Seq(Row(1, "31"), Row(2, "27"), Row(3, "35"), Row(4, "29")))

    // check select and match or not match pre-aggregate table
    checkPreAggTable(sql("SELECT id, SUM(age) FROM main_table GROUP BY id"),
      true, "main_table_preagg_sum")
    checkPreAggTable(sql("SELECT id, SUM(age) FROM main_table GROUP BY id"),
      false, "main_table_preagg_avg", "main_table")

    checkPreAggTable(sql("SELECT id, AVG(age) FROM main_table GROUP BY id"),
      true, "main_table_preagg_avg")
    checkPreAggTable(sql("SELECT id, AVG(age) from main_table GROUP BY id"),
      false, "main_table_preagg_sum", "main_table")

    checkPreAggTable(sql("SELECT id, COUNT(age) FROM main_table GROUP BY id"),
      true, "main_table_preagg_count")
    checkPreAggTable(sql("SELECT id, COUNT(age) FROM main_table GROUP BY id"),
      false, "main_table_preagg_sum", "main_table")

    checkPreAggTable(sql("SELECT id, MIN(age) FROM main_table GROUP BY id"),
      true, "main_table_preagg_min")
    checkPreAggTable(sql("SELECT id, MIN(age) FROM main_table GROUP BY id"),
      false, "main_table_preagg_sum", "main_table")

    checkPreAggTable(sql("SELECT id, MAX(age) FROM main_table GROUP BY id"),
      true, "main_table_preagg_max")
    checkPreAggTable(sql("SELECT id, MAX(age) FROM main_table GROUP BY id"),
      false, "main_table_preagg_sum", "main_table")

    // sub query should match pre-aggregate table
    checkPreAggTable(sql("SELECT SUM(age) FROM main_table"),
      true, "main_table_preagg_sum")
    checkPreAggTable(sql("SELECT SUM(age) FROM main_table"),
      false, "main_table_preagg_avg", "main_table")

    checkPreAggTable(sql("SELECT AVG(age) FROM main_table GROUP BY id"),
      true, "main_table_preagg_avg")
    checkPreAggTable(sql("SELECT AVG(age) from main_table GROUP BY id"),
      false, "main_table_preagg_sum", "main_table")

    checkPreAggTable(sql("SELECT COUNT(age) FROM main_table GROUP BY id"),
      true, "main_table_preagg_count")
    checkPreAggTable(sql("SELECT COUNT(age) FROM main_table GROUP BY id"),
      false, "main_table_preagg_sum", "main_table")

    checkPreAggTable(sql("SELECT MIN(age) FROM main_table GROUP BY id"),
      true, "main_table_preagg_min")
    checkPreAggTable(sql("SELECT MIN(age) FROM main_table GROUP BY id"),
      false, "main_table_preagg_sum", "main_table")

    checkPreAggTable(sql("SELECT MAX(age) FROM main_table GROUP BY id"),
      true, "main_table_preagg_max")
    checkPreAggTable(sql("SELECT MAX(age) FROM main_table GROUP BY id"),
      false, "main_table_preagg_sum", "main_table")
  }

  test("test load into main table with pre-aggregate table: sum string column") {
    sql(
      """
        | CREATE TABLE main_table(
        |     id INT,
        |     name STRING,
        |     city STRING,
        |     age STRING)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    createAllAggregateTables("main_table", "name")
    sql(s"LOAD DATA LOCAL INPATH '$testData' INTO TABLE main_table")

    checkAnswer(sql(s"SELECT * FROM main_table_preagg_sum"),
      Seq(Row(1, null), Row(2, null), Row(3, null), Row(4, null)))
    checkAnswer(sql(s"SELECT * FROM main_table_preagg_avg"),
      Seq(Row(1, null, 1.0), Row(2, null, 1.0), Row(3, null, 2.0), Row(4, null, 2.0)))
    checkAnswer(sql(s"SELECT * FROM main_table_preagg_count"),
      Seq(Row(1, 1), Row(2, 1), Row(3, 2), Row(4, 2)))
    checkAnswer(sql(s"SELECT * FROM main_table_preagg_min"),
      Seq(Row(1, "david"), Row(2, "eason"), Row(3, "jarry"), Row(4, "kunal")))
    checkAnswer(sql(s"SELECT * FROM main_table_preagg_max"),
      Seq(Row(1, "david"), Row(2, "eason"), Row(3, "jarry"), Row(4, "vishal")))

    // check select and match or not match pre-aggregate table
    checkPreAggTable(sql("SELECT id, SUM(name) FROM main_table GROUP BY id"),
      true, "main_table_preagg_sum")
    checkPreAggTable(sql("SELECT id, SUM(name) FROM main_table GROUP BY id"),
      false, "main_table_preagg_avg", "main_table")

    checkPreAggTable(sql("SELECT id, AVG(name) FROM main_table GROUP BY id"),
      true, "main_table_preagg_avg")
    checkPreAggTable(sql("SELECT id, AVG(name) from main_table GROUP BY id"),
      false, "main_table_preagg_sum", "main_table")

    checkPreAggTable(sql("SELECT id, COUNT(name) FROM main_table GROUP BY id"),
      true, "main_table_preagg_count")
    checkPreAggTable(sql("SELECT id, COUNT(name) FROM main_table GROUP BY id"),
      false, "main_table_preagg_sum", "main_table")

    checkPreAggTable(sql("SELECT id, MIN(name) FROM main_table GROUP BY id"),
      true, "main_table_preagg_min")
    checkPreAggTable(sql("SELECT id, MIN(name) FROM main_table GROUP BY id"),
      false, "main_table_preagg_sum", "main_table")

    checkPreAggTable(sql("SELECT id, MAX(name) FROM main_table GROUP BY id"),
      true, "main_table_preagg_max")
    checkPreAggTable(sql("SELECT id, MAX(name) FROM main_table GROUP BY id"),
      false, "main_table_preagg_sum", "main_table")

    // sub query should match pre-aggregate table
    checkPreAggTable(sql("SELECT SUM(name) FROM main_table"),
      true, "main_table_preagg_sum")
    checkPreAggTable(sql("SELECT SUM(name) FROM main_table"),
      false, "main_table_preagg_avg", "main_table")

    checkPreAggTable(sql("SELECT AVG(name) FROM main_table GROUP BY id"),
      true, "main_table_preagg_avg")
    checkPreAggTable(sql("SELECT AVG(name) from main_table GROUP BY id"),
      false, "main_table_preagg_sum", "main_table")

    checkPreAggTable(sql("SELECT COUNT(name) FROM main_table GROUP BY id"),
      true, "main_table_preagg_count")
    checkPreAggTable(sql("SELECT COUNT(name) FROM main_table GROUP BY id"),
      false, "main_table_preagg_sum", "main_table")

    checkPreAggTable(sql("SELECT MIN(name) FROM main_table GROUP BY id"),
      true, "main_table_preagg_min")
    checkPreAggTable(sql("SELECT MIN(name) FROM main_table GROUP BY id"),
      false, "main_table_preagg_sum", "main_table")

    checkPreAggTable(sql("SELECT MAX(name) FROM main_table GROUP BY id"),
      true, "main_table_preagg_max")
    checkPreAggTable(sql("SELECT MAX(name) FROM main_table GROUP BY id"),
      false, "main_table_preagg_sum", "main_table")
  }

  test("test whether all segments are loaded into pre-aggregate table if segments are set on main table 2") {
    sql("DROP TABLE IF EXISTS segmaintable")
    sql(
      """
        | CREATE TABLE segmaintable(
        |     id INT,
        |     name STRING,
        |     city STRING,
        |     age INT)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")

    sql("set carbon.input.segments.default.segmaintable=0")
    checkAnswer(sql(s"SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      Seq(Row(1, 26)))
    sql(
      s"""
         | CREATE DATAMAP preagg_sum
         | ON TABLE segmaintable
         | USING 'preaggregate'
         | AS SELECT id, SUM(age)
         | FROM segmaintable
         | GROUP BY id
       """.stripMargin)
    checkPreAggTable(sql("SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      false, "segmaintable_preagg_sum")
    sqlContext.sparkSession.catalog.clearCache()
    sql("reset")
    checkAnswer(sql("SELECT * FROM segmaintable_preagg_sum"), Seq(Row(1, 26)))
    checkPreAggTable(sql("SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      true, "segmaintable_preagg_sum")
  }

  test("test whether all segments are loaded into pre-aggregate table if segments are set on main table 3") {
    sql("DROP TABLE IF EXISTS segmaintable")
    sql(
      """
        | CREATE TABLE segmaintable(
        |     id INT,
        |     name STRING,
        |     city STRING,
        |     age INT)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")

    sql("set carbon.input.segments.default.segmaintable=0")
    checkAnswer(sql(s"SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      Seq(Row(1, 26)))
    sql(
      s"""
         | CREATE DATAMAP preagg_sum
         | ON TABLE segmaintable
         | USING 'preaggregate'
         | AS SELECT id, SUM(age)
         | FROM segmaintable
         | GROUP BY id
       """.stripMargin)
    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")
    sqlContext.sparkSession.catalog.clearCache()
    sql("reset")
    checkAnswer(sql("SELECT * FROM segmaintable_preagg_sum"), Seq(Row(1, 26), Row(1, 26)))
    checkPreAggTable(sql("SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      true, "segmaintable_preagg_sum")
  }

  test("test whether all segments are loaded into pre-aggregate table if segments are set on main table 4") {
    sql("DROP TABLE IF EXISTS segmaintable")
    sql(
      """
        | CREATE TABLE segmaintable(
        |     id INT,
        |     name STRING,
        |     city STRING,
        |     age INT)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")
    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")

    //  check value before set segments
    checkAnswer(sql(s"SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      Seq(Row(1, 52)))

    sql("set carbon.input.segments.default.segmaintable=0")
    //  check value after set segments
    checkAnswer(sql(s"SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      Seq(Row(1, 26)))

    sql(
      s"""
         | CREATE DATAMAP preagg_sum
         | ON TABLE segmaintable
         | USING 'preaggregate'
         | AS SELECT id, SUM(age)
         | FROM segmaintable
         | GROUP BY id
       """.stripMargin)
    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")

    checkAnswer(sql("SELECT * FROM segmaintable_preagg_sum"), Seq(Row(1, 52), Row(1, 26)))
    checkAnswer(sql(s"SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      Seq(Row(1, 26)))
    checkPreAggTable(sql("SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      false, "segmaintable_preagg_sum")
    sqlContext.sparkSession.catalog.clearCache()
    // reset
    sql("reset")
    checkAnswer(sql(s"SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      Seq(Row(1, 78)))
    checkPreAggTable(sql("SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      true, "segmaintable_preagg_sum")
  }

  test("test whether all segments are loaded into pre-aggregate table: auto merge and input segment") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    sqlContext.sparkSession.catalog.clearCache()
    sql("reset")
    sql("DROP TABLE IF EXISTS segmaintable")
    sql(
      """
        | CREATE TABLE segmaintable(
        |     id INT,
        |     name STRING,
        |     city STRING,
        |     age INT)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")
    sql("set carbon.input.segments.default.segmaintable=0")
    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")
    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")

    //  check value before auto merge
    checkAnswer(sql(s"SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      Seq(Row(1, 26)))


    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")

    //  check value after set segments and auto merge
    checkAnswer(sql(s"SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      Seq.empty)

    checkPreAggTable(sql("SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      false, "segmaintable_preagg_sum")

    sql(
      s"""
         | CREATE DATAMAP preagg_sum
         | ON TABLE segmaintable
         | USING 'preaggregate'
         | AS SELECT id, SUM(age)
         | FROM segmaintable
         | GROUP BY id
       """.stripMargin)

    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")
    sqlContext.sparkSession.catalog.clearCache()
    // reset
    sql("reset")
    checkAnswer(sql(s"SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      Seq(Row(1, 130)))
    checkPreAggTable(sql("SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      true, "segmaintable_preagg_sum")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
  }

  //TODO: need to check and fix
  ignore("test whether all segments are loaded into pre-aggregate table: auto merge and no input segment") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    sqlContext.sparkSession.catalog.clearCache()
    sql("reset")
    sql("DROP TABLE IF EXISTS segmaintable")
    sql(
      """
        | CREATE TABLE segmaintable(
        |     id INT,
        |     name STRING,
        |     city STRING,
        |     age INT)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")
    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")
    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")

    sql(
      s"""
         | CREATE DATAMAP preagg_sum
         | ON TABLE segmaintable
         | USING 'preaggregate'
         | AS SELECT id, SUM(age)
         | FROM segmaintable
         | GROUP BY id
       """.stripMargin)

    //  check value before auto merge
    checkAnswer(sql(s"SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      Seq(Row(1, 78)))
    checkPreAggTable(sql("SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      true, "segmaintable_preagg_sum")

    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")

    //  check value after auto merge
    checkPreAggTable(sql("SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      true, "segmaintable_preagg_sum")

    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")

    checkAnswer(sql(s"SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      Seq(Row(1, 130)))
    checkPreAggTable(sql("SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      true, "segmaintable_preagg_sum")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
  }

  test("test whether all segments are loaded into pre-aggregate table: create after auto merge and no input segment") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    sqlContext.sparkSession.catalog.clearCache()
    sql("reset")
    sql("DROP TABLE IF EXISTS segmaintable")
    sql(
      """
        | CREATE TABLE segmaintable(
        |     id INT,
        |     name STRING,
        |     city STRING,
        |     age INT)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")
    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")
    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")
    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")
    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")
    sql(
      s"""
         | CREATE DATAMAP preagg_sum
         | ON TABLE segmaintable
         | USING 'preaggregate'
         | AS SELECT id, SUM(age)
         | FROM segmaintable
         | GROUP BY id
       """.stripMargin)

    checkAnswer(sql(s"SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      Seq(Row(1, 130)))
    checkPreAggTable(sql("SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      true, "segmaintable_preagg_sum")

    sql(s"INSERT INTO segmaintable VALUES(1, 'xyz', 'bengaluru', 26)")
    checkAnswer(sql(s"SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      Seq(Row(1, 156)))
    checkPreAggTable(sql("SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      true, "segmaintable_preagg_sum")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
  }

  //TODO: need to check and fix
  ignore("test whether all segments are loaded into pre-aggregate table: mixed, load, auto merge and input segment") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    sqlContext.sparkSession.catalog.clearCache()
    sql("reset")
    sql("DROP TABLE IF EXISTS main_table")
    sql(
      """
        | CREATE TABLE main_table(
        |     id INT,
        |     name STRING,
        |     city STRING,
        |     age INT)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(s"INSERT INTO main_table VALUES(1, 'xyz', 'bengaluru', 26)")
    sql(s"INSERT INTO main_table VALUES(1, 'xyz', 'bengaluru', 26)")

    sql(s"LOAD DATA LOCAL INPATH '$testData' INTO TABLE main_table")

    createAllAggregateTables("main_table", "age")
    sql("set carbon.input.segments.default.main_table=0")

    checkPreAggTable(sql("SELECT id, SUM(age) FROM main_table GROUP BY id"),
      false, "main_table_preagg_sum")
    checkAnswer(sql(s"SELECT id, SUM(age) FROM main_table GROUP BY id"),
      Seq(Row(1, 26)))
    sqlContext.sparkSession.catalog.clearCache()
    sql("reset")
    checkPreAggTable(sql("SELECT id, SUM(age) FROM main_table GROUP BY id"),
      true, "main_table_preagg_sum")

    sql(s"INSERT INTO main_table VALUES(1, 'xyz', 'bengaluru', 26)")
    sql(s"LOAD DATA LOCAL INPATH '$testData' INTO TABLE main_table")
    sql(s"LOAD DATA LOCAL INPATH '$testData' INTO TABLE main_table")

    checkAnswer(sql(s"SELECT id, SUM(age) FROM main_table GROUP BY id"),
      Seq(Row(1, 171), Row(2, 81), Row(3, 210), Row(4, 165)))
    checkPreAggTable(sql("SELECT id, SUM(age) FROM main_table GROUP BY id"),
      true, "main_table_preagg_sum")

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
  }

  //TODO: need to check and fix
  ignore("test whether all segments are loaded into pre-aggregate table: auto merge and check pre-aggregate segment") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    sqlContext.sparkSession.catalog.clearCache()
    sql("reset")
    sql("DROP TABLE IF EXISTS main_table")
    sql(
      """
        | CREATE TABLE main_table(
        |     id INT,
        |     name STRING,
        |     city STRING,
        |     age INT)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(s"INSERT INTO main_table VALUES(1, 'xyz', 'bengaluru', 26)")

    sql(
      s"""
         | CREATE DATAMAP preagg_sum
         | ON TABLE main_table
         | USING 'preaggregate'
         | AS SELECT id, SUM(age)
         | FROM main_table
         | GROUP BY id
       """.stripMargin)

    sql(s"INSERT INTO main_table VALUES(1, 'xyz', 'bengaluru', 26)")
    sql(s"LOAD DATA LOCAL INPATH '$testData' INTO TABLE main_table")

    checkExistence(sql("show segments for table main_table_preagg_sum"), false, "Compacted")
    sql(s"INSERT INTO main_table VALUES(1, 'xyz', 'bengaluru', 26)")

    // check the data whether auto merge
    checkAnswer(sql(s"SELECT id, SUM(age) FROM main_table GROUP BY id"),
      Seq(Row(1, 109), Row(2, 27), Row(3, 70), Row(4, 55)))
    checkExistence(sql("show segments for table main_table_preagg_sum"), true, "Compacted")

    sql(s"LOAD DATA LOCAL INPATH '$testData' INTO TABLE main_table")
    sql(s"LOAD DATA LOCAL INPATH '$testData' INTO TABLE main_table")

    checkAnswer(sql(s"SELECT id, SUM(age) FROM main_table GROUP BY id"),
      Seq(Row(1, 171), Row(2, 81), Row(3, 210), Row(4, 165)))
    checkPreAggTable(sql("SELECT id, SUM(age) FROM main_table GROUP BY id"),
      true, "main_table_preagg_sum")

    checkExistence(sql("show segments for table main_table_preagg_sum"), true, "Compacted")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
  }

  test("test deferred rebuild is not supported for preagg") {
    val baseTable = "maintable"
    val preagg = "preaggtable"
    sql(s"DROP TABLE IF EXISTS $baseTable")
    sql(
      s"""
        | CREATE TABLE $baseTable(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    val deferredRebuildException = intercept[MalformedDataMapCommandException] {
      sql(
        s"""
           | CREATE DATAMAP $preagg ON TABLE $baseTable
           | USING 'preaggregate'
           | WITH DEFERRED REBUILD
           | AS select id, sum(age) from $baseTable group by id
       """.stripMargin)
    }
    assert(deferredRebuildException.getMessage.contains(
      s"DEFERRED REBUILD is not supported on this datamap $preagg with provider preaggregate"))

    sql(
      s"""
         | CREATE DATAMAP $preagg ON TABLE $baseTable
         | USING 'preaggregate'
         | AS select id, sum(age) from $baseTable group by id
       """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table $baseTable")
    checkExistence(sql(s"SHOW DATAMAP ON TABLE $baseTable"), true, preagg, "preaggregate")
    val exception = intercept[MalformedDataMapCommandException] {
      sql(s"REBUILD DATAMAP $preagg ON TABLE $baseTable").show()
    }
    LOGGER.error(s"XU ${exception.getMessage}")
    assert(exception.getMessage.contains(s"Non-lazy datamap $preagg does not support rebuild"))
    sql(s"DROP TABLE IF EXISTS $baseTable")
  }
}
