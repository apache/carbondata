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
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.util.SparkUtil4Test
import org.scalatest.{BeforeAndAfterAll, Ignore}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException

class TestPreAggregateLoad extends QueryTest with BeforeAndAfterAll {

  val testData = s"$resourcesPath/sample.csv"

  override def beforeAll(): Unit = {
    SparkUtil4Test.createTaskMockUp(sqlContext)
    sql("DROP TABLE IF EXISTS maintable")
  }

  private def createAllAggregateTables(parentTableName: String): Unit = {
    sql(
      s"""create datamap preagg_sum on table $parentTableName using 'preaggregate' as select id,sum(age) from $parentTableName group by id"""
        .stripMargin)
    sql(
      s"""create datamap preagg_avg on table $parentTableName using 'preaggregate' as select id,avg(age) from $parentTableName group by id"""
        .stripMargin)
    sql(
      s"""create datamap preagg_count on table $parentTableName using 'preaggregate' as select id,count(age) from $parentTableName group by id"""
        .stripMargin)
    sql(
      s"""create datamap preagg_min on table $parentTableName using 'preaggregate' as select id,min(age) from $parentTableName group by id"""
        .stripMargin)
    sql(
      s"""create datamap preagg_max on table $parentTableName using 'preaggregate' as select id,max(age) from $parentTableName group by id"""
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
    }.getMessage.equalsIgnoreCase("Cannot insert/load data directly into pre-aggregate table"))
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
    checkAnswer(sql("select * from maintable_preagg_sum"), Row(1, 52, "xyz"))
  }
test("check load and select for avg double datatype") {
  sql("drop table if exists maintbl ")
  sql("create table maintbl(year int,month int,name string,salary double) stored by 'carbondata' tblproperties('sort_scope'='Global_sort','table_blocksize'='23','sort_columns'='month,year,name')")
  sql("insert into maintbl select 10,11,'babu',12.89")
  sql("insert into maintbl select 10,11,'babu',12.89")
  sql("create datamap maintbl_douoble on table maintbl using 'preaggregate' as select name,avg(salary) from maintbl group by name")
  checkAnswer(sql("select name,avg(salary) from maintbl group by name"), Row("babu", 12.89))
}


  test("check load and select for avg int datatype") {
    sql("drop table if exists maintbl ")
    sql("create table maintbl(year int,month int,name string,salary int) stored by 'carbondata' tblproperties('sort_scope'='Global_sort','table_blocksize'='23','sort_columns'='month,year,name')")
    sql("insert into maintbl select 10,11,'babu',12")
    sql("insert into maintbl select 10,11,'babu',12")
    sql("create datamap maintbl_douoble on table maintbl using 'preaggregate' as select name,avg(salary) from maintbl group by name")
    checkAnswer(sql("select name,avg(salary) from maintbl group by name"), Row("babu", 12.0))
  }

  test("check load and select for avg bigint datatype") {
    sql("drop table if exists maintbl ")
    sql("create table maintbl(year int,month int,name string,salary bigint) stored by 'carbondata' tblproperties('sort_scope'='Global_sort','table_blocksize'='23','sort_columns'='month,year,name')")
    sql("insert into maintbl select 10,11,'babu',12")
    sql("insert into maintbl select 10,11,'babu',12")
    sql("create datamap maintbl_douoble on table maintbl using 'preaggregate' as select name,avg(salary) from maintbl group by name")
    checkAnswer(sql("select name,avg(salary) from maintbl group by name"), Row("babu", 12.0))
  }

  test("check load and select for avg short datatype") {
    sql("drop table if exists maintbl ")
    sql("create table maintbl(year int,month int,name string,salary short) stored by 'carbondata' tblproperties('sort_scope'='Global_sort','table_blocksize'='23','sort_columns'='month,year,name')")
    sql("insert into maintbl select 10,11,'babu',12")
    sql("insert into maintbl select 10,11,'babu',12")
    sql("create datamap maintbl_douoble on table maintbl using 'preaggregate' as select name,avg(salary) from maintbl group by name")
    checkAnswer(sql("select name,avg(salary) from maintbl group by name"), Row("babu", 12.0))
  }

  test("check load and select for avg float datatype") {
    sql("drop table if exists maintbl ")
    sql("create table maintbl(year int,month int,name string,salary float) stored by 'carbondata' tblproperties('sort_scope'='Global_sort','table_blocksize'='23','sort_columns'='month,year,name')")
    sql("insert into maintbl select 10,11,'babu',12")
    sql("insert into maintbl select 10,11,'babu',12")
    val rows = sql("select name,avg(salary) from maintbl group by name").collect()
    sql("create datamap maintbl_douoble on table maintbl using 'preaggregate' as select name,avg(salary) from maintbl group by name")
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

    val e: Exception = intercept[TableAlreadyExistsException] {
      sql(
        s"""
           | create datamap preagg_sum
           | on table maintable
           | using 'preaggregate'
           | as select id,sum(age) from maintable
           | group by id
       """.stripMargin)
    }
    assert(e.getMessage.contains("already exists in database"))
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
    sql("create datamap maintbl_douoble on table maintable using 'preaggregate' as select avg(age) from maintable group by age")
    checkAnswer(sql("select age,avg(age) from maintable group by age"), rows)
  }

  test("test whether all segments are loaded into pre-aggregate table if segments are set on main table 5") {
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

    // set *
    sql("set carbon.input.segments.default.segmaintable=*")
    checkAnswer(sql(s"SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      Seq(Row(1, 78)))

    // TODO: should support match pre-aggregate table when set carbon.input.segments.default.segmaintable=*
    checkPreAggTable(sql("SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      true, "segmaintable_preagg_sum")

    // reset
    sql("reset")
    checkAnswer(sql(s"SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      Seq(Row(1, 78)))
    checkPreAggTable(sql("SELECT id, SUM(age) FROM segmaintable GROUP BY id"),
      true, "segmaintable_preagg_sum")
  }

}
