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
import org.scalatest.{BeforeAndAfterAll, Ignore}

class TestPreAggregateLoad extends QueryTest with BeforeAndAfterAll {

  val testData = s"$resourcesPath/sample.csv"

  override def beforeAll(): Unit = {
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
    sql("drop table if exists maintable")
  }

}
