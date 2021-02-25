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
package org.apache.carbondata.spark.testsuite.allqueries

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestPruneUsingSegmentMinMax extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    drop
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_LOAD_ALL_SEGMENT_INDEXES_TO_CACHE, "false")
  }

  private def drop = {
    sql("drop table if exists carbon")
    sql("drop table if exists parquet")
  }

  // Exclude when running with index server, as show cache result varies.
  test("test if matched segment is only loaded to cache", true) {
    createTablesAndLoadData
    checkAnswer(sql("select * from carbon where a=1"), sql("select * from parquet where a=1"))
    val showCache = sql("show metacache on table carbon").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("1/3 index files cached"))
    drop
  }

  private def createTablesAndLoadData = {
    drop
    // scalastyle:off lineLength
    sql("create table carbon(a int, b string, c double,d int,e timestamp) stored as carbondata")
    sql("insert into carbon values(1,'ab',23.4,5,'2017-09-01 00:00:00'),(2,'aa',23.6,8,'2017-09-02 00:00:00')")
    sql("insert into carbon values(3,'ab',23.4,5,'2017-09-01 00:00:00'),(4,'aa',23.6,8,'2017-09-02 00:00:00')")
    sql("insert into carbon values(5,'ab',23.4,5,'2017-09-01 00:00:00'),(6,'aa',23.6,8,'2017-09-02 00:00:00')")
    sql("create table parquet(a int, b string, c double,d int,e timestamp) using parquet")
    sql("insert into parquet values(1,'ab',23.4,5,'2017-09-01 00:00:00'),(2,'aa',23.6,8,'2017-09-02 00:00:00')")
    sql("insert into parquet values(3,'ab',23.4,5,'2017-09-01 00:00:00'),(4,'aa',23.6,8,'2017-09-02 00:00:00')")
    sql("insert into parquet values(5,'ab',23.4,5,'2017-09-01 00:00:00'),(6,'aa',23.6,8,'2017-09-02 00:00:00')")
    // scalastyle:on lineLength
  }

  // Exclude when running with index server, as show cache result varies.
  test("test if matched segment is only loaded to cache after drop column", true) {
    createTablesAndLoadData
    checkAnswer(sql("select * from carbon where a=1"), sql("select * from parquet where a=1"))
    checkAnswer(sql("select * from carbon where a=2"), sql("select * from parquet where a=2"))
    var showCache = sql("show metacache on table carbon").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("1/3 index files cached"))
    checkAnswer(sql("select * from carbon where a=5"), sql("select * from parquet where a=5"))
    showCache = sql("show metacache on table carbon").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("2/3 index files cached"))
    sql("alter table carbon drop columns(d,e)")
    sql("insert into carbon values(7,'gg',45.6),(8,'eg',45.6)")
    checkAnswer(sql("select a from carbon where a=1"), sql("select a from parquet where a=1"))
    showCache = sql("show metacache on table carbon").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("1/4 index files cached"))
    checkAnswer(sql("select * from carbon where a=7"), Seq(Row(7, "gg", 45.6)))
    showCache = sql("show metacache on table carbon").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("2/4 index files cached"))
    drop
  }

  // Exclude when running with index server, as show cache result varies.
  test("test if matched segment is only loaded to cache after add column", true) {
    createTablesAndLoadData
    sql("alter table carbon add columns(g decimal(3,2))")
    sql("insert into carbon values" +
        "(7,'gg',45.6,3,'2017-09-01 00:00:00',23.5),(8,'eg',45.6,6,'2017-09-01 00:00:00', 4.34)")
    checkAnswer(sql("select a from carbon where a=1"), sql("select a from parquet where a=1"))
    var showCache = sql("show metacache on table carbon").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("1/4 index files cached"))
    checkAnswer(sql("select a from carbon where a=7"), Seq(Row(7)))
    showCache = sql("show metacache on table carbon").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("2/4 index files cached"))
    drop
  }

  // Exclude when running with index server, as show cache result varies.
  test("test segment pruning after update operation", true) {
    createTablesAndLoadData
    checkAnswer(sql("select a from carbon where a=1"), Seq(Row(1)))
    var showCache = sql("show metacache on table carbon").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("1/3 index files cached"))
    sql("insert into carbon values" +
        "(1,'ab',23.4,5,'2017-09-01 00:00:00'),(2,'aa',23.6,8,'2017-09-02 00:00:00')")
    sql("insert into carbon values" +
        "(1,'ab',23.4,5,'2017-09-01 00:00:00'),(2,'aa',23.6,8,'2017-09-02 00:00:00')")
    sql("update carbon set(a)=(10) where a=1").collect()
    checkAnswer(sql("select count(*) from carbon where a=10"), Seq(Row(3)))
    showCache = sql("show metacache on table carbon").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("1/6 index files cached"))
    drop
  }

  // Exclude when running with index server, as show cache result varies.
  test("alter set/unset sort column properties", true) {
    createTablesAndLoadData
    sql(s"alter table carbon set tblproperties('sort_scope'='local_sort', 'sort_columns'='a')")
    sql("insert into carbon values" +
        "(3,'ab',23.4,5,'2017-09-01 00:00:00'),(4,'aa',23.6,8,'2017-09-02 00:00:00')")
    sql("insert into carbon values" +
        "(3,'ab',23.4,5,'2017-09-01 00:00:00'),(6,'aa',23.6,8,'2017-09-02 00:00:00')")
    assert(sql("select a from carbon where a=3").count() == 3)
    val showCache = sql("show metacache on table carbon").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("3/5 index files cached"))
  }

  override def afterAll(): Unit = {
    drop
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_LOAD_ALL_SEGMENT_INDEXES_TO_CACHE,
        CarbonCommonConstants.CARBON_LOAD_ALL_SEGMENT_INDEXES_TO_CACHE_DEFAULT)
  }

}
