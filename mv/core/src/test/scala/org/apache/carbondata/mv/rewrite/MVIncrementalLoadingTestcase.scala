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

package org.apache.carbondata.mv.rewrite

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.status.DataMapSegmentStatusUtil
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}


/**
 * Test Class to verify Incremental Load on  MV Datamap
 */
class MVIncrementalLoadingTestcase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    defaultConfig()
    sql("drop table IF EXISTS test_table")
    sql("drop table IF EXISTS test_table1")
    sql("drop table IF EXISTS main_table")
    sql("drop table IF EXISTS dimensiontable")
    sql("drop table if exists products")
    sql("drop table if exists sales")
    sql("drop table if exists products1")
    sql("drop table if exists sales1")
    sql("drop materialized view if exists datamap1")
  }

  test("test Incremental Loading on refresh MV") {
    //create table and load data
    createTableFactTable("test_table")
    loadDataToFactTable("test_table")
    createTableFactTable("test_table1")
    loadDataToFactTable("test_table1")
    //create materialized view on table test_table
    sql("drop materialized view if exists datamap1")
    sql(
      "create materialized view datamap1  with deferred refresh as select empname, designation " +
      "from test_table")
    val query: String = "select empname from test_table"
    val df1 = sql(s"$query")
    assert(!TestUtil.verifyMVDataMap(df1.queryExecution.optimizedPlan, "datamap1"))
    sql(s"refresh materialized view datamap1")
    val dataMapTable = CarbonMetadata.getInstance().getCarbonTable(
      CarbonCommonConstants.DATABASE_DEFAULT_NAME,
      "datamap1_table"
    )
    var loadMetadataDetails = SegmentStatusManager.readLoadMetadata(dataMapTable.getMetadataPath)
    var segmentMap = DataMapSegmentStatusUtil.getSegmentMap(loadMetadataDetails(0).getExtraInfo)
    val segmentList = new java.util.ArrayList[String]()
    segmentList.add("0")
    assert(segmentList.containsAll( segmentMap.get("default.test_table")))
    val df2 = sql(s"$query")
    assert(TestUtil.verifyMVDataMap(df2.queryExecution.optimizedPlan, "datamap1"))
    loadDataToFactTable("test_table")
    loadDataToFactTable("test_table1")
    sql(s"refresh materialized view datamap1")
    loadMetadataDetails = SegmentStatusManager.readLoadMetadata(dataMapTable.getMetadataPath)
    segmentMap = DataMapSegmentStatusUtil.getSegmentMap(loadMetadataDetails(1).getExtraInfo)
    segmentList.clear()
    segmentList.add("1")
    assert(segmentList.containsAll( segmentMap.get("default.test_table")))
    checkAnswer(sql("select empname, designation from test_table"),
      sql("select empname, designation from test_table1"))
    val df3 = sql(s"$query")
    assert(TestUtil.verifyMVDataMap(df3.queryExecution.optimizedPlan, "datamap1"))
    loadDataToFactTable("test_table")
    loadDataToFactTable("test_table1")
    val df4 = sql(s"$query")
    assert(!TestUtil.verifyMVDataMap(df4.queryExecution.optimizedPlan, "datamap1"))
    checkAnswer(sql("select empname, designation from test_table"),
      sql("select empname, designation from test_table1"))
  }

  test("test MV incremental loading with main table having Marked for Delete segments") {
    createTableFactTable("test_table")
    loadDataToFactTable("test_table")
    createTableFactTable("test_table1")
    loadDataToFactTable("test_table1")
    loadDataToFactTable("test_table")
    loadDataToFactTable("test_table1")
    sql("Delete from table test_table where segment.id in (0)")
    sql("Delete from table test_table1 where segment.id in (0)")
    sql("drop materialized view if exists datamap1")
    sql("create materialized view datamap1 with deferred refresh as select empname, designation " +
        "from test_table")
    loadDataToFactTable("test_table")
    loadDataToFactTable("test_table1")
    sql(s"refresh materialized view datamap1")
    val dataMapTable = CarbonMetadata.getInstance().getCarbonTable(
      CarbonCommonConstants.DATABASE_DEFAULT_NAME,
      "datamap1_table")
    val loadMetadataDetails = SegmentStatusManager.readLoadMetadata(dataMapTable.getMetadataPath)
    val segmentMap = DataMapSegmentStatusUtil.getSegmentMap(loadMetadataDetails(0).getExtraInfo)
    val segmentList = new java.util.ArrayList[String]()
    segmentList.add("1")
    segmentList.add("2")
    assert(segmentList.containsAll( segmentMap.get("default.test_table")))
    checkAnswer(sql("select empname, designation from test_table"),
      sql("select empname, designation from test_table1"))
    dropTable("test_table")
    dropTable("test_table1")
  }

  test("test MV incremental loading with update operation on main table") {
    sql("drop table IF EXISTS main_table")
    sql("drop table IF EXISTS testtable")
    sql("create table main_table(a string,b string,c int) STORED AS carbondata")
    sql("insert into main_table values('a','abc',1)")
    sql("insert into main_table values('b','bcd',2)")
    sql("create table testtable(a string,b string,c int) STORED AS carbondata")
    sql("insert into testtable values('a','abc',1)")
    sql("insert into testtable values('b','bcd',2)")
    sql("drop materialized view if exists datamap1")
    sql("create materialized view datamap1 with deferred refresh as select a, sum(b) from main_table group by a")
    sql(s"refresh materialized view datamap1")
    var df = sql(
      s"""select a, sum(b) from main_table group by a""".stripMargin)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap1"))
    checkAnswer(sql(" select a, sum(b) from testtable group by a"),
      sql(" select a, sum(b) from main_table group by a"))
    sql("update main_table set(a) = ('aaa') where b = 'abc'").show(false)
    sql("update testtable set(a) = ('aaa') where b = 'abc'").show(false)
    val dataMapTable = CarbonEnv.getCarbonTable(
      Option(CarbonCommonConstants.DATABASE_DEFAULT_NAME),
      "datamap1_table")(sqlContext.sparkSession)
    var loadMetadataDetails = SegmentStatusManager.readLoadMetadata(dataMapTable.getMetadataPath)
    assert(loadMetadataDetails(0).getSegmentStatus == SegmentStatus.MARKED_FOR_DELETE)
    checkAnswer(sql("select * from main_table"), sql("select * from testtable"))
    sql(s"refresh materialized view datamap1")
    loadMetadataDetails = SegmentStatusManager.readLoadMetadata(dataMapTable.getMetadataPath)
    val segmentMap = DataMapSegmentStatusUtil.getSegmentMap(loadMetadataDetails(1).getExtraInfo)
    val segmentList = new java.util.ArrayList[String]()
    segmentList.add("0")
    segmentList.add("1")
    assert(segmentList.containsAll( segmentMap.get("default.main_table")))
    df = sql(s""" select a, sum(b) from main_table group by a""".stripMargin)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap1"))
    checkAnswer(sql(" select a, sum(b) from testtable group by a"),
      sql(" select a, sum(b) from main_table group by a"))
    sql("drop table IF EXISTS main_table")
    sql("drop table IF EXISTS testtable")
  }

  test("test compaction on mv table") {
    createTableFactTable("test_table")
    loadDataToFactTable("test_table")
    sql("drop materialized view if exists datamap1")
    sql(
      "create materialized view datamap1 with deferred refresh  as select empname, designation " +
      "from test_table")
    loadDataToFactTable("test_table")
    sql(s"refresh materialized view datamap1")
    loadDataToFactTable("test_table")
    sql(s"refresh materialized view datamap1")
    loadDataToFactTable("test_table")
    sql(s"refresh materialized view datamap1")
    sql("alter table datamap1_table compact 'major'")
    val dataMapTable = CarbonMetadata.getInstance().getCarbonTable(
      CarbonCommonConstants.DATABASE_DEFAULT_NAME,
      "datamap1_table"
    )
    val loadMetadataDetails = SegmentStatusManager.readLoadMetadata(dataMapTable.getMetadataPath)
    val segmentMap = DataMapSegmentStatusUtil.getSegmentMap(loadMetadataDetails(3).getExtraInfo)
    val segmentList = new java.util.ArrayList[String]()
    segmentList.add("0")
    segmentList.add("1")
    segmentList.add("2")
    segmentList.add("3")
    assert(segmentList.containsAll( segmentMap.get("default.test_table")))
    checkExistence(sql("show segments for table datamap1_table"), true, "0.1")
    sql("clean files for table datamap1_table")
    sql("drop table IF EXISTS test_table")
  }

  test("test auto-compaction on mv table") {
    sql("set carbon.enable.auto.load.merge=true")
    createTableFactTable("test_table")
    loadDataToFactTable("test_table")
    sql("drop materialized view if exists datamap1")
    sql(
      "create materialized view datamap1 with deferred refresh as select empname, designation " +
      "from test_table")
    loadDataToFactTable("test_table")
    sql(s"refresh materialized view datamap1")
    loadDataToFactTable("test_table")
    sql(s"refresh materialized view datamap1")
    loadDataToFactTable("test_table")
    sql(s"refresh materialized view datamap1")
    loadDataToFactTable("test_table")
    sql(s"refresh materialized view datamap1")
    loadDataToFactTable("test_table")
    sql(s"refresh materialized view datamap1")
    loadDataToFactTable("test_table")
    sql(s"refresh materialized view datamap1")
    sql("clean files for table datamap1_table")
    sql("clean files for table test_table")
    val dataMapTable = CarbonMetadata.getInstance().getCarbonTable(
      CarbonCommonConstants.DATABASE_DEFAULT_NAME,
      "datamap1_table")
    val loadMetadataDetails = SegmentStatusManager.readLoadMetadata(dataMapTable.getMetadataPath)
    val segmentMap = DataMapSegmentStatusUtil.getSegmentMap(loadMetadataDetails(2).getExtraInfo)
    val segmentList = new java.util.ArrayList[String]()
    segmentList.add("0.1")
    segmentList.add("4")
    segmentList.add("5")
    segmentList.add("6")
    assert(segmentList.containsAll(segmentMap.get("default.test_table")))
    dropTable("test_table")
  }

  test("test insert overwrite") {
    sql("drop table IF EXISTS test_table")
    sql("create table test_table(a string,b string,c int) STORED AS carbondata")
    sql("insert into test_table values('a','abc',1)")
    sql("insert into test_table values('b','bcd',2)")
    sql("drop materialized view if exists datamap1")
    sql(
      "create materialized view datamap1 with deferred refresh as select a, sum(b) from test_table  group by a")
    sql(s"refresh materialized view datamap1")
    checkAnswer(sql(" select a, sum(b) from test_table  group by a"), Seq(Row("a", null), Row("b", null)))
    sql("insert overwrite table test_table select 'd','abc',3")
    val dataMapTable = CarbonEnv.getCarbonTable(
      Option(CarbonCommonConstants.DATABASE_DEFAULT_NAME),
      "datamap1_table")(sqlContext.sparkSession)
    var loadMetadataDetails = SegmentStatusManager.readLoadMetadata(dataMapTable.getMetadataPath)
    assert(loadMetadataDetails(0).getSegmentStatus == SegmentStatus.MARKED_FOR_DELETE)
    checkAnswer(sql(" select a, sum(b) from test_table  group by a"), Seq(Row("d", null)))
    sql(s"refresh materialized view datamap1")
    loadMetadataDetails = SegmentStatusManager.readLoadMetadata(dataMapTable.getMetadataPath)
    val segmentMap = DataMapSegmentStatusUtil.getSegmentMap(loadMetadataDetails(1).getExtraInfo)
    val segmentList = new java.util.ArrayList[String]()
    segmentList.add("2")
    assert(segmentList.containsAll( segmentMap.get("default.test_table")))
    sql("drop table IF EXISTS test_table")
  }

  test("test inner join with mv") {
    sql("drop table if exists products")
    sql("create table products (product string, amount int) STORED AS carbondata ")
    sql(s"load data INPATH '$resourcesPath/products.csv' into table products")
    sql("drop table if exists sales")
    sql("create table sales (product string, quantity int) STORED AS carbondata")
    sql(s"load data INPATH '$resourcesPath/sales_data.csv' into table sales")
    sql("drop materialized view if exists innerjoin")
    sql("Create materialized view innerjoin with deferred refresh as Select p.product, p.amount, s.quantity, s.product from " +
        "products p, sales s where p.product=s.product")
    sql("drop table if exists products1")
    sql("create table products1 (product string, amount int) STORED AS carbondata ")
    sql(s"load data INPATH '$resourcesPath/products.csv' into table products1")
    sql("drop table if exists sales1")
    sql("create table sales1 (product string, quantity int) STORED AS carbondata")
    sql(s"load data INPATH '$resourcesPath/sales_data.csv' into table sales1")
    sql(s"refresh materialized view innerjoin")
    checkAnswer(sql("Select p.product, p.amount, s.quantity from products1 p, sales1 s where p.product=s.product"),
      sql("Select p.product, p.amount, s.quantity from products p, sales s where p.product=s.product"))
    sql("insert into products values('Biscuits',10)")
    sql("insert into products1 values('Biscuits',10)")
    sql("refresh materialized view innerjoin")
    checkAnswer(sql("Select p.product, p.amount, s.quantity from products1 p, sales1 s where p.product=s.product"),
      sql("Select p.product, p.amount, s.quantity from products p, sales s where p.product=s.product"))
    sql("insert into sales values('Biscuits',100)")
    sql("insert into sales1 values('Biscuits',100)")
    checkAnswer(sql("Select p.product, p.amount, s.quantity from products1 p, sales1 s where p.product=s.product"),
      sql("Select p.product, p.amount, s.quantity from products p, sales s where p.product=s.product"))
  }

  test("test set segments with main table having mv") {
    sql("drop table IF EXISTS main_table")
    sql("drop table IF EXISTS test_table")
    sql("create table main_table(a string,b string,c int) STORED AS carbondata")
    sql("insert into main_table values('a','abc',1)")
    sql("insert into main_table values('b','bcd',2)")
    sql("create table test_table(a string,b string,c int) STORED AS carbondata")
    sql("insert into test_table values('a','abc',1)")
    sql("insert into test_table values('b','bcd',2)")
    sql("drop materialized view if exists datamap_mt")
    sql(
      "create materialized view datamap_mt with deferred refresh as select a, sum(b) from main_table  group by a")
    sql(s"refresh materialized view datamap_mt")
    checkAnswer(sql("select a, sum(b) from main_table  group by a"),
      sql("select a, sum(b) from test_table  group by a"))
    sql("SET carbon.input.segments.default.main_table = 1")
    sql("SET carbon.input.segments.default.test_table=1")
    checkAnswer(sql("select a, sum(b) from main_table  group by a"),
      sql("select a, sum(b) from test_table  group by a"))
    sql("drop table IF EXISTS main_table")
    sql("drop table IF EXISTS test_table")
  }

  test("test set segments with main table having mv before refresh") {
    sql("drop table IF EXISTS main_table")
    sql("create table main_table(a string,b string,c int) STORED AS carbondata")
    sql("insert into main_table values('a','abc',1)")
    sql("insert into main_table values('b','bcd',2)")
    sql("drop materialized view if exists datamap1")
    sql("create materialized view datamap1 with deferred refresh as select a, sum(c) from main_table  group by a")
    sql("SET carbon.input.segments.default.main_table=1")
    sql(s"refresh materialized view datamap1")
    val df = sql("select a, sum(c) from main_table  group by a")
    assert(!TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap1"))
    defaultConfig()
    sqlContext.sparkSession.conf.unset("carbon.input.segments.default.main_table")
    checkAnswer(sql("select a, sum(c) from main_table  group by a"), Seq(Row("a", 1), Row("b", 2)))
    val df1= sql("select a, sum(c) from main_table  group by a")
    assert(TestUtil.verifyMVDataMap(df1.queryExecution.optimizedPlan, "datamap1"))
    sql("drop table IF EXISTS main_table")
  }

  test("test materialized view table after materialized view table compaction- custom") {
    sql("drop table IF EXISTS main_table")
    sql("create table main_table(a string,b string,c int) STORED AS carbondata")
    sql("insert into main_table values('a','abc',1)")
    sql("insert into main_table values('b','bcd',2)")
    sql("drop materialized view if exists datamap1")
    sql("create materialized view datamap1 with deferred refresh as select a, sum(b) from main_table  group by a")
    sql("refresh materialized view datamap1")
    sql("insert into main_table values('a','abc',1)")
    sql("insert into main_table values('b','bcd',2)")
    sql("refresh materialized view datamap1")
    sql("alter table datamap1_table compact 'custom' where segment.id in (0,1)")
    val dataMapTable = CarbonMetadata.getInstance().getCarbonTable(
      CarbonCommonConstants.DATABASE_DEFAULT_NAME,
      "datamap1_table")
    val loadMetadataDetails = SegmentStatusManager.readLoadMetadata(dataMapTable.getMetadataPath)
    assert(loadMetadataDetails(0).getSegmentStatus == SegmentStatus.COMPACTED)
    assert(loadMetadataDetails(1).getSegmentStatus == SegmentStatus.COMPACTED)
    var segmentMap = DataMapSegmentStatusUtil.getSegmentMap(loadMetadataDetails(2).getExtraInfo)
    val segmentList = new java.util.ArrayList[String]()
    segmentList.add("0")
    segmentList.add("1")
    segmentList.add("2")
    segmentList.add("3")
    assert(segmentList.containsAll(segmentMap.get("default.main_table")))
    sql("drop table IF EXISTS main_table")
  }

  test("test sum(a) + sum(b)") {
    // Full refresh will happen in this case
    sql("drop table IF EXISTS main_table")
    sql("create table main_table(a int,b int,c int) STORED AS carbondata")
    sql("insert into main_table values(1,2,3)")
    sql("insert into main_table values(1,4,5)")
    sql("drop materialized view if exists datamap_1")
    sql("create materialized view datamap_1 with deferred refresh as select sum(a)+sum(b) from main_table")
    checkAnswer(sql("select sum(a)+sum(b) from main_table"), Seq(Row(8)))
    sql("refresh materialized view datamap_1")
    checkAnswer(sql("select sum(a)+sum(b) from main_table"), Seq(Row(8)))
    sql("insert into main_table values(1,2,3)")
    sql("insert into main_table values(1,4,5)")
    checkAnswer(sql("select sum(a)+sum(b) from main_table"), Seq(Row(16)))
    sql("refresh materialized view datamap_1")
    checkAnswer(sql("select sum(a)+sum(b) from main_table"), Seq(Row(16)))
    sql("drop table IF EXISTS main_table")
  }

  test("test Incremental Loading on non-lazy mv") {
    //create table and load data
    createTableFactTable("test_table")
    loadDataToFactTable("test_table")
    createTableFactTable("test_table1")
    loadDataToFactTable("test_table1")
    //create materialized view on table test_table
    sql("drop materialized view if exists datamap1")
    sql(
      "create materialized view datamap1  as select empname, designation " +
      "from test_table")
    val query: String = "select empname from test_table"
    val dataMapTable = CarbonMetadata.getInstance().getCarbonTable(
      CarbonCommonConstants.DATABASE_DEFAULT_NAME,
      "datamap1_table"
    )
    var loadMetadataDetails = SegmentStatusManager.readLoadMetadata(dataMapTable.getMetadataPath)
    var segmentMap = DataMapSegmentStatusUtil.getSegmentMap(loadMetadataDetails(0).getExtraInfo)
    val segmentList = new java.util.ArrayList[String]()
    segmentList.add("0")
    assert(segmentList.containsAll( segmentMap.get("default.test_table")))
    val df2 = sql(s"$query")
    assert(TestUtil.verifyMVDataMap(df2.queryExecution.optimizedPlan, "datamap1"))
    loadDataToFactTable("test_table")
    loadDataToFactTable("test_table1")
    loadMetadataDetails = SegmentStatusManager.readLoadMetadata(dataMapTable.getMetadataPath)
    segmentMap = DataMapSegmentStatusUtil.getSegmentMap(loadMetadataDetails(1).getExtraInfo)
    segmentList.clear()
    segmentList.add("1")
    assert(segmentList.containsAll( segmentMap.get("default.test_table")))
    checkAnswer(sql("select empname, designation from test_table"),
      sql("select empname, designation from test_table1"))
    val df3 = sql(s"$query")
    assert(TestUtil.verifyMVDataMap(df3.queryExecution.optimizedPlan, "datamap1"))
    loadDataToFactTable("test_table")
    loadDataToFactTable("test_table1")
    val df4 = sql(s"$query")
    assert(TestUtil.verifyMVDataMap(df4.queryExecution.optimizedPlan, "datamap1"))
    checkAnswer(sql("select empname, designation from test_table"),
      sql("select empname, designation from test_table1"))
  }

  test("test MV incremental loading on non-lazy materialized view with update operation on main table") {
    sql("drop table IF EXISTS main_table")
    sql("drop table IF EXISTS testtable")
    sql("create table main_table(a string,b string,c int) STORED AS carbondata")
    sql("insert into main_table values('a','abc',1)")
    sql("insert into main_table values('b','bcd',2)")
    sql("create table testtable(a string,b string,c int) STORED AS carbondata")
    sql("insert into testtable values('a','abc',1)")
    sql("insert into testtable values('b','bcd',2)")
    sql("drop materialized view if exists datamap1")
    sql("create materialized view datamap1  as select a, sum(b) from main_table group by a")
    var df = sql(s"""select a, sum(b) from main_table group by a""".stripMargin)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap1"))
    checkAnswer(sql(" select a, sum(b) from testtable group by a"),
      sql(" select a, sum(b) from main_table group by a"))
    sql("update main_table set(a) = ('aaa') where b = 'abc'").show(false)
    sql("update testtable set(a) = ('aaa') where b = 'abc'").show(false)
    val dataMapTable = CarbonMetadata.getInstance().getCarbonTable(
      CarbonCommonConstants.DATABASE_DEFAULT_NAME,
      "datamap1_table")
    var loadMetadataDetails = SegmentStatusManager.readLoadMetadata(dataMapTable.getMetadataPath)
    assert(loadMetadataDetails(0).getSegmentStatus == SegmentStatus.MARKED_FOR_DELETE)
    var segmentMap = DataMapSegmentStatusUtil.getSegmentMap(loadMetadataDetails(1).getExtraInfo)
    val segmentList = new java.util.ArrayList[String]()
    segmentList.add("0")
    segmentList.add("1")
    assert(segmentList.containsAll(segmentMap.get("default.main_table")))
    df = sql(s""" select a, sum(b) from main_table group by a""".stripMargin)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap1"))
    checkAnswer(sql(" select a, sum(b) from testtable group by a"),
      sql(" select a, sum(b) from main_table group by a"))
    sql("drop table IF EXISTS main_table")
    sql("drop table IF EXISTS testtable")
  }

  test("test MV incremental loading on non-lazy materialized view with delete operation on main table") {
    sql("drop table IF EXISTS main_table")
    sql("drop table IF EXISTS testtable")
    sql("create table main_table(a string,b string,c int) STORED AS carbondata")
    sql("insert into main_table values('a','abc',1)")
    sql("insert into main_table values('b','bcd',2)")
    sql("create table testtable(a string,b string,c int) STORED AS carbondata")
    sql("insert into testtable values('a','abc',1)")
    sql("insert into testtable values('b','bcd',2)")
    sql("drop materialized view if exists datamap1")
    sql("create materialized view datamap1  as select a, sum(b) from main_table group by a")
    var df = sql(s"""select a, sum(b) from main_table group by a""".stripMargin)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap1"))
    checkAnswer(sql(" select a, sum(b) from testtable group by a"),
      sql(" select a, sum(b) from main_table group by a"))
    sql("delete from  main_table  where b = 'abc'").show(false)
    sql("delete from  testtable  where b = 'abc'").show(false)
    val dataMapTable = CarbonMetadata.getInstance().getCarbonTable(
      CarbonCommonConstants.DATABASE_DEFAULT_NAME,
      "datamap1_table")
    var loadMetadataDetails = SegmentStatusManager.readLoadMetadata(dataMapTable.getMetadataPath)
    assert(loadMetadataDetails(0).getSegmentStatus == SegmentStatus.MARKED_FOR_DELETE)
    var segmentMap = DataMapSegmentStatusUtil.getSegmentMap(loadMetadataDetails(1).getExtraInfo)
    val segmentList = new java.util.ArrayList[String]()
    segmentList.add("0")
    segmentList.add("1")
    assert(segmentList.containsAll(segmentMap.get("default.main_table")))
    df = sql(s""" select a, sum(b) from main_table group by a""".stripMargin)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap1"))
    checkAnswer(sql(" select a, sum(b) from testtable group by a"),
      sql(" select a, sum(b) from main_table group by a"))
    sql("drop table IF EXISTS main_table")
    sql("drop table IF EXISTS testtable")
  }

  test("test whether materialized view table is compacted after main table compaction") {
    sql("drop table IF EXISTS main_table")
    sql("create table main_table(a string,b string,c int) STORED AS carbondata")
    sql("insert into main_table values('a','abc',1)")
    sql("insert into main_table values('b','bcd',2)")
    sql("drop materialized view if exists datamap1")
    sql("create materialized view datamap1  as select a, sum(b) from main_table group by a")
    sql("insert into main_table values('c','abc',1)")
    sql("insert into main_table values('d','bcd',2)")
    sql("alter table main_table compact 'major'")
    checkExistence(sql("show segments for table main_table"), true, "0.1")
    checkExistence(sql("show segments for table datamap1_table"), true, "0.1")
    sql("drop table IF EXISTS main_table")
  }

  test("test delete record when table contains single segment") {
    sql("drop table IF EXISTS main_table")
    sql("create table main_table(a string,b string,c int) STORED AS carbondata")
    sql("insert into main_table values('a','abc',1)")
    sql("drop materialized view if exists datamap1")
    sql("create materialized view datamap1  as select a, sum(b) from main_table group by a")
    sql("delete from  main_table  where b = 'abc'").show(false)
    val dataMapTable = CarbonMetadata.getInstance().getCarbonTable(
      CarbonCommonConstants.DATABASE_DEFAULT_NAME,
      "datamap1_table")
    val loadMetadataDetails = SegmentStatusManager.readLoadMetadata(dataMapTable.getMetadataPath)
    assert(loadMetadataDetails.length == 1)
    assert(loadMetadataDetails(0).getSegmentStatus == SegmentStatus.MARKED_FOR_DELETE)
  }

  test("set segments on materialized view table") {
    sql("drop table IF EXISTS main_table")
    sql("create table main_table(a string,b string,c int) STORED AS carbondata")
    sql("insert into main_table values('a','abc',1)")
    sql("drop materialized view if exists datamap1")
    sql("create materialized view datamap1  as select a,b from main_table")
    sql("insert into main_table values('b','abcd',1)")
    sql("SET carbon.input.segments.default.datamap1_table=0")
    assert(sql("select a,b from main_table").collect().length == 1)
    sql("drop table IF EXISTS main_table")
  }

  test("test compaction on main table and refresh") {
    createTableFactTable("test_table")
    loadDataToFactTable("test_table")
    sql("drop materialized view if exists datamap1")
    sql(
      "create materialized view datamap1 with deferred refresh  as select empname, designation " +
      "from test_table")
    loadDataToFactTable("test_table")
    loadDataToFactTable("test_table")
    sql(s"refresh materialized view datamap1")
    sql("alter table test_table compact 'major'")
    sql(s"refresh materialized view datamap1")
    val dataMapTable = CarbonMetadata.getInstance().getCarbonTable(
      CarbonCommonConstants.DATABASE_DEFAULT_NAME,
      "datamap1_table")
    val loadMetadataDetails = SegmentStatusManager.readLoadMetadata(dataMapTable.getMetadataPath)
    assert(loadMetadataDetails.length == 1)
    var segmentMap = DataMapSegmentStatusUtil.getSegmentMap(loadMetadataDetails(0).getExtraInfo)
    val segmentList = new java.util.ArrayList[String]()
    segmentList.add("0.1")
    assert(segmentList.containsAll(segmentMap.get("default.test_table")))
  }

  test("test auto compaction with threshold") {
    sql(s"drop table IF EXISTS test_table")
    sql(
      s"""
         | CREATE TABLE test_table (empname String, designation String, doj Timestamp,
         |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
         |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
         |  utilization int,salary int)
         | STORED AS carbondata TBLPROPERTIES('AUTO_LOAD_MERGE'='true','COMPACTION_LEVEL_THRESHOLD'='6,0')
      """.stripMargin)
    loadDataToFactTable("test_table")
    sql("drop materialized view if exists datamap1")
    sql("create materialized view datamap_com  as select empname, designation from test_table")
    for (i <- 0 to 16) {
      loadDataToFactTable("test_table")
    }
    createTableFactTable("test_table1")
    for (i <- 0 to 17) {
      loadDataToFactTable("test_table1")
    }
    checkAnswer(sql("select empname, designation from test_table"),
      sql("select empname, designation from test_table1"))
    val result = sql("show materialized views on table test_table").collectAsList()
    assert(result.get(0).get(6).toString.contains("\"default.test_table\":\"12.1\""))
    val df = sql(s""" select empname, designation from test_table""".stripMargin)
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap_com"))
  }

  test("test all aggregate functions") {
    createTableFactTable("test_table")
    createTableFactTable("test_table1")
    loadDataToFactTable("test_table")
    loadDataToFactTable("test_table1")
    sql("drop materialized view if exists datamap1")
    sql(
      "create materialized view datamap_agg  as select variance(workgroupcategory),var_samp" +
      "(projectcode), var_pop(projectcode), stddev(projectcode),stddev_samp(workgroupcategory)," +
      "corr(projectcode,workgroupcategory),skewness(workgroupcategory)," +
      "kurtosis(workgroupcategory),covar_pop(projectcode,workgroupcategory),covar_samp" +
      "(projectcode,workgroupcategory),projectjoindate from test_table group by projectjoindate")
    val df = sql(
      "select variance(workgroupcategory),var_samp(projectcode), var_pop(projectcode), stddev" +
      "(projectcode),stddev_samp(workgroupcategory),corr(projectcode,workgroupcategory)," +
      "skewness(workgroupcategory),kurtosis(workgroupcategory),covar_pop(projectcode," +
      "workgroupcategory),covar_samp(projectcode,workgroupcategory),projectjoindate from " +
      "test_table group by projectjoindate")
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap_agg"))
    checkAnswer(sql(
      "select variance(workgroupcategory),var_samp(projectcode), var_pop(projectcode), stddev" +
      "(projectcode),stddev_samp(workgroupcategory),corr(projectcode,workgroupcategory)," +
      "skewness(workgroupcategory),kurtosis(workgroupcategory),covar_pop(projectcode," +
      "workgroupcategory),covar_samp(projectcode,workgroupcategory),projectjoindate from " +
      "test_table group by projectjoindate"),
      sql(
        "select variance(workgroupcategory),var_samp(projectcode), var_pop(projectcode), stddev" +
        "(projectcode),stddev_samp(workgroupcategory),corr(projectcode,workgroupcategory)," +
        "skewness(workgroupcategory),kurtosis(workgroupcategory),covar_pop(projectcode," +
        "workgroupcategory),covar_samp(projectcode,workgroupcategory),projectjoindate from " +
        "test_table1 group by projectjoindate"))
  }


  override def afterAll(): Unit = {
    defaultConfig()
    Seq("carbon.enable.auto.load.merge",
      "carbon.input.segments.default.main_table",
      "carbon.input.segments.default.test_table",
      "carbon.input.segments.default.datamap1_table").foreach { key =>
      sqlContext.sparkSession.conf.unset(key)
    }
    sql("drop table if exists products")
    sql("drop table if exists sales")
    sql("drop table if exists products1")
    sql("drop table if exists sales1")
    sql("drop table IF EXISTS test_table")
    sql("drop table IF EXISTS test_table1")
    sql("drop table IF EXISTS main_table")
    sql("drop table IF EXISTS dimensiontable")
  }

  private def createTableFactTable(tableName: String) = {
    sql(s"drop table IF EXISTS $tableName")
    sql(
      s"""
         | CREATE TABLE $tableName (empname String, designation String, doj Timestamp,
         |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
         |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
         |  utilization int,salary int)
         | STORED AS carbondata
      """.stripMargin)
  }

  private def loadDataToFactTable(tableName: String) = {
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data_big.csv' INTO TABLE $tableName  OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '"')""".stripMargin)
  }
}
