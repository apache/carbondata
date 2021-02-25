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

package org.apache.carbondata.sql.commands

import java.util

import scala.collection.JavaConverters._

import mockit.{Mock, MockUp}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.util.QueryTest
import org.junit.Assert
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.{IndexUtil, Segment}
import org.apache.carbondata.core.indexstore.{ExtendedBlocklet, PartitionSpec}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf
import org.apache.carbondata.hadoop.api.CarbonInputFormat

class TestCarbonShowCacheCommand extends QueryTest with BeforeAndAfterAll {
  override protected def beforeAll(): Unit = {
    // use new database
    sql("drop database if exists cache_db cascade").collect()
    sql("drop database if exists cache_empty_db cascade").collect()
    sql("create database cache_db").collect()
    sql("create database cache_empty_db").collect()
    dropTable
    sql("use cache_db").collect()
    sql(
      """
        | CREATE TABLE cache_db.cache_1
        | (empno int, empname String, designation String, doj Timestamp, workgroupcategory int,
        |  workgroupcategoryname String, deptno int, deptname String, projectcode int,
        |  projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,
        |  salary int)
        | STORED AS carbondata
      """.stripMargin)
    // bloom
    sql("CREATE INDEX IF NOT EXISTS cache_1_bloom " +
        "ON TABLE cache_db.cache_1(deptno) AS 'bloomfilter' ")
    sql(s"LOAD DATA INPATH '$resourcesPath/data.csv' INTO TABLE cache_1 ")

    sql(
      """
        | CREATE TABLE cache_2
        | (empno int, empname String, designation String, doj Timestamp, workgroupcategory int,
        |  workgroupcategoryname String, deptno int, deptname String, projectcode int,
        |  projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,
        |  salary int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"LOAD DATA INPATH '$resourcesPath/data.csv' INTO TABLE cache_db.cache_2 ")
    sql("insert into table cache_2 select * from cache_1").collect()

    sql(
      """
        | CREATE TABLE cache_3
        | (empno int, empname String, designation String, doj Timestamp, workgroupcategory int,
        |  workgroupcategoryname String, deptno int, deptname String, projectcode int,
        |  projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,
        |  salary int)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"LOAD DATA INPATH '$resourcesPath/data.csv' INTO TABLE cache_3 ")

    // use default database
    sql("use default").collect()
    sql(
      """
        | CREATE TABLE cache_4
        | (empno int, empname String, designation String, doj Timestamp, workgroupcategory int,
        |  workgroupcategoryname String, deptno int, deptname String, projectcode int,
        |  projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,
        |  salary int)
        | STORED AS carbondata
      """.stripMargin)
    sql("insert into table cache_4 select * from cache_db.cache_2").collect()

    // standard partition table
    sql(
      """
        | CREATE TABLE cache_5
        | (empno int, empname String, designation String, doj Timestamp, workgroupcategory int,
        |  workgroupcategoryname String, deptname String, projectcode int,
        |  projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,
        |  salary int)
        | PARTITIONED BY (deptno int)
        | STORED AS carbondata
      """.stripMargin)
    sql(
      "insert into table cache_5 select empno,empname,designation,doj,workgroupcategory," +
      "workgroupcategoryname,deptname,projectcode,projectjoindate,projectenddate,attendance," +
      "utilization,salary,deptno from cache_4").collect()

    // count star to cache index
    sql("select max(deptname) from cache_db.cache_1").collect()
    sql("SELECT deptno FROM cache_db.cache_1 where deptno=10").collect()
    sql("select count(*) from cache_db.cache_2").collect()
    sql("select count(*) from cache_4").collect()
    sql("select count(*) from cache_5").collect()
    sql("select workgroupcategoryname,count(empname) as count " +
        "from cache_4 group by workgroupcategoryname").collect()
  }

  test("test drop cache invalidation in case of invalid segments") {
    sql(s"CREATE TABLE empTable(empno int, empname String, designation String, " +
        s"doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, " +
        s"deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp," +
        s"attendance int, utilization int, salary int) STORED AS carbondata")
    sql(s"LOAD DATA INPATH '$resourcesPath/data.csv' INTO TABLE empTable")
    sql(s"LOAD DATA INPATH '$resourcesPath/data.csv' INTO TABLE empTable")
    sql(s"LOAD DATA INPATH '$resourcesPath/data.csv' INTO TABLE empTable")
    sql("select count(*) from empTable").collect()
    var showCache = sql("SHOW METACACHE on table empTable").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("3/3 index files cached"))
    sql("delete from table empTable where segment.id in(0)").collect()
    // check whether count(*) query invalidates the cache for the invalid segments
    sql("select count(*) from empTable").collect()
    showCache = sql("SHOW METACACHE on table empTable").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("2/2 index files cached"))
    sql("delete from table empTable where segment.id in(1)").collect()
    // check whether select * query invalidates the cache for the invalid segments
    sql("select * from empTable").collect()
    showCache = sql("SHOW METACACHE on table empTable").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("1/1 index files cached"))
  }

  test("test external table show cache") {
    sql(s"CREATE TABLE employeeTable(empno int, empname String, designation String, " +
        s"doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, " +
        s"deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp," +
        s"attendance int, utilization int, salary int) STORED AS carbondata")
    sql(s"LOAD DATA INPATH '$resourcesPath/data.csv' INTO TABLE employeeTable")
    val table = CarbonEnv.getCarbonTable(Some("default"), "employeeTable")(sqlContext.sparkSession)
    val location = FileFactory
      .getUpdatedFilePath(
        table.getTablePath + CarbonCommonConstants.FILE_SEPARATOR + "/Fact/Part0/Segment_0")
    sql(s"CREATE EXTERNAL TABLE extTable stored as carbondata LOCATION '${location}'")
    sql("select * from extTable").collect()
    val rows = sql("SHOW METACACHE ON TABLE extTable").collect()
    var isPresent = false
    rows.foreach(row => {
      if (row.getString(2).equalsIgnoreCase("1/1 index files cached (external table)")) {
        isPresent = true
      }
    })
    Assert.assertTrue(isPresent)
  }

  override protected def afterAll(): Unit = {
    sql("use default").collect()
    dropTable
  }

  private def dropTable = {
    sql("DROP TABLE IF EXISTS cache_db.cache_1")
    sql("DROP TABLE IF EXISTS cache_db.cache_2")
    sql("DROP TABLE IF EXISTS cache_db.cache_3")
    sql("DROP TABLE IF EXISTS default.cache_4")
    sql("DROP TABLE IF EXISTS default.cache_5")
    sql("DROP TABLE IF EXISTS empTable")
    sql("DROP TABLE IF EXISTS employeeTable")
    sql("DROP TABLE IF EXISTS extTable")
    sql("drop table if exists carbonTable")
  }

  // Exclude when running with index server, as show cache rows count varies.
  test("show cache", true) {

    // Empty database
    sql("use cache_empty_db").collect()
    val result1 = sql("show metacache").collect()
    assertResult(0)(result1.length)

    // Database with 3 tables but only 2 are in cache
    sql("use cache_db").collect()
    val result2 = sql("show metacache").collect()
    assertResult(3)(result2.length)

    // Make sure PreAgg tables are not in SHOW METADATA
    sql("use default").collect()
    val result3 = sql("show metacache").collect()
    val indexCacheInfo = result3
      .map(row => row.getString(1))
      .filter(table => table.equals("cache_4_cache_4_count"))
    assertResult(0)(indexCacheInfo.length)
  }

  // Exclude when running with index server, as show cache rows count varies.
  test("show metacache on table", true) {
    sql("use cache_db").collect()

    // Table with Index & Bloom filter
    val result1 = sql("show metacache on table cache_1").collect()
    assertResult(2)(result1.length)
    assertResult("1/1 index files cached")(result1(0).getString(2))
    assertResult("bloomfilter")(result1(1).getString(2))

    // Table with Index
    val result2 = sql("show metacache on table cache_db.cache_2").collect()
    assertResult(1)(result2.length)
    assertResult("2/2 index files cached")(result2(0).getString(2))

    // Table not in cache
    checkAnswer(sql("show metacache on table cache_db.cache_3"),
      Seq(Row("Index", "0 B", "0/1 index files cached", "DRIVER")))

    sql("use default").collect()

    // Table with 5 index files
    val result5 = sql("show metacache on table cache_5").collect()
    assertResult(1)(result5.length)
    assertResult("5/5 index files cached")(result5(0).getString(2))
  }

  test("test index files cached for table with single partition") {
    sql("drop table if exists partitionTable")
    sql("create table partitionTable(" +
        "col1 int, col2 string) partitioned by (col3 string) stored as carbondata")
    sql("insert into partitionTable values(1,'aa','bb'),(1,'aa1','bb1')")
    sql("insert into partitionTable values(1,'cc','dd')")
    sql("insert into partitionTable values(2,'aa','bb')")
    sql("insert into partitionTable values(1,'aa','ee')")
    checkAnswer(sql("select * from partitionTable where col3='bb'"),
      Seq(Row(1, "aa", "bb"), Row(2, "aa", "bb")))
    var showCache = sql("SHOW METACACHE on table partitionTable").collect()
    val tableIdentifier = new TableIdentifier("partitionTable", Some("default"))
    val carbonTablePath = CarbonEnv
      .getCarbonTable(tableIdentifier)(sqlContext.sparkSession)
      .getTablePath
    var result = CacheProvider.getInstance()
      .getCarbonCache
      .getCacheMap
      .keySet()
      .asScala
      .filter(index => index.startsWith(carbonTablePath))
    assert(result.exists(index => index.startsWith(
      carbonTablePath + CarbonCommonConstants.FILE_SEPARATOR + "col3=bb")) && result.size == 2)
    assert(showCache(0).get(2).toString.equalsIgnoreCase("2/5 index files cached"))
    checkAnswer(sql("select * from partitionTable where col3='ee'"), Seq(Row(1, "aa", "ee")))
    showCache = sql("SHOW METACACHE on table partitionTable").collect()
    result = CacheProvider.getInstance()
      .getCarbonCache
      .getCacheMap
      .keySet()
      .asScala
      .filter(index => index.startsWith(carbonTablePath))
    assert(result.exists(index =>
      index.startsWith(carbonTablePath + CarbonCommonConstants.FILE_SEPARATOR + "col3=bb") ||
      index.startsWith(carbonTablePath + CarbonCommonConstants.FILE_SEPARATOR + "col3=ee") &&
      result.size == 3))
    assert(showCache(0).get(2).toString.equalsIgnoreCase("3/5 index files cached"))
    sql("drop table if exists partitionTable")
  }

  test("test index files cached for table with multiple partition") {
    sql("drop table if exists partitionTable")
    sql("create table partitionTable(col1 int, col2 string) " +
        "partitioned by (col3 string, col4 string, col5 int) stored as carbondata")
    sql("insert into partitionTable values(1,'aa','bb','cc',1),(1,'aa1','bb1','ff',3)")
    sql("insert into partitionTable values(1,'cc','dd','ff',3)")
    sql("insert into partitionTable values(2,'aa','bb','gg',2)")
    sql("insert into partitionTable values(1,'aa','ee','kk',4)")
    checkAnswer(sql("select * from partitionTable where col3='bb' and col4='cc'"),
      Seq(Row(1, "aa", "bb", "cc", 1)))
    var showCache = sql("SHOW METACACHE on table partitionTable").collect()
    val tableIdentifier = new TableIdentifier("partitionTable", Some("default"))
    val carbonTablePath = CarbonEnv
      .getCarbonTable(tableIdentifier)(sqlContext.sparkSession)
      .getTablePath
    var result = CacheProvider.getInstance()
      .getCarbonCache
      .getCacheMap
      .keySet()
      .asScala
      .filter(index => index.startsWith(carbonTablePath))
    assert(result.exists(index => index.startsWith(
      carbonTablePath + CarbonCommonConstants.FILE_SEPARATOR + "col3=bb/col4=cc")) &&
           result.size == 1)
    assert(showCache(0).get(2).toString.equalsIgnoreCase("1/5 index files cached"))
    checkAnswer(sql("select * from partitionTable where col3='bb'"),
      Seq(Row(1, "aa", "bb", "cc", 1), Row(2, "aa", "bb", "gg", 2)))
    showCache = sql("SHOW METACACHE on table partitionTable").collect()
    result = CacheProvider.getInstance()
      .getCarbonCache
      .getCacheMap
      .keySet()
      .asScala
      .filter(index => index.startsWith(carbonTablePath))
    assert(result.exists(
      index => index.startsWith(
        carbonTablePath + CarbonCommonConstants.FILE_SEPARATOR + "col3=bb/col4=cc") ||
               index.startsWith(carbonTablePath + CarbonCommonConstants.FILE_SEPARATOR +
                                "col3=bb/col4=gg")) &&
           result.size == 2)
    assert(showCache(0).get(2).toString.equalsIgnoreCase("2/5 index files cached"))
    sql("drop table if exists partitionTable")
  }

  test("test index files cached for table with partition without filter") {
    sql("drop table if exists partitionTable")
    sql("create table partitionTable(col1 int, col2 string) " +
        "partitioned by (col3 string) stored as carbondata")
    sql("insert into partitionTable values(1,'aa','bb'),(1,'aa1','bb1')")
    sql("insert into partitionTable values(1,'cc','dd')")
    sql("insert into partitionTable values(2,'aa','bb')")
    sql("insert into partitionTable values(1,'aa','ee')")
    checkAnswer(sql("select * from partitionTable where col3='bb'"),
      Seq(Row(1, "aa", "bb"), Row(2, "aa", "bb")))
    var showCache = sql("SHOW METACACHE on table partitionTable").collect()
    val tableIdentifier = new TableIdentifier("partitionTable", Some("default"))
    val carbonTablePath = CarbonEnv
      .getCarbonTable(tableIdentifier)(sqlContext.sparkSession)
      .getTablePath
    var result = CacheProvider.getInstance()
      .getCarbonCache
      .getCacheMap
      .keySet()
      .asScala
      .filter(index => index.startsWith(carbonTablePath))
    assert(result.exists(index => index.startsWith(
      carbonTablePath + CarbonCommonConstants.FILE_SEPARATOR + "col3=bb")) && result.size == 2)
    assert(showCache(0).get(2).toString.equalsIgnoreCase("2/5 index files cached"))
    sql("select * from partitionTable").collect()
    showCache = sql("SHOW METACACHE on table partitionTable").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("5/5 index files cached"))
    sql("drop table if exists partitionTable")
  }

  test("test cache expiration using expiringMap") {
    sql("drop table if exists carbonTable")
    sql("create table carbonTable(col1 int, col2 string,col3 string) stored as carbondata " +
        "tblproperties('index_cache_expiration_seconds'='1')")
    sql("insert into carbonTable select 1, 'ab', 'vf'")
    checkAnswer(sql("select count(*) from carbonTable"), Seq(Row(1)))
    var showCache = sql("show metacache on table carbonTable").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("1/1 index files cached"))
    Thread.sleep(1000)
    showCache = sql("show metacache on table carbonTable").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("0/1 index files cached"))
    sql("drop table if exists carbonTable")
  }

  // Exclude when running with index server, as show cache rows count varies.
  test("test cache expiration using expiringMap with bloom", true) {
    sql("drop table if exists carbonTable")
    sql("create table carbonTable(col1 int, col2 string,col3 string) stored as carbondata " +
        "tblproperties('index_cache_expiration_seconds'='1')")
    sql("insert into carbonTable select 1, 'ab', 'vf'")
    sql("drop index if exists cache_2_bloom on carbonTable")
    sql("CREATE INDEX IF NOT EXISTS cache_2_bloom ON TABLE carbonTable (col3) AS 'bloomfilter' ")
    checkAnswer(sql("select count(*) from carbonTable where col3='vf'"), Seq(Row(1)))
    var showCache = sql("show metacache on table carbonTable").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("1/1 index files cached"))
    assertResult("bloomfilter")(showCache(1).getString(2))
    Thread.sleep(1000)
    showCache = sql("show metacache on table carbonTable").collect()
    assert(showCache.length == 1)
    assert(showCache(0).get(2).toString.equalsIgnoreCase("0/1 index files cached"))
    sql("drop table if exists carbonTable")
  }

  test("test cache expiration using expiringMap with invalid cache expiration time") {
    sql("drop table if exists carbonTable")
    intercept[MalformedCarbonCommandException] {
      sql("create table carbonTable(col1 int, col2 string,col3 string) stored as carbondata " +
        "tblproperties('index_cache_expiration_seconds'='ab')")
    }.getMessage.contains("Invalid cache_expiration_time value found: ab")
  }

  test("test cache expiration using expiringMap with alter set and unset tblproperties") {
    sql("drop table if exists carbonTable")
    sql("create table carbonTable(col1 int, col2 string,col3 string) stored as carbondata " +
        "tblproperties('index_cache_expiration_seconds'='5')")
    sql("insert into carbonTable select 1, 'ab', 'vf'")
    // check cache expiration with 10 seconds
    checkAnswer(sql("select count(*) from carbonTable"), Seq(Row(1)))
    var showCache = sql("show metacache on table carbonTable").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("1/1 index files cached"))
    Thread.sleep(10000)
    showCache = sql("show metacache on table carbonTable").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("0/1 index files cached"))
    // check cache expiration with 3 seconds with alter set
    sql("alter table carbontable set tblproperties('index_cache_expiration_seconds'='3')")
    sql("insert into carbonTable select 1, 'ab', 'vf'")
    checkAnswer(sql("select count(*) from carbonTable"), Seq(Row(2)))
    showCache = sql("show metacache on table carbonTable").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("2/2 index files cached"))
    Thread.sleep(3000)
    showCache = sql("show metacache on table carbonTable").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("0/2 index files cached"))
    // revert cache expiration property
    sql("ALTER TABLE carbonTable UNSET TBLPROPERTIES('index_cache_expiration_seconds')")
    checkAnswer(sql("select count(*) from carbonTable"), Seq(Row(2)))
    showCache = sql("show metacache on table carbonTable").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("2/2 index files cached"))
    Thread.sleep(3000)
    showCache = sql("show metacache on table carbonTable").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("2/2 index files cached"))
    sql("drop table if exists carbonTable")
  }

  test("test cache expiration using expiringMap more than one table") {
    sql("drop table if exists carbonTable1")
    sql("create table carbonTable1(col1 int, col2 string,col3 string) stored as carbondata " +
        "tblproperties('index_cache_expiration_seconds'='60')")
    sql("drop table if exists carbonTable2")
    sql("create table carbonTable2(col1 int, col2 string,col3 string) stored as carbondata " +
        "tblproperties('index_cache_expiration_seconds'='5')")
    sql("insert into carbonTable1 select 1, 'ab', 'vf'")
    sql("insert into carbonTable2 select 1, 'ab', 'vf'")
    checkAnswer(sql("select count(*) from carbonTable1"), Seq(Row(1)))
    checkAnswer(sql("select count(*) from carbonTable2"), Seq(Row(1)))
    Thread.sleep(5000)
    var showCache = sql("show metacache on table carbonTable2").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("0/1 index files cached"))
    showCache = sql("show metacache on table carbonTable1").collect()
    assert(showCache(0).get(2).toString.equalsIgnoreCase("1/1 index files cached"))
    sql("drop table if exists carbonTable1")
    sql("drop table if exists carbonTable2")
  }

  // Runs only when index server is enabled.
  test("test embedded pruning", false) {
    val mock: MockUp[CarbonInputFormat[Object]] = new MockUp[CarbonInputFormat[Object]]() {
      @Mock
      def getDistributedSplit(table: CarbonTable, filterResolverIntf: FilterResolverIntf,
          partitionNames: util.List[PartitionSpec], validSegments: util.List[Segment],
          invalidSegments: util.List[Segment], segmentsToBeRefreshed: util.List[String],
          isCountJob: Boolean, configuration: Configuration, missingSISegments: util.Set[String]):
      util.List[ExtendedBlocklet] = {
        IndexUtil.executeIndexJob(table, filterResolverIntf, IndexUtil.getEmbeddedJob,
          partitionNames, validSegments, invalidSegments, null, true,
          segmentsToBeRefreshed, isCountJob, false, configuration, missingSISegments)
      }
    }
    sql("drop table if exists maintable1")
    sql("create table maintable1(a string, b int, c string) stored as carbondata")
    sql("insert into maintable1 select 'k',1,'k'")
    checkAnswer(sql("select * from maintable1"), Seq(Row("k", 1, "k")))
    mock.tearDown()
  }

}
