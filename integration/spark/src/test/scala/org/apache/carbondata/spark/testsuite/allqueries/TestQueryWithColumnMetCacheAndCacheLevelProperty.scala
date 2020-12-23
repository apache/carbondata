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

import scala.collection.JavaConverters._

import mockit.{Mock, MockUp}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.index.{IndexChooser, IndexFilter, IndexStoreManager, Segment, TableIndex}
import org.apache.carbondata.core.index.dev.Index
import org.apache.carbondata.core.indexstore.Blocklet
import org.apache.carbondata.core.indexstore.blockletindex.{BlockIndex, BlockletIndex, BlockletIndexRowIndexes}
import org.apache.carbondata.core.indexstore.schema.CarbonRowSchema
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.readcommitter.TableStatusReadCommittedScope
import org.apache.carbondata.core.scan.expression.{ColumnExpression, LiteralExpression}
import org.apache.carbondata.core.scan.expression.conditional.NotEqualsExpression
import org.apache.carbondata.core.scan.expression.logical.AndExpression
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf
import org.apache.carbondata.core.util.CarbonProperties

/**
 * test class for validating COLUMN_META_CACHE and CACHE_LEVEL
 */
class TestQueryWithColumnMetCacheAndCacheLevelProperty
  extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach {

  override def beforeAll(): Unit = {
    dropSchema
  }

  override def afterEach(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MINMAX_ALLOWED_BYTE_COUNT,
        CarbonCommonConstants.CARBON_MINMAX_ALLOWED_BYTE_COUNT_DEFAULT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_LOAD_INDEXES_PARALLEL,
        CarbonCommonConstants.CARBON_LOAD_INDEXES_PARALLEL_DEFAULT)
    dropSchema
  }

  private def dropSchema: Unit = {
    sql("drop table if exists metaCache")
    sql("drop table if exists column_min_max_cache_test")
    sql("drop table if exists minMaxSerialize")
    sql("drop table if exists parallel_index_load")
  }

  private def createAndLoadTable(cacheLevel: String): Unit = {
    // scalastyle:off lineLength
    sql(s"CREATE table column_min_max_cache_test (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp, attendance int, utilization int,salary int) STORED AS carbondata TBLPROPERTIES('column_meta_cache'='workgroupcategoryname,designation,salary,attendance', 'CACHE_LEVEL'= '$cacheLevel')")
    // scalastyle:on lineLength
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO " +
        "TABLE column_min_max_cache_test OPTIONS('DELIMITER'=',', " +
        "'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")
  }

  private def getIndexes(
      dbName: String,
      tableName: String,
      segmentId: String,
      isSchemaModified: Boolean = false): List[Index[_ <: Blocklet]] = {
    val relation: CarbonRelation = CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore
      .lookupRelation(Some(dbName), tableName)(sqlContext.sparkSession)
      .asInstanceOf[CarbonRelation]
    val carbonTable = relation.carbonTable
    assert(carbonTable.getTableInfo.isSchemaModified == isSchemaModified)
    val segment: Segment = Segment.getSegment(segmentId, carbonTable.getTablePath)
    val defaultIndex: TableIndex = IndexStoreManager.getInstance()
      .getDefaultIndex(carbonTable)
    defaultIndex.getIndexFactory.getIndexes(segment).asScala.toList
  }

  private def validateMinMaxColumnsCacheLength(indexes: List[Index[_ <: Blocklet]],
      expectedLength: Int, storeBlockletCount: Boolean = false): Boolean = {
    val segmentPropertiesWrapper = indexes(0).asInstanceOf[BlockIndex].getSegmentPropertiesWrapper
    val summarySchema =
      segmentPropertiesWrapper.getTaskSummarySchemaForBlock(storeBlockletCount, false)
    val minSchemas = summarySchema(BlockletIndexRowIndexes.TASK_MIN_VALUES_INDEX)
      .asInstanceOf[CarbonRowSchema.StructCarbonRowSchema]
      .getChildSchemas
    minSchemas.length == expectedLength
  }

  test("verify if number of columns cached are as per the COLUMN_META_CACHE property " +
       "index instance is as per CACHE_LEVEL property") {
    sql("create table metaCache(name string, c1 string, c2 string) STORED AS carbondata")
    sql("insert into metaCache select 'a','aa','aaa'")
    checkAnswer(sql("select * from metaCache"), Row("a", "aa", "aaa"))
    var indexes = getIndexes("default", "metaCache", "0")
    // validate index is non empty, its an instance of BlockIndex and minMaxSchema length is 3
    assert(indexes.nonEmpty)
    assert(indexes(0).isInstanceOf[BlockIndex])
    assert(validateMinMaxColumnsCacheLength(indexes, 3, true))
    // alter table to add column_meta_cache and cache_level
    sql("alter table metaCache " +
        "set tblproperties('column_meta_cache'='c2,c1', 'CACHE_LEVEL'='BLOCKLET')")
    // after alter operation cache should be cleaned and cache should be evicted
    checkAnswer(sql("select * from metaCache"), Row("a", "aa", "aaa"))
    // validate index is non empty, its an instance of BlockletIndex and minMaxSchema length
    // is 1
    indexes = getIndexes("default", "metaCache", "0")
    assert(indexes.nonEmpty)
    assert(indexes(0).isInstanceOf[BlockletIndex])
    assert(validateMinMaxColumnsCacheLength(indexes, 2))

    // alter table to add same value as previous with order change
    // for column_meta_cache and cache_level
    sql("alter table metaCache " +
      "set tblproperties('column_meta_cache'='c1,c2', 'CACHE_LEVEL'='BLOCKLET')")
    sql(
      "alter table metaCache set tblproperties('column_meta_cache'='')")
    // after alter operation cache should be cleaned and cache should be evicted
    checkAnswer(sql("select * from metaCache"), Row("a", "aa", "aaa"))
    // validate index is non empty, its an instance of BlockletIndex and minMaxSchema length
    // is 0
    indexes = getIndexes("default", "metaCache", "0")
    assert(indexes.nonEmpty)
    assert(indexes(0).isInstanceOf[BlockletIndex])
    assert(validateMinMaxColumnsCacheLength(indexes, 0))

    // alter table to cache no column in column_meta_cache
    sql(
      "alter table metaCache unset tblproperties('column_meta_cache', 'cache_level')")
    checkAnswer(sql("select * from metaCache"), Row("a", "aa", "aaa"))
    // validate index is non empty, its an instance of BlockletIndex and minMaxSchema length
    // is 3
    indexes = getIndexes("default", "metaCache", "0")
    assert(indexes.nonEmpty)
    assert(indexes(0).isInstanceOf[BlockIndex])
    assert(validateMinMaxColumnsCacheLength(indexes, 3))
  }

  test("test UPDATE scenario after column_meta_cache") {
    sql("create table metaCache(name string, c1 string, c2 string) " +
        "STORED AS carbondata TBLPROPERTIES('COLUMN_META_CACHE'='')")
    sql("insert into metaCache select 'a','aa','aaa'")
    sql("insert into metaCache select 'b','bb','bbb'")
    sql("update metaCache set(c1)=('new_c1') where c1='aa'").collect()
    checkAnswer(sql("select c1 from metaCache"), Seq(Row("new_c1"), Row("bb")))
  }

  test("test queries with column_meta_cache and cache_level='BLOCK'") {
    // set cache_level
    createAndLoadTable("BLOCK")
    // check count(*)
    checkAnswer(sql("select count(*) from column_min_max_cache_test"), Row(10))
    // check query on cached dimension columns
    checkAnswer(
      sql("select count(*) from column_min_max_cache_test " +
          "where workgroupcategoryname='developer' OR designation='PL'"),
      Row(6))
    // check query on cached dimension column and non cached column
    checkAnswer(sql(
      "select count(*) from column_min_max_cache_test where empname='pramod' and " +
      "workgroupcategoryname='developer'"),
      Row(1))
    // query on cached column
    checkAnswer(sql(
      "select count(*) from column_min_max_cache_test where workgroupcategoryname='developer'"),
      Row(5))
    // check query on non cached column
    checkAnswer(sql(
      "select count(*) from column_min_max_cache_test where empname='pramod' and " +
      "deptname='network'"),
      Row(0))
    // check query on cached dimension and measure column
    checkAnswer(sql(
      "select count(*) from column_min_max_cache_test where attendance='77' and " +
      "salary='11248' and workgroupcategoryname='manager'"),
      Row(1))
    // check query on cached dimension and measure column with one non cached column
    checkAnswer(sql(
      "select count(*) from column_min_max_cache_test where attendance='77' and " +
      "salary='11248' OR deptname='network'"),
      Row(4))
  }

  test("test queries with column_meta_cache and cache_level='BLOCKLET'") {
    // set cache_level
    createAndLoadTable("BLOCKLET")
    // check count(*)
    checkAnswer(sql("select count(*) from column_min_max_cache_test"), Row(10))
    // check query on cached dimension columns
    checkAnswer(
      sql("select count(*) from column_min_max_cache_test " +
          "where workgroupcategoryname='developer' OR designation='PL'"),
      Row(6))
    // check query on cached dimension column and non cached column
    checkAnswer(sql(
      "select count(*) from column_min_max_cache_test where empname='pramod' and " +
      "workgroupcategoryname='developer'"),
      Row(1))
    // query on cached column
    checkAnswer(sql(
      "select count(*) from column_min_max_cache_test where workgroupcategoryname='developer'"),
      Row(5))
    // check query on non cached column
    checkAnswer(sql(
      "select count(*) from column_min_max_cache_test where empname='pramod' and " +
      "deptname='network'"),
      Row(0))
    // check query on cached dimension and measure column
    checkAnswer(sql(
      "select count(*) from column_min_max_cache_test where attendance='77' and " +
      "salary='11248' and workgroupcategoryname='manager'"),
      Row(1))
    // check query on cached dimension and measure column with one non cached column
    checkAnswer(sql(
      "select count(*) from column_min_max_cache_test where attendance='77' and " +
      "salary='11248' OR deptname='network'"),
      Row(4))
  }

  test("test update on column cached") {
    // set cache_level
    createAndLoadTable("BLOCKLET")
    sql("update column_min_max_cache_test set (designation)=('SEG') where empname='ayushi'")
      .collect()
    checkAnswer(sql(
      "select count(*) from column_min_max_cache_test where empname='ayushi' and " +
      "designation='SEG'"),
      Row(1))
  }

  test("test update on column not cached") {
    // set cache_level
    createAndLoadTable("BLOCKLET")
    sql(
      "update column_min_max_cache_test set (workgroupcategoryname)=('solution engrr') where " +
      "workgroupcategoryname='developer'")
      .collect()
    checkAnswer(sql(
      "select count(*) from column_min_max_cache_test where workgroupcategoryname='solution " +
      "engrr'"),
      Row(5))
  }

  test("verify column caching with alter add column") {
    // scalastyle:off lineLength
    sql("drop table if exists alter_add_column_min_max")
    sql("create table alter_add_column_min_max (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED AS carbondata TBLPROPERTIES('table_blocksize'='1','COLUMN_META_CACHE'='AMSize','CACHE_LEVEL'='BLOCKLET')")
    sql("insert into alter_add_column_min_max select '1AA1','8RAM size','4','Chinese','guangzhou',2738,1,'2014-07-01 12:07:28','2014-07-01 12:07:28',25")
    sql("alter table alter_add_column_min_max add columns(age int, name string)")
    sql("ALTER TABLE alter_add_column_min_max SET TBLPROPERTIES('COLUMN_META_CACHE'='age,name')")
    sql("insert into alter_add_column_min_max select '1AA1','8RAM size','4','Chinese','guangzhou',2738,1,'2014-07-01 12:07:28','2014-07-01 12:07:28',25,29,'Rahul'")
    checkAnswer(sql("select count(*) from alter_add_column_min_max where AMSize='8RAM size'"), Row(2))
    sql("drop table if exists alter_add_column_min_max")
    // scalastyle:on lineLength
  }

  test("verify min/max getting serialized to executor when cache_level = blocklet") {
    sql("create table minMaxSerialize(name string, c1 string, c2 string) " +
        "STORED AS carbondata TBLPROPERTIES('CACHE_LEVEL'='BLOCKLET', 'COLUMN_META_CACHE'='c1,c2')")
    sql("insert into minMaxSerialize select 'a','aa','aaa'")
    checkAnswer(sql("select * from minMaxSerialize where name='a'"), Row("a", "aa", "aaa"))
    checkAnswer(sql("select * from minMaxSerialize where name='b'"), Seq.empty)
    val relation: CarbonRelation = CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore
      .lookupRelation(Some("default"), "minMaxSerialize")(sqlContext.sparkSession)
      .asInstanceOf[CarbonRelation]
    val carbonTable = relation.carbonTable
    // form a filter expression and generate filter resolver tree
    val columnExpression = new ColumnExpression("name", DataTypes.STRING)
    columnExpression.setDimension(true)
    val dimension: CarbonDimension = carbonTable.getDimensionByName("name")
    columnExpression.setDimension(dimension)
    columnExpression.setCarbonColumn(dimension)
    val literalValueExpression = new LiteralExpression("a", DataTypes.STRING)
    val literalNullExpression = new LiteralExpression(null, DataTypes.STRING)
    val notEqualsExpression = new NotEqualsExpression(columnExpression, literalNullExpression)
    val equalsExpression = new NotEqualsExpression(columnExpression, literalValueExpression)
    val andExpression = new AndExpression(notEqualsExpression, equalsExpression)
    val resolveFilter: FilterResolverIntf =
      new IndexFilter(carbonTable, andExpression).getResolver()
    val exprWrapper = IndexChooser.getDefaultIndex(carbonTable, resolveFilter)
    val segment = new Segment("0", new TableStatusReadCommittedScope(carbonTable
      .getAbsoluteTableIdentifier, new Configuration(false)))
    // get the pruned blocklets
    val prunedBlocklets = exprWrapper.prune(List(segment).asJava, null)
    prunedBlocklets.asScala.foreach { blocklet =>
      // all the blocklets should have useMinMaxForPrune flag set to true
      assert(blocklet.getDetailInfo.isUseMinMaxForPruning)
    }
  }

  test("Test For Cache set but Min/Max exceeds") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MINMAX_ALLOWED_BYTE_COUNT, "30")
    sql("DROP TABLE IF EXISTS carbonCache")
    sql(
      s"""
         | CREATE TABLE carbonCache (
         | name STRING,
         | age STRING,
         | desc STRING
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('COLUMN_META_CACHE'='name,desc')
       """.stripMargin)
    sql(
      "INSERT INTO carbonCache values('Manish Nalla','24'," +
      "'gvsahgvsahjvcsahjgvavacavkjvaskjvsahgsvagkjvkjgvsackjg')")
    checkAnswer(sql(
      "SELECT count(*) FROM carbonCache where " +
      "desc='gvsahgvsahjvcsahjgvavacavkjvaskjvsahgsvagkjvkjgvsackjg'"),
      Row(1))
    sql("DROP table IF EXISTS carbonCahe")
  }
  test("Test For Cache set but Min/Max exceeds with Cache Level as Blocklet") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MINMAX_ALLOWED_BYTE_COUNT, "30")
    sql("DROP TABLE IF EXISTS carbonCache")
    sql(
      s"""
         | CREATE TABLE carbonCache (
         | name STRING,
         | age STRING,
         | desc STRING
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('COLUMN_META_CACHE'='name,desc','CACHE_LEVEL'='BLOCKLET')
       """.stripMargin)
    sql(
      "INSERT INTO carbonCache values('Manish Nalla','24'," +
      "'gvsahgvsahjvcsahjgvavacavkjvaskjvsahgsvagkjvkjgvsackjg')")
    checkAnswer(sql(
      "SELECT count(*) FROM carbonCache where " +
      "desc='gvsahgvsahjvcsahjgvavacavkjvaskjvsahgsvagkjvkjgvsackjg'"),
      Row(1))
    sql("DROP table IF EXISTS carbonCahe")
  }

  test("Test query with parallel index load with and without index") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_LOAD_INDEXES_PARALLEL, "true")
    sql("CREATE table parallel_index_load (a STRING, b STRING, c INT) STORED AS carbondata")
    sql("insert into parallel_index_load select 'aa', 'bb', 1")
    sql("insert into parallel_index_load select 'cc', 'dd', 2")
    sql("insert into parallel_index_load select 'ee', 'ff', 3")
    sql("select a, b from parallel_index_load").collect()
    assert(sql("select a, b from parallel_index_load").count() == 3)
    sql("drop index if exists parallel_index on parallel_index_load")
    sql("CREATE INDEX parallel_index on parallel_index_load(b) AS 'carbondata'")
    checkAnswer(sql("select b from parallel_index"), Seq(Row("bb"), Row("dd"), Row("ff")))
    sql("drop index if exists parallel_index on parallel_index_load")
    val mock: MockUp[TableInfo] = new MockUp[TableInfo] {
      @Mock
      def isSchemaModified(): Boolean = {
        true
      }
    }
    sql("CREATE INDEX parallel_index on parallel_index_load(b) AS 'carbondata'")
    checkAnswer(sql("select b from parallel_index"), Seq(Row("bb"), Row("dd"), Row("ff")))
    sql("drop index if exists parallel_index on parallel_index_load")
    mock.tearDown()
  }
}
