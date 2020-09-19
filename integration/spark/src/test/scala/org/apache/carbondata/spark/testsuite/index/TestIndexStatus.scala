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

package org.apache.carbondata.spark.testsuite.index

import java.util

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.block.SegmentProperties
import org.apache.carbondata.core.datastore.page.ColumnPage
import org.apache.carbondata.core.features.TableOperation
import org.apache.carbondata.core.index.{IndexInputSplit, IndexMeta, Segment}
import org.apache.carbondata.core.index.dev.{IndexBuilder, IndexWriter}
import org.apache.carbondata.core.index.dev.cgindex.{CoarseGrainIndex, CoarseGrainIndexFactory}
import org.apache.carbondata.core.index.status.IndexStatus
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, IndexSchema}
import org.apache.carbondata.core.scan.filter.intf.ExpressionType
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events.Event

class TestIndexStatus extends QueryTest with BeforeAndAfterAll {

  val testData = s"$resourcesPath/sample.csv"

  override def beforeAll: Unit = {
    drop
  }

  test("indexSchema status enable for new indexSchema") {
    sql("DROP TABLE IF EXISTS indexstatustest")
    sql(
      """
        | CREATE TABLE indexstatustest(id int, name string, city string, age int)
        | STORED AS carbondata TBLPROPERTIES('sort_scope'='local_sort','sort_columns'='name,city')
      """.stripMargin)
    sql(
      s"""create index statusindex on table indexstatustest (name)
         |as '${classOf[TestIndexFactory].getName}'
         | """.stripMargin)
    checkIndexStatus("indexstatustest", "statusindex", IndexStatus.ENABLED.name())
    sql("DROP TABLE IF EXISTS indexstatustest")
  }

  test("indexSchema status disable for new indexSchema with deferred rebuild") {
    sql("DROP TABLE IF EXISTS indexstatustest")
    sql(
      """
        | CREATE TABLE indexstatustest(id int, name string, city string, age int)
        | STORED AS carbondata TBLPROPERTIES('sort_scope'='local_sort','sort_columns'='name,city')
      """.stripMargin)
    sql(
      s"""create index statusindex
         |on table indexstatustest (name)
         |as '${classOf[TestIndexFactory].getName}'
         |with deferred refresh
         | """.stripMargin)
    checkIndexStatus("indexstatustest", "statusindex", IndexStatus.DISABLED.name())
    sql("DROP TABLE IF EXISTS indexstatustest")
  }

  test("indexSchema status disable after new load  with deferred rebuild") {
    sql("DROP TABLE IF EXISTS indexstatustest1")
    sql(
      """
        | CREATE TABLE indexstatustest1(id int, name string, city string, age int)
        | STORED AS carbondata TBLPROPERTIES('sort_scope'='local_sort','sort_columns'='name,city')
      """.stripMargin)
    sql(
      s"""create index statusindex1
         |on table indexstatustest1 (name)
         |as '${classOf[TestIndexFactory].getName}'
         |with deferred refresh
         | """.stripMargin)

    checkIndexStatus("indexstatustest1", "statusindex1", IndexStatus.DISABLED.name())

    sql(s"LOAD DATA LOCAL INPATH '$testData' into table indexstatustest1")
    checkIndexStatus("indexstatustest1", "statusindex1", IndexStatus.DISABLED.name())
    sql("DROP TABLE IF EXISTS indexstatustest1")
  }

  test("indexSchema status with REFRESH INDEX") {
    sql("DROP TABLE IF EXISTS indexstatustest2")
    sql(
      """
        | CREATE TABLE indexstatustest2(id int, name string, city string, age int)
        | STORED AS carbondata TBLPROPERTIES('sort_scope'='local_sort','sort_columns'='name,city')
      """.stripMargin)
    sql(
      s"""create index statusindex2
         | on table indexstatustest2 (name)
         |as '${classOf[TestIndexFactory].getName}'
         |with deferred refresh
         | """.stripMargin)

    checkIndexStatus("indexstatustest2", "statusindex2", IndexStatus.DISABLED.name())

    sql(s"LOAD DATA LOCAL INPATH '$testData' into table indexstatustest2")
    checkIndexStatus("indexstatustest2", "statusindex2", IndexStatus.DISABLED.name())

    sql(s"REFRESH INDEX statusindex2 on table indexstatustest2")
    checkIndexStatus("indexstatustest2", "statusindex2", IndexStatus.ENABLED.name())
    sql("DROP TABLE IF EXISTS indexstatustest2")
  }

  private def checkIndexStatus(tableName: String, indexName: String, indexStatus: String): Unit = {
    val carbonTable = CarbonEnv.getCarbonTable(Some(CarbonCommonConstants.DATABASE_DEFAULT_NAME),
      tableName)(sqlContext.sparkSession)
    val indexes = carbonTable.getIndexMetadata
      .getIndexesMap
      .get({ classOf[TestIndexFactory].getName })
      .asScala
      .filter(p => p._2.get(CarbonCommonConstants.INDEX_STATUS).equalsIgnoreCase(indexStatus))
    assert(indexes.keySet.size == 1)
    assert(indexes.exists(p => p._1.equals(indexName) &&
                               p._2.get(CarbonCommonConstants.INDEX_STATUS) == indexStatus))
  }

  override def afterAll {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE,
      CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE_DEFAULT)
    drop
  }

  private def drop = {
    sql("drop table if exists indexstatustest")
    sql("drop table if exists indexshowtest")
    sql("drop table if exists indexstatustest1")
    sql("drop table if exists indexstatustest2")
  }
}

class TestIndexFactory(
    carbonTable: CarbonTable,
    indexSchema: IndexSchema) extends CoarseGrainIndexFactory(carbonTable, indexSchema) {
  // scalastyle:off ???
  override def fireEvent(event: Event): Unit = ???

  override def clear(): Unit = {}

  override def getIndexes(distributable: IndexInputSplit): util.List[CoarseGrainIndex] = {
    ???
  }

  override def getIndexes(segment: Segment): util.List[CoarseGrainIndex] = {
    ???
  }

  override def createWriter(segment: Segment,
      shardName: String,
      segmentProperties: SegmentProperties): IndexWriter = {
    new IndexWriter(carbonTable.getTablePath,
      "testdm",
      carbonTable.getIndexedColumns(indexSchema.getIndexColumns),
      segment,
      shardName) {
      override def onPageAdded(blockletId: Int,
          pageId: Int,
          pageSize: Int,
          pages: Array[ColumnPage]): Unit = {}

      override def onBlockletEnd(blockletId: Int): Unit = { }

      override def onBlockEnd(blockId: String): Unit = { }

      override def onBlockletStart(blockletId: Int): Unit = { }

      override def onBlockStart(blockId: String): Unit = {
        // trigger the second SQL to execute
      }

      override def finish(): Unit = {

      }
    }
  }

  override def getMeta: IndexMeta = {
    new IndexMeta(carbonTable.getIndexedColumns(indexSchema.getIndexColumns),
      Seq(ExpressionType.EQUALS).asJava)
  }

  override def toDistributable(segmentId: Segment): util.List[IndexInputSplit] = {
    util.Collections.emptyList()
  }

  /**
   * delete indexSchema of the segment
   */
  override def deleteIndexData(segment: Segment): Unit = {

  }

  /**
   * delete indexSchema data if any
   */
  override def deleteIndexData(): Unit = {

  }

  /**
   * defines the features scopes for the indexSchema
   */
  override def willBecomeStale(operation: TableOperation): Boolean = {
    false
  }

  override def createBuilder(segment: Segment,
      shardName: String, segmentProperties: SegmentProperties): IndexBuilder = {
    return new IndexBuilder {
      override def initialize(): Unit = { }

      override def addRow(blockletId: Int,
          pageId: Int,
          rowId: Int,
          values: Array[AnyRef]): Unit = { }

      override def finish(): Unit = { }

      override def close(): Unit = { }

      /**
       * whether create index on internal carbon bytes (such as dictionary encoded) or original
       * value
       */
      override def isIndexForCarbonRawBytes: Boolean = {
        false
      }
    }
  }

  override def supportRebuild(): Boolean = true

  /**
   * Get the indexSchema for segmentId and partitionSpecs
   */
  override def getIndexes(segment: Segment,
      partitionLocations: util.Set[Path]): util.List[CoarseGrainIndex] = {
    ???
  }
  // scalastyle:on ???
}
