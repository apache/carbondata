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

package org.apache.carbondata.spark.testsuite.datamap

import java.io.File
import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.test.util.CarbonQueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.dev.cgdatamap.{CoarseGrainDataMap, CoarseGrainDataMapFactory}
import org.apache.carbondata.core.datamap.dev.{DataMapBuilder, DataMapWriter}
import org.apache.carbondata.core.datamap.status.{DataMapStatus, DataMapStatusManager}
import org.apache.carbondata.core.datamap.{DataMapDistributable, DataMapMeta, Segment}
import org.apache.carbondata.core.datastore.block.SegmentProperties
import org.apache.carbondata.core.datastore.page.ColumnPage
import org.apache.carbondata.core.features.TableOperation
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.core.scan.filter.intf.ExpressionType
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events.Event

class TestDataMapStatus extends CarbonQueryTest with BeforeAndAfterAll {

  val testData = s"$resourcesPath/sample.csv"

  override def beforeAll: Unit = {
    new File(CarbonProperties.getInstance().getSystemFolderLocation).delete()
    drop
  }

  test("datamap status enable for new datamap") {
    sql("DROP TABLE IF EXISTS datamapstatustest")
    sql(
      """
        | CREATE TABLE datamapstatustest(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('sort_scope'='local_sort','sort_columns'='name,city')
      """.stripMargin)
    sql(
      s"""create datamap statusdatamap on table datamapstatustest
         |using '${classOf[TestDataMapFactory].getName}'
         |dmproperties('index_columns'='name')
         | """.stripMargin)

    val details = DataMapStatusManager.readDataMapStatusDetails()

    assert(details.length == 1)

    assert(details.exists(p => p.getDataMapName.equals("statusdatamap") && p.getStatus == DataMapStatus.ENABLED))
    sql("DROP TABLE IF EXISTS datamapstatustest")
  }

  test("datamap status disable for new datamap with deferred rebuild") {
    sql("DROP TABLE IF EXISTS datamapstatustest")
    sql(
      """
        | CREATE TABLE datamapstatustest(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('sort_scope'='local_sort','sort_columns'='name,city')
      """.stripMargin)
    sql(
      s"""create datamap statusdatamap on table datamapstatustest
         |using '${classOf[TestDataMapFactory].getName}'
         |with deferred rebuild
         |dmproperties('index_columns'='name')
         | """.stripMargin)

    val details = DataMapStatusManager.readDataMapStatusDetails()

    assert(details.length == 1)

    assert(details.exists(p => p.getDataMapName.equals("statusdatamap") && p.getStatus == DataMapStatus.DISABLED))
    sql("DROP TABLE IF EXISTS datamapstatustest")
  }

  test("datamap status disable after new load  with deferred rebuild") {
    sql("DROP TABLE IF EXISTS datamapstatustest1")
    sql(
      """
        | CREATE TABLE datamapstatustest1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('sort_scope'='local_sort','sort_columns'='name,city')
      """.stripMargin)
    sql(
      s"""create datamap statusdatamap1 on table datamapstatustest1
         |using '${classOf[TestDataMapFactory].getName}'
         |with deferred rebuild
         |dmproperties('index_columns'='name')
         | """.stripMargin)

    var details = DataMapStatusManager.readDataMapStatusDetails()

    assert(details.length == 1)

    assert(details.exists(p => p.getDataMapName.equals("statusdatamap1") && p.getStatus == DataMapStatus.DISABLED))

    sql(s"LOAD DATA LOCAL INPATH '$testData' into table datamapstatustest1")
    details = DataMapStatusManager.readDataMapStatusDetails()
    assert(details.length == 1)
    assert(details.exists(p => p.getDataMapName.equals("statusdatamap1") && p.getStatus == DataMapStatus.DISABLED))
    sql("DROP TABLE IF EXISTS datamapstatustest1")
  }

  test("datamap status with REBUILD DATAMAP") {
    sql("DROP TABLE IF EXISTS datamapstatustest2")
    sql(
      """
        | CREATE TABLE datamapstatustest2(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('sort_scope'='local_sort','sort_columns'='name,city')
      """.stripMargin)
    sql(
      s"""create datamap statusdatamap2 on table datamapstatustest2
         |using '${classOf[TestDataMapFactory].getName}'
         |with deferred rebuild
         |dmproperties('index_columns'='name')
         | """.stripMargin)

    var details = DataMapStatusManager.readDataMapStatusDetails()

    assert(details.length == 1)

    assert(details.exists(p => p.getDataMapName.equals("statusdatamap2") && p.getStatus == DataMapStatus.DISABLED))

    sql(s"LOAD DATA LOCAL INPATH '$testData' into table datamapstatustest2")
    details = DataMapStatusManager.readDataMapStatusDetails()
    assert(details.length == 1)
    assert(details.exists(p => p.getDataMapName.equals("statusdatamap2") && p.getStatus == DataMapStatus.DISABLED))

    sql(s"REBUILD DATAMAP statusdatamap2 on table datamapstatustest2")

    details = DataMapStatusManager.readDataMapStatusDetails()
    assert(details.length == 1)
    assert(details.exists(p => p.getDataMapName.equals("statusdatamap2") && p.getStatus == DataMapStatus.ENABLED))

    sql("DROP TABLE IF EXISTS datamapstatustest2")
  }

  test("datamap create without on table test") {
    sql("DROP TABLE IF EXISTS datamapstatustest3")
    sql(
      """
        | CREATE TABLE datamapstatustest3(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('sort_scope'='local_sort','sort_columns'='name,city')
      """.stripMargin)
    intercept[MalformedDataMapCommandException] {
      sql(
        s"""create datamap statusdatamap3
           |using '${classOf[TestDataMapFactory].getName}'
           |dmproperties('index_columns'='name')
           | """.stripMargin)

    }
    sql("DROP TABLE IF EXISTS datamapstatustest3")
  }

  test("rebuild datamap status") {
    sql("DROP TABLE IF EXISTS datamapstatustest3")
    sql(
      """
        | CREATE TABLE datamapstatustest3(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('sort_scope'='local_sort','sort_columns'='name,city')
      """.stripMargin)
    sql(
      s"""create datamap statusdatamap3 on table datamapstatustest3
         |using '${classOf[TestDataMapFactory].getName}'
         |with deferred rebuild
         |dmproperties('index_columns'='name')
         | """.stripMargin)

    var details = DataMapStatusManager.readDataMapStatusDetails()

    assert(details.length == 1)

    assert(details.exists(p => p.getDataMapName.equals("statusdatamap3") && p.getStatus == DataMapStatus.DISABLED))

    sql(s"LOAD DATA LOCAL INPATH '$testData' into table datamapstatustest3")
    details = DataMapStatusManager.readDataMapStatusDetails()
    assert(details.length == 1)
    assert(details.exists(p => p.getDataMapName.equals("statusdatamap3") && p.getStatus == DataMapStatus.DISABLED))

    sql(s"REBUILD DATAMAP statusdatamap3")

    details = DataMapStatusManager.readDataMapStatusDetails()
    assert(details.length == 1)
    assert(details.exists(p => p.getDataMapName.equals("statusdatamap3") && p.getStatus == DataMapStatus.ENABLED))

    checkExistence(sql(s"show datamap"), true, "statusdatamap3")

    sql("DROP TABLE IF EXISTS datamapstatustest3")
  }

  override def afterAll {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE,
      CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE_DEFAULT)
    drop
  }

  private def drop = {
    sql("drop table if exists datamapstatustest")
    sql("drop table if exists datamapshowtest")
    sql("drop table if exists datamapstatustest1")
    sql("drop table if exists datamapstatustest2")
  }
}

class TestDataMapFactory(
    carbonTable: CarbonTable,
    dataMapSchema: DataMapSchema) extends CoarseGrainDataMapFactory(carbonTable, dataMapSchema) {

  override def fireEvent(event: Event): Unit = ???

  override def clear(): Unit = {}

  override def getDataMaps(distributable: DataMapDistributable): util.List[CoarseGrainDataMap] = {
    ???
  }

  override def getDataMaps(segment: Segment): util.List[CoarseGrainDataMap] = {
    ???
  }

  override def createWriter(segment: Segment, shardName: String, segmentProperties: SegmentProperties): DataMapWriter = {
    new DataMapWriter(carbonTable.getTablePath, "testdm", carbonTable.getIndexedColumns(dataMapSchema),
      segment, shardName) {
      override def onPageAdded(blockletId: Int, pageId: Int, pageSize: Int, pages: Array[ColumnPage]): Unit = { }

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

  override def getMeta: DataMapMeta = new DataMapMeta(carbonTable.getIndexedColumns(dataMapSchema),
    Seq(ExpressionType.EQUALS).asJava)

  override def toDistributable(segmentId: Segment): util.List[DataMapDistributable] = {
    util.Collections.emptyList()
  }

  /**
   * delete datamap of the segment
   */
  override def deleteDatamapData(segment: Segment): Unit = {

  }

  /**
   * delete datamap data if any
   */
  override def deleteDatamapData(): Unit = {

  }

  /**
   * defines the features scopes for the datamap
   */
  override def willBecomeStale(operation: TableOperation): Boolean = {
    false
  }

  override def createBuilder(segment: Segment,
      shardName: String, segmentProperties: SegmentProperties): DataMapBuilder = {
    return new DataMapBuilder {
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
}
