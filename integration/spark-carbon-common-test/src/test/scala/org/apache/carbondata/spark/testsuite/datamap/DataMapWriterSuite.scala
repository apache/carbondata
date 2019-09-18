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

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.{DataMapDistributable, DataMapMeta, Segment}
import org.apache.carbondata.core.datamap.dev.{DataMapBuilder, DataMapWriter}
import org.apache.carbondata.core.datamap.dev.cgdatamap.{CoarseGrainDataMap, CoarseGrainDataMapFactory}
import org.apache.carbondata.core.datastore.block.SegmentProperties
import org.apache.carbondata.core.datastore.page.ColumnPage
import org.apache.carbondata.core.features.TableOperation
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.core.scan.filter.intf.ExpressionType
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events.Event

class C2DataMapFactory(
    carbonTable: CarbonTable,
    dataMapSchema: DataMapSchema) extends CoarseGrainDataMapFactory(carbonTable, dataMapSchema) {

  var identifier: AbsoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier

  override def fireEvent(event: Event): Unit = ???

  override def clear(): Unit = {}

  override def getDataMaps(distributable: DataMapDistributable): util.List[CoarseGrainDataMap] = ???

  override def getDataMaps(segment: Segment): util.List[CoarseGrainDataMap] = ???

  override def createWriter(segment: Segment, shardName: String, segmentProperties: SegmentProperties): DataMapWriter =
    DataMapWriterSuite.dataMapWriterC2Mock(identifier, "testdm", segment, shardName)

  override def getMeta: DataMapMeta =
    new DataMapMeta(carbonTable.getIndexedColumns(dataMapSchema), List(ExpressionType.EQUALS).asJava)

  /**
   * Get all distributable objects of a segmentId
   *
   * @return
   */
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
    ???
  }
}

class DataMapWriterSuite extends QueryTest with BeforeAndAfterAll {
  def buildTestData(numRows: Int): DataFrame = {
    import sqlContext.implicits._
    sqlContext.sparkContext.parallelize(1 to numRows, 1)
      .map(x => ("a" + x, "b", x))
      .toDF("c1", "c2", "c3")
  }

  def dropTable(): Unit = {
    sql("DROP TABLE IF EXISTS carbon1")
    sql("DROP TABLE IF EXISTS carbon2")
  }

  override def beforeAll {
    dropTable()
  }

  test("test write datamap 2 pages") {
    sql(s"CREATE TABLE carbon1(c1 STRING, c2 STRING, c3 INT) STORED BY 'org.apache.carbondata.format'")
    // register datamap writer
    sql(
      s"""
         | CREATE DATAMAP test1 ON TABLE carbon1
         | USING '${classOf[C2DataMapFactory].getName}'
         | DMPROPERTIES('index_columns'='c2')
       """.stripMargin)
    val df = buildTestData(33000)

    // save dataframe to carbon file
    df.write
      .format("carbondata")
      .option("tableName", "carbon1")
      .option("tempCSV", "false")
      .option("sort_columns","c1")
      .mode(SaveMode.Overwrite)
      .save()

    assert(DataMapWriterSuite.callbackSeq.head.contains("block start"))
    assert(DataMapWriterSuite.callbackSeq.last.contains("block end"))
    assert(
      DataMapWriterSuite.callbackSeq.slice(1, DataMapWriterSuite.callbackSeq.length - 1) == Seq(
        "blocklet start 0",
        "add page data: blocklet 0, page 0",
        "add page data: blocklet 0, page 1",
        "blocklet end: 0"
      ))
    DataMapWriterSuite.callbackSeq = Seq()
  }

  test("test write datamap 2 blocklet") {
    sql(s"CREATE TABLE carbon2(c1 STRING, c2 STRING, c3 INT) STORED BY 'org.apache.carbondata.format'")
    sql(
      s"""
         | CREATE DATAMAP test2 ON TABLE carbon2
         | USING '${classOf[C2DataMapFactory].getName}'
         | DMPROPERTIES('index_columns'='c2')
       """.stripMargin)
    CarbonProperties.getInstance()
      .addProperty("carbon.blockletgroup.size.in.mb", "16")
    CarbonProperties.getInstance()
      .addProperty("carbon.number.of.cores.while.loading",
        CarbonCommonConstants.NUM_CORES_DEFAULT_VAL)

    val df = buildTestData(300000)

    // save dataframe to carbon file
    df.write
      .format("carbondata")
      .option("tableName", "carbon2")
      .option("tempCSV", "false")
      .option("sort_columns","c1")
      .option("SORT_SCOPE","GLOBAL_SORT")
      .mode(SaveMode.Overwrite)
      .save()

    assert(DataMapWriterSuite.callbackSeq.head.contains("block start"))
    assert(DataMapWriterSuite.callbackSeq.last.contains("block end"))
    // corrected test case the min "carbon.blockletgroup.size.in.mb" size could not be less than
    // 64 MB
    assert(
      DataMapWriterSuite.callbackSeq.slice(1, DataMapWriterSuite.callbackSeq.length - 1) == Seq(
        "blocklet start 0",
        "add page data: blocklet 0, page 0",
        "add page data: blocklet 0, page 1",
        "add page data: blocklet 0, page 2",
        "add page data: blocklet 0, page 3",
        "add page data: blocklet 0, page 4",
        "add page data: blocklet 0, page 5",
        "add page data: blocklet 0, page 6",
        "add page data: blocklet 0, page 7",
        "add page data: blocklet 0, page 8",
        "add page data: blocklet 0, page 9",
        "blocklet end: 0"
      ))
    DataMapWriterSuite.callbackSeq = Seq()
  }

  override def afterAll {
    dropTable()
  }
}

object DataMapWriterSuite {

  var callbackSeq: Seq[String] = Seq[String]()

  def dataMapWriterC2Mock(identifier: AbsoluteTableIdentifier, dataMapName:String, segment: Segment,
      shardName: String) =
    new DataMapWriter(identifier.getTablePath, dataMapName, Seq().asJava, segment, shardName) {

      override def onPageAdded(
          blockletId: Int,
          pageId: Int,
          pageSize: Int,
          pages: Array[ColumnPage]): Unit = {
        assert(pages.length == 1)
        assert(pages(0).getDataType == DataTypes.STRING)
        val bytes: Array[Byte] = pages(0).getBytes(0)
        assert(bytes.sameElements(Seq(0, 1, 'b'.toByte)))
        callbackSeq :+= s"add page data: blocklet $blockletId, page $pageId"
      }

    override def onBlockletEnd(blockletId: Int): Unit = {
      callbackSeq :+= s"blocklet end: $blockletId"
    }

    override def onBlockEnd(blockId: String): Unit = {
      callbackSeq :+= s"block end $blockId"
    }

    override def onBlockletStart(blockletId: Int): Unit = {
      callbackSeq :+= s"blocklet start $blockletId"
    }

    /**
     * Start of new block notification.
     *
     * @param blockId file name of the carbondata file
     */
    override def onBlockStart(blockId: String): Unit = {
      callbackSeq :+= s"block start $blockId"
    }

    /**
     * This is called during closing of writer.So after this call no more data will be sent to this
     * class.
     */
    override def finish() = {

    }
  }
}