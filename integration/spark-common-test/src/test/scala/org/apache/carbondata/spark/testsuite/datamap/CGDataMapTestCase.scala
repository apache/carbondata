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

import java.io.{ByteArrayInputStream, DataOutputStream, ObjectInputStream, ObjectOutputStream}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.datamap.{DataMapDistributable, DataMapMeta, Segment}
import org.apache.carbondata.core.datamap.dev.{DataMapModel, DataMapWriter}
import org.apache.carbondata.core.datamap.dev.cgdatamap.{CoarseGrainDataMap, CoarseGrainDataMapFactory}
import org.apache.carbondata.core.datastore.FileReader
import org.apache.carbondata.core.datastore.block.SegmentProperties
import org.apache.carbondata.core.datastore.compression.SnappyCompressor
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.datastore.page.ColumnPage
import org.apache.carbondata.core.indexstore.{Blocklet, PartitionSpec}
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapDistributable
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata}
import org.apache.carbondata.core.readcommitter.ReadCommittedScope
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression
import org.apache.carbondata.core.scan.filter.intf.ExpressionType
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf
import org.apache.carbondata.core.util.{ByteUtil, CarbonProperties}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.Event
import org.apache.carbondata.spark.testsuite.datacompaction.CompactionSupportGlobalSortBigFileTest

class CGDataMapFactory extends CoarseGrainDataMapFactory {
  var identifier: AbsoluteTableIdentifier = _
  var dataMapSchema: DataMapSchema = _

  /**
   * Initialization of Datamap factory with the identifier and datamap name
   */
  override def init(carbonTable: CarbonTable, dataMapSchema: DataMapSchema): Unit = {
    this.identifier = carbonTable.getAbsoluteTableIdentifier
    this.dataMapSchema = dataMapSchema
  }

  /**
   * Return a new write for this datamap
   */
  override def createWriter(segment: Segment, dataWritePath: String): DataMapWriter = {
    new CGDataMapWriter(identifier, segment, dataWritePath, dataMapSchema)
  }

  /**
   * Get the datamap for segmentid
   */
  override def getDataMaps(segment: Segment, readCommitted: ReadCommittedScope): java.util.List[CoarseGrainDataMap] = {
    val file = FileFactory.getCarbonFile(
      CarbonTablePath.getSegmentPath(identifier.getTablePath, segment.getSegmentNo))

    val files = file.listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = file.getName.endsWith(".datamap")
    })
    files.map {f =>
      val dataMap: CoarseGrainDataMap = new CGDataMap()
      dataMap.init(new DataMapModel(f.getCanonicalPath))
      dataMap
    }.toList.asJava
  }


  /**
   * Get datamaps for distributable object.
   */
  override def getDataMaps(distributable: DataMapDistributable, readCommitted: ReadCommittedScope): java.util.List[CoarseGrainDataMap] = {
    val mapDistributable = distributable.asInstanceOf[BlockletDataMapDistributable]
    val dataMap: CoarseGrainDataMap = new CGDataMap()
    dataMap.init(new DataMapModel(mapDistributable.getFilePath))
    Seq(dataMap).asJava
  }

  /**
   *
   * @param event
   */
  override def fireEvent(event: Event): Unit = {
    ???
  }

  /**
   * Get all distributable objects of a segmentid
   *
   * @return
   */
  override def toDistributable(segment: Segment): java.util.List[DataMapDistributable] = {
    val file = FileFactory.getCarbonFile(
      CarbonTablePath.getSegmentPath(identifier.getTablePath, segment.getSegmentNo))

    val files = file.listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = file.getName.endsWith(".datamap")
    })
    files.map { f =>
      val d:DataMapDistributable = new BlockletDataMapDistributable(f.getCanonicalPath)
      d
    }.toList.asJava
  }


  /**
   * Clears datamap of the segment
   */
  override def clear(segment: Segment): Unit = {

  }

  /**
   * Clear all datamaps from memory
   */
  override def clear(): Unit = {

  }

  /**
   * Return metadata of this datamap
   */
  override def getMeta: DataMapMeta = {
    new DataMapMeta(dataMapSchema.getProperties.get("indexcolumns").split(",").toList.asJava,
      List(ExpressionType.EQUALS, ExpressionType.IN).asJava)
  }
}

class CGDataMap extends CoarseGrainDataMap {

  var maxMin: ArrayBuffer[(String, Int, (Array[Byte], Array[Byte]))] = _
  var FileReader: FileReader = _
  var filePath: String = _
  val compressor = new SnappyCompressor

  /**
   * It is called to load the data map to memory or to initialize it.
   */
  override def init(dataMapModel: DataMapModel): Unit = {
    this.filePath = dataMapModel.getFilePath
    val size = FileFactory.getCarbonFile(filePath).getSize
    FileReader = FileFactory.getFileHolder(FileFactory.getFileType(filePath))
    val footerLen = FileReader.readInt(filePath, size-4)
    val bytes = FileReader.readByteArray(filePath, size-footerLen-4, footerLen)
    val in = new ByteArrayInputStream(compressor.unCompressByte(bytes))
    val obj = new ObjectInputStream(in)
    maxMin = obj.readObject().asInstanceOf[ArrayBuffer[(String, Int, (Array[Byte], Array[Byte]))]]
  }

  /**
   * Prune the datamap with filter expression. It returns the list of
   * blocklets where these filters can exist.
   *
   * @param filterExp
   * @return
   */
  override def prune(
      filterExp: FilterResolverIntf,
      segmentProperties: SegmentProperties,
      partitions: java.util.List[PartitionSpec]): java.util.List[Blocklet] = {
    val buffer: ArrayBuffer[Expression] = new ArrayBuffer[Expression]()
    val expression = filterExp.getFilterExpression
    getEqualToExpression(expression, buffer)
    val value = buffer.map { f =>
      f.getChildren.get(1).evaluate(null).getString
    }
    val meta = findMeta(value(0).getBytes)
    meta.map { f=>
      new Blocklet(f._1, f._2 + "")
    }.asJava
  }


  private def findMeta(value: Array[Byte]) = {
    val tuples = maxMin.filter { f =>
      ByteUtil.UnsafeComparer.INSTANCE.compareTo(value, f._3._1) <= 0 &&
      ByteUtil.UnsafeComparer.INSTANCE.compareTo(value, f._3._2) >= 0
    }
    tuples
  }

  private def getEqualToExpression(expression: Expression, buffer: ArrayBuffer[Expression]): Unit = {
    if (expression.isInstanceOf[EqualToExpression]) {
      buffer += expression
    } else {
      if (expression.getChildren != null) {
        expression.getChildren.asScala.map { f =>
          if (f.isInstanceOf[EqualToExpression]) {
            buffer += f
          }
          getEqualToExpression(f, buffer)
        }
      }
    }
  }

  /**
   * Clear complete index table and release memory.
   */
  override def clear() = {
    ???
  }

  override def isScanRequired(filterExp: FilterResolverIntf): Boolean = ???
}

class CGDataMapWriter(identifier: AbsoluteTableIdentifier,
    segment: Segment,
    dataWritePath: String,
    dataMapSchema: DataMapSchema)
  extends DataMapWriter(identifier, segment, dataWritePath) {

  var currentBlockId: String = null
  val cgwritepath = dataWritePath + "/" +
                    dataMapSchema.getDataMapName + System.nanoTime() + ".datamap"
  lazy val stream: DataOutputStream = FileFactory
    .getDataOutputStream(cgwritepath, FileFactory.getFileType(cgwritepath))
  val blockletList = new ArrayBuffer[Array[Byte]]()
  val maxMin = new ArrayBuffer[(String, Int, (Array[Byte], Array[Byte]))]()
  val compressor = new SnappyCompressor

  /**
   * Start of new block notification.
   *
   * @param blockId file name of the carbondata file
   */
  override def onBlockStart(blockId: String, taskId: Long): Unit = {
    currentBlockId = blockId
  }

  /**
   * End of block notification
   */
  override def onBlockEnd(blockId: String): Unit = {

  }

  /**
   * Start of new blocklet notification.
   *
   * @param blockletId sequence number of blocklet in the block
   */
  override def onBlockletStart(blockletId: Int): Unit = {

  }

  /**
   * End of blocklet notification
   *
   * @param blockletId sequence number of blocklet in the block
   */
  override def onBlockletEnd(blockletId: Int): Unit = {
    val sorted = blockletList
      .sortWith((l, r) => ByteUtil.UnsafeComparer.INSTANCE.compareTo(l, r) <= 0)
    maxMin +=
    ((currentBlockId+"", blockletId, (sorted.last, sorted.head)))
    blockletList.clear()
  }

  /**
   * Add the column pages row to the datamap, order of pages is same as `indexColumns` in
   * DataMapMeta returned in DataMapFactory.
   *
   * Implementation should copy the content of `pages` as needed, because `pages` memory
   * may be freed after this method returns, if using unsafe column page.
   */
  override def onPageAdded(blockletId: Int,
      pageId: Int,
      pages: Array[ColumnPage]): Unit = {
    val size = pages(0).getPageSize
    val list = new ArrayBuffer[Array[Byte]]()
    var i = 0
    while (i < size) {
      val bytes = pages(0).getBytes(i)
      val newBytes = new Array[Byte](bytes.length - 2)
      System.arraycopy(bytes, 2, newBytes, 0, newBytes.length)
      list += newBytes
      i = i + 1
    }
    // Sort based on the column data in order to create index.
    val sorted = list
      .sortWith((l, r) => ByteUtil.UnsafeComparer.INSTANCE.compareTo(l, r) <= 0)
    blockletList += sorted.head
    blockletList += sorted.last
  }


  /**
   * This is called during closing of writer.So after this call no more data will be sent to this
   * class.
   */
  override def finish(): Unit = {
    val out = new ByteOutputStream()
    val outStream = new ObjectOutputStream(out)
    outStream.writeObject(maxMin)
    outStream.close()
    val bytes = compressor.compressByte(out.getBytes)
    stream.write(bytes)
    stream.writeInt(bytes.length)
    stream.close()
    commitFile(cgwritepath)
  }


}

class CGDataMapTestCase extends QueryTest with BeforeAndAfterAll {

  val file2 = resourcesPath + "/compaction/fil2.csv"
  override protected def beforeAll(): Unit = {
    //n should be about 5000000 of reset if size is default 1024
    val n = 150000
    CompactionSupportGlobalSortBigFileTest.createFile(file2, n * 4, n)
    sql("DROP TABLE IF EXISTS normal_test")
    sql(
      """
        | CREATE TABLE normal_test(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE normal_test OPTIONS('header'='false')")
  }

  test("test cg datamap") {
    sql("DROP TABLE IF EXISTS datamap_test_cg")
    sql(
      """
        | CREATE TABLE datamap_test_cg(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    val table = CarbonMetadata.getInstance().getCarbonTable("default_datamap_test_cg")
    // register datamap writer
    sql(s"create datamap cgdatamap on table datamap_test_cg " +
        s"using '${classOf[CGDataMapFactory].getName}' " +
        s"DMPROPERTIES('indexcolumns'='name')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test_cg OPTIONS('header'='false')")
    checkAnswer(sql("select * from datamap_test_cg where name='n502670'"),
      sql("select * from normal_test where name='n502670'"))
  }

  test("test cg datamap with 2 datamaps ") {
    sql("DROP TABLE IF EXISTS datamap_test")
    sql(
      """
        | CREATE TABLE datamap_test(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    val table = CarbonMetadata.getInstance().getCarbonTable("default_datamap_test")
    // register datamap writer
    sql(s"create datamap ggdatamap1 on table datamap_test using '${classOf[CGDataMapFactory].getName}' DMPROPERTIES('indexcolumns'='name')")
    sql(s"create datamap ggdatamap2 on table datamap_test using '${classOf[CGDataMapFactory].getName}' DMPROPERTIES('indexcolumns'='city')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test OPTIONS('header'='false')")
    checkAnswer(sql("select * from datamap_test where name='n502670' and city='c2670'"),
      sql("select * from normal_test where name='n502670' and city='c2670'"))
  }

  test("test datamap storage in system folder") {
    sql("DROP TABLE IF EXISTS datamap_store_test")
    sql(
      """
        | CREATE TABLE datamap_store_test(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)

    sql(s"create datamap test_cg_datamap on table datamap_store_test using '${classOf[CGDataMapFactory].getName}' as select  id, name from datamap_store_test")

    val loc = CarbonProperties.getInstance().getSystemFolderLocation + "/datamap_store_test_test_cg_datamap.dmschema"

    assert(FileFactory.isFileExist(loc))
  }

  test("test datamap storage and drop in system folder") {
    sql("DROP TABLE IF EXISTS datamap_store_test1")
    sql(
      """
        | CREATE TABLE datamap_store_test1(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)

    sql(s"create datamap test_cg_datamap1 on table datamap_store_test1 using '${classOf[CGDataMapFactory].getName}' as select  id, name from datamap_store_test")

    val loc = CarbonProperties.getInstance().getSystemFolderLocation + "/datamap_store_test1_test_cg_datamap1.dmschema"

    assert(FileFactory.isFileExist(loc))

    sql(s"drop datamap test_cg_datamap1 on table datamap_store_test1")

    assert(!FileFactory.isFileExist(loc))
  }

  test("test show datamap storage") {
    sql("DROP TABLE IF EXISTS datamap_store_test2")
    sql(
      """
        | CREATE TABLE datamap_store_test2(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)

    sql(s"create datamap test_cg_datamap2 on table datamap_store_test2 using '${classOf[CGDataMapFactory].getName}' as select  id, name from datamap_store_test")

    val loc = CarbonProperties.getInstance().getSystemFolderLocation + "/datamap_store_test2_test_cg_datamap2.dmschema"

    assert(FileFactory.isFileExist(loc))

    checkExistence(sql("show datamap"), true, "test_cg_datamap2")

    sql(s"drop datamap test_cg_datamap2 on table datamap_store_test2")

    assert(!FileFactory.isFileExist(loc))
  }

  override protected def afterAll(): Unit = {
    CompactionSupportGlobalSortBigFileTest.deleteFile(file2)
    sql("DROP TABLE IF EXISTS normal_test")
    sql("DROP TABLE IF EXISTS datamap_test")
    sql("DROP TABLE IF EXISTS datamap_test_cg")
    sql("DROP TABLE IF EXISTS datamap_store_test")
    sql("DROP TABLE IF EXISTS datamap_store_test1")
    sql("DROP TABLE IF EXISTS datamap_store_test2")
  }
}
