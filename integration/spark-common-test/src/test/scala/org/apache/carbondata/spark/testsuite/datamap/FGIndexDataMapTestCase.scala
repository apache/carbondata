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

import org.apache.carbondata.core.datamap.dev.fgdatamap.{AbstractFineGrainIndexDataMap, AbstractFineGrainIndexDataMapFactory}
import org.apache.carbondata.core.datamap.dev.{AbstractDataMapWriter, DataMapModel}
import org.apache.carbondata.core.datamap.{DataMapDistributable, DataMapMeta}
import org.apache.carbondata.core.datastore.FileReader
import org.apache.carbondata.core.datastore.block.SegmentProperties
import org.apache.carbondata.core.datastore.compression.SnappyCompressor
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.datastore.page.ColumnPage
import org.apache.carbondata.core.indexstore.FineGrainBlocklet
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapDistributable
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata}
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression
import org.apache.carbondata.core.scan.filter.intf.ExpressionType
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf
import org.apache.carbondata.core.util.ByteUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.Event
import org.apache.carbondata.spark.testsuite.datacompaction.CompactionSupportGlobalSortBigFileTest

class FGIndexDataMapFactory extends AbstractFineGrainIndexDataMapFactory {
  var identifier: AbsoluteTableIdentifier = _
  var dataMapSchema: DataMapSchema = _

  /**
   * Initialization of Datamap factory with the identifier and datamap name
   */
  override def init(identifier: AbsoluteTableIdentifier, dataMapSchema: DataMapSchema): Unit = {
    this.identifier = identifier
    this.dataMapSchema = dataMapSchema
  }

  /**
   * Return a new write for this datamap
   */
  override def createWriter(segmentId: String, dataWritePath: String): AbstractDataMapWriter = {
    new FGDataMapWriter(identifier, segmentId, dataWritePath, dataMapSchema)
  }

  /**
   * Get the datamap for segmentid
   */
  override def getDataMaps(segmentId: String): java.util.List[AbstractFineGrainIndexDataMap] = {
    val file = FileFactory
      .getCarbonFile(CarbonTablePath.getSegmentPath(identifier.getTablePath, segmentId))

    val files = file.listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = file.getName.endsWith(".datamap")
    })
    files.map { f =>
      val dataMap: AbstractFineGrainIndexDataMap = new FGIndexDataMap()
      dataMap.init(new DataMapModel(f.getCanonicalPath))
      dataMap
    }.toList.asJava
  }

  /**
   * Get datamap for distributable object.
   */
  override def getDataMaps(
      distributable: DataMapDistributable): java.util.List[AbstractFineGrainIndexDataMap]= {
    val mapDistributable = distributable.asInstanceOf[BlockletDataMapDistributable]
    val dataMap: AbstractFineGrainIndexDataMap = new FGIndexDataMap()
    dataMap.init(new DataMapModel(mapDistributable.getFilePath))
    Seq(dataMap).asJava
  }

  /**
   * Get all distributable objects of a segmentid
   *
   * @return
   */
  override def toDistributable(segmentId: String): java.util.List[DataMapDistributable] = {
    val file = FileFactory
      .getCarbonFile(CarbonTablePath.getSegmentPath(identifier.getTablePath, segmentId))

    val files = file.listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = file.getName.endsWith(".datamap")
    })
    files.map { f =>
      val d: DataMapDistributable = new BlockletDataMapDistributable(f.getCanonicalPath)
      d
    }.toList.asJava
  }


  /**
   *
   * @param event
   */
  override def fireEvent(event: Event):Unit = {
    ???
  }

  /**
   * Clears datamap of the segment
   */
  override def clear(segmentId: String): Unit = {
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

class FGIndexDataMap extends AbstractFineGrainIndexDataMap {

  var maxMin: ArrayBuffer[(String, Int, (Array[Byte], Array[Byte]), Long, Int)] = _
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
    val footerLen = FileReader.readInt(filePath, size - 4)
    val bytes = FileReader.readByteArray(filePath, size - footerLen - 4, footerLen)
    val in = new ByteArrayInputStream(compressor.unCompressByte(bytes))
    val obj = new ObjectInputStream(in)
    maxMin = obj.readObject()
      .asInstanceOf[ArrayBuffer[(String, Int, (Array[Byte], Array[Byte]), Long, Int)]]
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
      partitions: java.util.List[String]): java.util.List[FineGrainBlocklet] = {
    val buffer: ArrayBuffer[Expression] = new ArrayBuffer[Expression]()
    val expression = filterExp.getFilterExpression
    getEqualToExpression(expression, buffer)
    val value = buffer.map { f =>
      f.getChildren.get(1).evaluate(null).getString
    }
    val meta = findMeta(value(0).getBytes)
    meta.map { f =>
      readAndFindData(f, value(0).getBytes())
    }.filter(_.isDefined).map(_.get).asJava
  }

  private def readAndFindData(meta: (String, Int, (Array[Byte], Array[Byte]), Long, Int),
      value: Array[Byte]): Option[FineGrainBlocklet] = {
    val bytes = FileReader.readByteArray(filePath, meta._4, meta._5)
    val outputStream = new ByteArrayInputStream(compressor.unCompressByte(bytes))
    val obj = new ObjectInputStream(outputStream)
    val blockletsData = obj.readObject()
      .asInstanceOf[ArrayBuffer[(Array[Byte], Seq[Seq[Int]], Seq[Int])]]

    import scala.collection.Searching._
    val searching = blockletsData
      .search[(Array[Byte], Seq[Seq[Int]], Seq[Int])]((value, Seq(Seq(0)), Seq(0)))(new Ordering[
      (Array[Byte], Seq[Seq[Int]], Seq[Int])] {
      override def compare(x: (Array[Byte], Seq[Seq[Int]], Seq[Int]),
          y: (Array[Byte], Seq[Seq[Int]], Seq[Int])) = {
        ByteUtil.UnsafeComparer.INSTANCE.compareTo(x._1, y._1)
      }
    })
    if (searching.insertionPoint >= 0) {
      val f = blockletsData(searching.insertionPoint)
      val pages = f._3.zipWithIndex.map { p =>
        val pg = new FineGrainBlocklet.Page
        pg.setPageId(p._1)
        pg.setRowId(f._2(p._2).toArray)
        pg
      }
      pages
      Some(new FineGrainBlocklet(meta._1, meta._2.toString, pages.toList.asJava))
    } else {
      None
    }

  }

  private def findMeta(value: Array[Byte]) = {
    val tuples = maxMin.filter { f =>
      ByteUtil.UnsafeComparer.INSTANCE.compareTo(value, f._3._1) >= 0 &&
      ByteUtil.UnsafeComparer.INSTANCE.compareTo(value, f._3._2) <= 0
    }
    tuples
  }

  def getEqualToExpression(expression: Expression, buffer: ArrayBuffer[Expression]): Unit = {
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
  override def clear():Unit = {
    ???
  }

  override def isScanRequired(filterExp: FilterResolverIntf): Boolean = ???
}

class FGDataMapWriter(identifier: AbsoluteTableIdentifier,
    segmentId: String, dataWriterPath: String, dataMapSchema: DataMapSchema)
  extends AbstractDataMapWriter(identifier, segmentId, dataWriterPath) {

  var currentBlockId: String = null
  val fgwritepath = dataWriterPath + "/" + dataMapSchema.getDataMapName + System.nanoTime() +
                    ".datamap"
  val stream: DataOutputStream = FileFactory
    .getDataOutputStream(fgwritepath, FileFactory.getFileType(fgwritepath))
  val blockletList = new ArrayBuffer[(Array[Byte], Seq[Int], Seq[Int])]()
  val maxMin = new ArrayBuffer[(String, Int, (Array[Byte], Array[Byte]), Long, Int)]()
  var position: Long = 0
  val compressor = new SnappyCompressor

  /**
   * Start of new block notification.
   *
   * @param blockId file name of the carbondata file
   */
  override def onBlockStart(blockId: String): Unit = {
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
      .sortWith((l, r) => ByteUtil.UnsafeComparer.INSTANCE.compareTo(l._1, r._1) <= 0)
    var oldValue: (Array[Byte], Seq[Seq[Int]], Seq[Int]) = null
    var addedLast: Boolean = false
    val blockletListUpdated = new ArrayBuffer[(Array[Byte], Seq[Seq[Int]], Seq[Int])]()
    // Merge all same column values to single row.
    sorted.foreach { f =>
      if (oldValue != null) {
        if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(f._1, oldValue._1) == 0) {
          oldValue = (oldValue._1, oldValue._2 ++ Seq(f._2), oldValue._3 ++ f._3)
          addedLast = false
        } else {
          blockletListUpdated += oldValue
          oldValue = (f._1, Seq(f._2), f._3)
          addedLast = true
        }
      } else {
        oldValue = (f._1, Seq(f._2), f._3)
        addedLast = false
      }
    }
    if (!addedLast && oldValue != null) {
      blockletListUpdated += oldValue
    }

    val out = new ByteOutputStream()
    val outStream = new ObjectOutputStream(out)
    outStream.writeObject(blockletListUpdated)
    outStream.close()
    val bytes = compressor.compressByte(out.getBytes)
    stream.write(bytes)
    maxMin +=
    ((currentBlockId + "", blockletId, (blockletListUpdated.head._1, blockletListUpdated.last
      ._1), position, bytes.length))
    position += bytes.length
    blockletList.clear()
  }

  /**
   * Add the column pages row to the datamap, order of pages is same as `indexColumns` in
   * DataMapMeta returned in IndexDataMapFactory.
   *
   * Implementation should copy the content of `pages` as needed, because `pages` memory
   * may be freed after this method returns, if using unsafe column page.
   */
  override def onPageAdded(blockletId: Int,
      pageId: Int,
      pages: Array[ColumnPage]): Unit = {
    val size = pages(0).getPageSize
    val list = new ArrayBuffer[(Array[Byte], Int)]()
    var i = 0
    while (i < size) {
      val bytes = pages(0).getBytes(i)
      val newBytes = new Array[Byte](bytes.length - 2)
      System.arraycopy(bytes, 2, newBytes, 0, newBytes.length)
      list += ((newBytes, i))
      i = i + 1
    }
    // Sort based on the column data in order to create index.
    val sorted = list
      .sortWith((l, r) => ByteUtil.UnsafeComparer.INSTANCE.compareTo(l._1, r._1) <= 0)
    var oldValue: (Array[Byte], Seq[Int], Seq[Int]) = null
    var addedLast: Boolean = false
    // Merge all same column values to single row.
    sorted.foreach { f =>
      if (oldValue != null) {
        if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(f._1, oldValue._1) == 0) {
          oldValue = (oldValue._1, oldValue._2 ++ Seq(f._2), oldValue._3)
          addedLast = false
        } else {
          blockletList += oldValue
          oldValue = (f._1, Seq(f._2), Seq(pageId))
          addedLast = true
        }
      } else {
        oldValue = (f._1, Seq(f._2), Seq(pageId))
        addedLast = false
      }
    }
    if (!addedLast && oldValue != null) {
      blockletList += oldValue
    }
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
    commitFile(fgwritepath)
  }
}

class FGIndexDataMapTestCase extends QueryTest with BeforeAndAfterAll {

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

  test("test fg datamap") {
    sql("DROP TABLE IF EXISTS datamap_test")
    sql(
      """
        | CREATE TABLE datamap_test(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    val table = CarbonMetadata.getInstance().getCarbonTable("default_datamap_test")
    // register datamap writer
    sql(
      s"""
         | CREATE DATAMAP ggdatamap ON TABLE datamap_test
         | USING '${classOf[FGIndexDataMapFactory].getName}'
         | DMPROPERTIES('indexcolumns'='name')
       """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test OPTIONS('header'='false')")
    checkAnswer(sql("select * from datamap_test where name='n502670'"),
      sql("select * from normal_test where name='n502670'"))
  }

  test("test fg datamap with 2 datamaps ") {
    sql("DROP TABLE IF EXISTS datamap_test")
    sql(
      """
        | CREATE TABLE datamap_test(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    val table = CarbonMetadata.getInstance().getCarbonTable("default_datamap_test")
    // register datamap writer
    sql(
      s"""
         | CREATE DATAMAP ggdatamap1 ON TABLE datamap_test
         | USING '${classOf[FGIndexDataMapFactory].getName}'
         | DMPROPERTIES('indexcolumns'='name')
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP ggdatamap2 ON TABLE datamap_test
         | USING '${classOf[FGIndexDataMapFactory].getName}'
         | DMPROPERTIES('indexcolumns'='city')
       """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test OPTIONS('header'='false')")
    checkAnswer(sql("select * from datamap_test where name='n502670' and city='c2670'"),
      sql("select * from normal_test where name='n502670' and city='c2670'"))
  }

  override protected def afterAll(): Unit = {
    CompactionSupportGlobalSortBigFileTest.deleteFile(file2)
    sql("DROP TABLE IF EXISTS normal_test")
    sql("DROP TABLE IF EXISTS datamap_test")
  }
}
