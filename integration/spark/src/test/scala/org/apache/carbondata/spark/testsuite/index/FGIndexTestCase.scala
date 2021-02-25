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

import java.io.{ByteArrayInputStream, DataOutputStream, ObjectInputStream, ObjectOutputStream}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.FileReader
import org.apache.carbondata.core.datastore.block.SegmentProperties
import org.apache.carbondata.core.datastore.compression.SnappyCompressor
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.datastore.page.ColumnPage
import org.apache.carbondata.core.features.TableOperation
import org.apache.carbondata.core.index.{IndexInputSplit, IndexMeta, Segment}
import org.apache.carbondata.core.index.dev.{IndexBuilder, IndexModel, IndexWriter}
import org.apache.carbondata.core.index.dev.fgindex.{FineGrainBlocklet, FineGrainIndex, FineGrainIndexFactory}
import org.apache.carbondata.core.indexstore.blockletindex.BlockletIndexInputSplit
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, IndexSchema}
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression
import org.apache.carbondata.core.scan.filter.executer.FilterExecutor
import org.apache.carbondata.core.scan.filter.intf.ExpressionType
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf
import org.apache.carbondata.core.util.{ByteUtil, CarbonProperties}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.Event
import org.apache.carbondata.spark.testsuite.datacompaction.CompactionSupportGlobalSortBigFileTest

class FGIndexFactory(carbonTable: CarbonTable,
    indexSchema: IndexSchema) extends FineGrainIndexFactory(carbonTable, indexSchema) {
  // scalastyle:off ???
  /**
   * Return a new write for this indexSchema
   */
  override def createWriter(segment: Segment,
      dataWritePath: String,
      segmentProperties: SegmentProperties): IndexWriter = {
    new FGIndexWriter(carbonTable, segment, dataWritePath, indexSchema)
  }

  /**
   * Get the indexSchema for segmentId
   */
  override def getIndexes(segment: Segment): java.util.List[FineGrainIndex] = {
    val path = CarbonTablePath.getSegmentPath(carbonTable.getTablePath, segment.getSegmentNo)
    val file = FileFactory.getCarbonFile(path + "/" + indexSchema.getIndexName)

    val files = file.listFiles()
    files.map { f =>
      val index: FineGrainIndex = new FGIndex()
      index.init(new IndexModel(f.getCanonicalPath, new Configuration(false)))
      index
    }.toList.asJava
  }

  /**
   * Get indexSchema for distributable object.
   */
  override def getIndexes(distributable: IndexInputSplit): java.util.List[FineGrainIndex] = {
    val mapDistributable = distributable.asInstanceOf[BlockletIndexInputSplit]
    val index: FineGrainIndex = new FGIndex()
    index.init(new IndexModel(mapDistributable.getFilePath, new Configuration(false)))
    Seq(index).asJava
  }

  /**
   * Get all distributable objects of a segmentId
   *
   * @return
   */
  override def toDistributable(segment: Segment): java.util.List[IndexInputSplit] = {
    val path = carbonTable.getTablePath
    val file = FileFactory.getCarbonFile(
      path + "/" + indexSchema.getIndexName + "/" + segment.getSegmentNo)

    val files = file.listFiles()
    files.map { f =>
      val d: IndexInputSplit = new BlockletIndexInputSplit(f.getCanonicalPath)
      d.setSegment(segment)
      d.setIndexSchema(getIndexSchema)
      d
    }.toList.asJava
  }

  /**
   * @param event
   */
  override def fireEvent(event: Event): Unit = {
    ???
  }

  /**
   * Clear all indexs from memory
   */
  override def clear(): Unit = {
  }

  /**
   * Return metadata of this indexSchema
   */
  override def getMeta: IndexMeta = {
    new IndexMeta(carbonTable.getIndexedColumns(indexSchema.getIndexColumns),
      List(ExpressionType.EQUALS, ExpressionType.IN).asJava)
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
    ???
  }

  /**
   * Get the indexSchema for segmentId
   */
  override def getIndexes(segment: Segment,
      partitionLocations: java.util.Set[Path]): java.util.List[FineGrainIndex] = {
    getIndexes(segment)
  }
}

class FGIndex extends FineGrainIndex {

  var maxMin: ArrayBuffer[(Int, (Array[Byte], Array[Byte]), Long, Int)] = _
  var FileReader: FileReader = _
  var filePath: String = _
  val compressor = new SnappyCompressor
  var taskName: String = _

  /**
   * It is called to load the index to memory or to initialize it.
   */
  override def init(indexModel: IndexModel): Unit = {
    this.filePath = indexModel.getFilePath
    val carbonFile = FileFactory.getCarbonFile(filePath)
    taskName = carbonFile.getName
    val size = carbonFile.getSize
    FileReader = FileFactory.getFileHolder(FileFactory.getFileType(filePath))
    val footerLen = FileReader.readInt(filePath, size - 4)
    val bytes = FileReader.readByteArray(filePath, size - footerLen - 4, footerLen)
    val in = new ByteArrayInputStream(compressor.unCompressByte(bytes))
    val obj = new ObjectInputStream(in)
    maxMin = obj.readObject()
      .asInstanceOf[ArrayBuffer[(Int, (Array[Byte], Array[Byte]), Long, Int)]]
  }

  /**
   * Prune the indexSchema with filter expression. It returns the list of
   * blocklets where these filters can exist.
   *
   * @param filterExp
   * @return
   */
  override def prune(
      filterExp: FilterResolverIntf,
      segmentProperties: SegmentProperties,
      filterExecuter: FilterExecutor,
      carbonTable: CarbonTable): java.util.List[FineGrainBlocklet] = {
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

  private def readAndFindData(meta: (Int, (Array[Byte], Array[Byte]), Long, Int),
      value: Array[Byte]): Option[FineGrainBlocklet] = {
    val bytes = FileReader.readByteArray(filePath, meta._3, meta._4)
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
      Some(new FineGrainBlocklet(taskName, meta._1.toString, pages.toList.asJava))
    } else {
      None
    }
  }

  private def findMeta(value: Array[Byte]) = {
    val tuples = maxMin.filter { f =>
      ByteUtil.UnsafeComparer.INSTANCE.compareTo(value, f._2._1) >= 0 &&
      ByteUtil.UnsafeComparer.INSTANCE.compareTo(value, f._2._2) <= 0
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
  override def clear(): Unit = {

  }

  override def isScanRequired(filterExp: FilterResolverIntf): Boolean = ???

  /**
   * clears all the resources for indexs
   */
  override def finish(): Unit = {

  }

  override def getNumberOfEntries: Int = 1
}

class FGIndexWriter(carbonTable: CarbonTable,
    segment: Segment, shardName: String, indexSchema: IndexSchema)
  extends IndexWriter(carbonTable.getTablePath, indexSchema.getIndexName,
    carbonTable.getIndexedColumns(indexSchema.getIndexColumns), segment, shardName) {

  var taskName: String = _
  val fgwritepath = indexPath
  var stream: DataOutputStream = _
  val blockletList = new ArrayBuffer[(Array[Byte], Seq[Int], Seq[Int])]()
  val maxMin = new ArrayBuffer[(Int, (Array[Byte], Array[Byte]), Long, Int)]()
  var position: Long = 0
  val compressor = new SnappyCompressor

  /**
   * Start of new block notification.
   *
   * @param blockId file name of the carbondata file
   */
  override def onBlockStart(blockId: String): Unit = {
    this.taskName = shardName
    if (stream == null) {
      val path = fgwritepath.substring(0, fgwritepath.lastIndexOf("/"))
      FileFactory.mkdirs(path)
      stream = FileFactory
        .getDataOutputStream(fgwritepath)
    }
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
    stream.write(bytes.array(), 0, bytes.limit())
    maxMin +=
    ((blockletId, (blockletListUpdated.head._1, blockletListUpdated.last
      ._1), position, bytes.limit()))
    position += bytes.limit()
    blockletList.clear()
  }

  /**
   * Add the column pages row to the indexSchema, order of pages is same as `indexColumns` in
   * IndexMeta returned in IndexFactory.
   *
   * Implementation should copy the content of `pages` as needed, because `pages` memory
   * may be freed after this method returns, if using unsafe column page.
   */
  override def onPageAdded(blockletId: Int,
      pageId: Int,
      pageSize: Int,
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
    FileFactory.mkdirs(fgwritepath)
    val out = new ByteOutputStream()
    val outStream = new ObjectOutputStream(out)
    outStream.writeObject(maxMin)
    outStream.close()
    val bytes = compressor.compressByte(out.getBytes)
    stream.write(bytes.array(), 0, bytes.limit())
    stream.writeInt(bytes.limit())
    stream.close()
  }
}

class FGIndexTestCase extends QueryTest with BeforeAndAfterAll {

  val file2 = resourcesPath + "/compaction/fil2.csv"

  override protected def beforeAll(): Unit = {
    // n should be about 5000000 of reset if size is default 1024
    val n = 150000
    CompactionSupportGlobalSortBigFileTest.createFile(file2, n * 4, n)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "true")
    sql("DROP TABLE IF EXISTS normal_test")
    sql(
      """
        | CREATE TABLE normal_test(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE normal_test OPTIONS('header'='false')")
  }

  test("test fg indexSchema") {
    sql("DROP TABLE IF EXISTS index_test")
    sql(
      """
        | CREATE TABLE index_test(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    val table = CarbonMetadata.getInstance().getCarbonTable("default_index_test")
    // register indexSchema writer
    sql(
      s"""
         | CREATE INDEX ggindex
         | ON TABLE index_test (name)
         | AS '${classOf[FGIndexFactory].getName}'
       """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE index_test OPTIONS('header'='false')")
    checkAnswer(sql("select * from index_test where name='n502670'"),
      sql("select * from normal_test where name='n502670'"))
  }

  test("test fg indexSchema with 2 indexs ") {
    sql("DROP TABLE IF EXISTS index_test")
    sql(
      """
        | CREATE TABLE index_test(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    val table = CarbonMetadata.getInstance().getCarbonTable("default_index_test")
    // register indexSchema writer
    sql(
      s"""
         | CREATE INDEX ggindex1
         | ON TABLE index_test (name)
         | AS '${classOf[FGIndexFactory].getName}'
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX ggindex2
         | ON TABLE index_test (city)
         | AS '${classOf[FGIndexFactory].getName}'
       """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE index_test OPTIONS('header'='false')")
    checkAnswer(sql("select * from index_test where name='n502670' and city='c2670'"),
      sql("select * from normal_test where name='n502670' and city='c2670'"))
    checkAnswer(sql("select * from index_test where name='n502670' or city='c2670'"),
      sql("select * from normal_test where name='n502670' or city='c2670'"))
  }

  // Exclude when running with index server, as pruning info for explain command
  // not set with index server.
  test("test invisible indexSchema during query", true) {
    val tableName = "index_test"
    val indexName1 = "index1"
    val indexName2 = "index2"
    sql(s"DROP TABLE IF EXISTS $tableName")
    sql(
      s"""
         | CREATE TABLE $tableName(id INT, name STRING, city STRING, age INT)
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    // register indexSchema writer
    sql(
      s"""
         | CREATE INDEX $indexName1
         | ON TABLE $tableName (name)
         | AS '${classOf[FGIndexFactory].getName}'
      """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $indexName2
         | ON TABLE $tableName (city)
         | AS '${classOf[FGIndexFactory].getName}'
       """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE $tableName OPTIONS('header'='false')")
    val df1 = sql(s"EXPLAIN EXTENDED SELECT * FROM $tableName " +
                  s"WHERE name='n502670' AND city='c2670'").collect()
    assert(df1(0).getString(0).contains("FG Index"))
    assert(df1(0).getString(0).contains(indexName1))
    assert(df1(0).getString(0).contains(indexName2))

    // make index1 invisible
    sql(s"SET ${CarbonCommonConstants.CARBON_INDEX_VISIBLE}default.$tableName.$indexName1 = false")
    val df2 = sql(s"EXPLAIN EXTENDED SELECT * FROM $tableName " +
                  s"WHERE name='n502670' AND city='c2670'").collect()
    val e = intercept[Exception] {
      assert(df2(0).getString(0).contains(indexName1))
    }
    assert(e.getMessage.contains("did not contain \"" + indexName1))
    assert(df2(0).getString(0).contains(indexName2))
    checkAnswer(sql(s"SELECT * FROM $tableName WHERE name='n502670' AND city='c2670'"),
      sql("SELECT * FROM normal_test WHERE name='n502670' AND city='c2670'"))

    // also make index2 invisible
    sql(s"SET ${CarbonCommonConstants.CARBON_INDEX_VISIBLE}default.$tableName.$indexName2 = false")
    checkAnswer(sql(s"SELECT * FROM $tableName WHERE name='n502670' AND city='c2670'"),
      sql("SELECT * FROM normal_test WHERE name='n502670' AND city='c2670'"))
    val df3 = sql(s"EXPLAIN EXTENDED SELECT * FROM $tableName " +
                  s"WHERE name='n502670' AND city='c2670'").collect()
    val e31 = intercept[Exception] {
      assert(df3(0).getString(0).contains(indexName1))
    }
    assert(e31.getMessage.contains("did not contain \"" + indexName1))
    val e32 = intercept[Exception] {
      assert(df3(0).getString(0).contains(indexName2))
    }
    assert(e32.getMessage.contains("did not contain \"" + indexName2))

    // make index1,index2 visible
    sql(s"SET ${CarbonCommonConstants.CARBON_INDEX_VISIBLE}default.$tableName.$indexName1 = true")
    sql(s"SET ${CarbonCommonConstants.CARBON_INDEX_VISIBLE}default.$tableName.$indexName2 = true")
    checkAnswer(sql(s"SELECT * FROM $tableName WHERE name='n502670' AND city='c2670'"),
      sql("SELECT * FROM normal_test WHERE name='n502670' AND city='c2670'"))
    val df4 = sql(s"EXPLAIN EXTENDED SELECT * FROM $tableName " +
                  s"WHERE name='n502670' AND city='c2670'").collect()
    assert(df4(0).getString(0).contains(indexName1))
    assert(df4(0).getString(0).contains(indexName2))
  }

  override protected def afterAll(): Unit = {
    defaultConfig()
//    CompactionSupportGlobalSortBigFileTest.deleteFile(file2)
//    sql("DROP TABLE IF EXISTS normal_test")
//    sql("DROP TABLE IF EXISTS index_test")
//    sql("DROP TABLE IF EXISTS index_testFG")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS,
        CarbonCommonConstants.ENABLE_QUERY_STATISTICS_DEFAULT)
  }
  // scalastyle:on ???
}
