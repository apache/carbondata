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
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.test.util.CarbonQueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.{DataMapDistributable, DataMapMeta, Segment}
import org.apache.carbondata.core.datamap.dev.{DataMapBuilder, DataMapModel, DataMapWriter}
import org.apache.carbondata.core.datamap.dev.cgdatamap.{CoarseGrainDataMap, CoarseGrainDataMapFactory}
import org.apache.carbondata.core.datastore.FileReader
import org.apache.carbondata.core.datastore.block.SegmentProperties
import org.apache.carbondata.core.datastore.compression.SnappyCompressor
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.datastore.page.ColumnPage
import org.apache.carbondata.core.features.TableOperation
import org.apache.carbondata.core.indexstore.{Blocklet, PartitionSpec}
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapDistributable
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema, DiskBasedDMSchemaStorageProvider}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata}
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression
import org.apache.carbondata.core.scan.filter.intf.ExpressionType
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf
import org.apache.carbondata.core.util.{ByteUtil, CarbonProperties}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.Event
import org.apache.carbondata.spark.testsuite.datacompaction.CompactionSupportGlobalSortBigFileTest

class CGDataMapFactory(
    carbonTable: CarbonTable,
    dataMapSchema: DataMapSchema) extends CoarseGrainDataMapFactory(carbonTable, dataMapSchema) {
  var identifier: AbsoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier

  /**
   * Return a new write for this datamap
   */
  override def createWriter(segment: Segment, shardName: String, segmentProperties: SegmentProperties): DataMapWriter = {
    new CGDataMapWriter(carbonTable, segment, shardName, dataMapSchema)
  }

  /**
   * Get the datamap for segmentId
   */
  override def getDataMaps(segment: Segment): java.util.List[CoarseGrainDataMap] = {
    val path = identifier.getTablePath
    val file = FileFactory.getCarbonFile(
      path+ "/" +dataMapSchema.getDataMapName + "/" + segment.getSegmentNo)

    val files = file.listFiles()
    files.map {f =>
      val dataMap: CoarseGrainDataMap = new CGDataMap()
      dataMap.init(new DataMapModel(f.getCanonicalPath, new Configuration(false)))
      dataMap
    }.toList.asJava
  }


  /**
   * Get datamaps for distributable object.
   */
  override def getDataMaps(distributable: DataMapDistributable): java.util.List[CoarseGrainDataMap] = {
    val mapDistributable = distributable.asInstanceOf[BlockletDataMapDistributable]
    val dataMap: CoarseGrainDataMap = new CGDataMap()
    dataMap.init(new DataMapModel(mapDistributable.getFilePath, new
        Configuration(false)))
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
   * Get all distributable objects of a segmentId
   *
   * @return
   */
  override def toDistributable(segment: Segment): java.util.List[DataMapDistributable] = {
    val path = identifier.getTablePath
    val file = FileFactory.getCarbonFile(
      path+ "/" +dataMapSchema.getDataMapName + "/" + segment.getSegmentNo)

    val files = file.listFiles()
    files.map { f =>
      val d:DataMapDistributable = new BlockletDataMapDistributable(f.getCanonicalPath)
      d
    }.toList.asJava
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
    new DataMapMeta(carbonTable.getIndexedColumns(dataMapSchema),
      List(ExpressionType.EQUALS, ExpressionType.IN).asJava)
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
  override def willBecomeStale(feature: TableOperation): Boolean = {
    false
  }

  override def createBuilder(segment: Segment,
      shardName: String, segmentProperties: SegmentProperties): DataMapBuilder = {
    ???
  }
}

class CGDataMap extends CoarseGrainDataMap {

  var maxMin: ArrayBuffer[(Int, (Array[Byte], Array[Byte]))] = _
  var FileReader: FileReader = _
  var filePath: String = _
  val compressor = new SnappyCompressor
  var shardName: String = _

  /**
   * It is called to load the data map to memory or to initialize it.
   */
  override def init(dataMapModel: DataMapModel): Unit = {
    val indexPath = FileFactory.getPath(dataMapModel.getFilePath)
    this.shardName = indexPath.getName

    this.filePath = dataMapModel.getFilePath + "/testcg.datamap"
    val carbonFile = FileFactory.getCarbonFile(filePath)
    val size = carbonFile.getSize
    FileReader = FileFactory.getFileHolder(FileFactory.getFileType(filePath))
    val footerLen = FileReader.readInt(filePath, size-4)
    val bytes = FileReader.readByteArray(filePath, size-footerLen-4, footerLen)
    val in = new ByteArrayInputStream(compressor.unCompressByte(bytes))
    val obj = new ObjectInputStream(in)
    maxMin = obj.readObject().asInstanceOf[ArrayBuffer[(Int, (Array[Byte], Array[Byte]))]]
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
      new Blocklet(shardName, f._1 + "")
    }.asJava
  }


  private def findMeta(value: Array[Byte]) = {
    val tuples = maxMin.filter { f =>
      ByteUtil.UnsafeComparer.INSTANCE.compareTo(value, f._2._1) <= 0 &&
      ByteUtil.UnsafeComparer.INSTANCE.compareTo(value, f._2._2) >= 0
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

  /**
   * clears all the resources for datamaps
   */
  override def finish() = {
    ???
  }

  override def getNumberOfEntries: Int = 1
}

class CGDataMapWriter(
    carbonTable: CarbonTable,
    segment: Segment,
    shardName: String,
    dataMapSchema: DataMapSchema)
  extends DataMapWriter(carbonTable.getTablePath, dataMapSchema.getDataMapName,
    carbonTable.getIndexedColumns(dataMapSchema), segment, shardName) {

  val blockletList = new ArrayBuffer[Array[Byte]]()
  val maxMin = new ArrayBuffer[(Int, (Array[Byte], Array[Byte]))]()
  val compressor = new SnappyCompressor

  /**
   * Start of new block notification.
   *
   * @param blockId file name of the carbondata file
   */
  override def onBlockStart(blockId: String): Unit = {
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
    ((blockletId, (sorted.last, sorted.head)))
    blockletList.clear()
  }

  /**
   * Add the column pages row to the datamap, order of pages is same as `index_columns` in
   * DataMapMeta returned in DataMapFactory.
   *
   * Implementation should copy the content of `pages` as needed, because `pages` memory
   * may be freed after this method returns, if using unsafe column page.
   */
  override def onPageAdded(blockletId: Int,
      pageId: Int,
      pageSize: Int,
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
    FileFactory.mkdirs(dataMapPath, FileFactory.getFileType(dataMapPath))
    val file = dataMapPath + "/testcg.datamap"
    val stream: DataOutputStream = FileFactory
      .getDataOutputStream(file, FileFactory.getFileType(file))
    val out = new ByteOutputStream()
    val outStream = new ObjectOutputStream(out)
    outStream.writeObject(maxMin)
    outStream.close()
    val bytes = compressor.compressByte(out.getBytes)
    stream.write(bytes)
    stream.writeInt(bytes.length)
    stream.close()
  }


}

class CGDataMapTestCase extends CarbonQueryTest with BeforeAndAfterAll {

  val file2 = resourcesPath + "/compaction/fil2.csv"
  val systemFolderStoreLocation = CarbonProperties.getInstance().getSystemFolderLocation

  override protected def beforeAll(): Unit = {
    //n should be about 5000000 of reset if size is default 1024
    val n = 150000
    CompactionSupportGlobalSortBigFileTest.createFile(file2, n * 4, n)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "true")
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
    sql(s"create datamap cgdatamap on table datamap_test_cg " +
        s"using '${classOf[CGDataMapFactory].getName}' " +
        s"DMPROPERTIES('index_columns'='name')")
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
    sql(s"create datamap ggdatamap1 on table datamap_test using '${classOf[CGDataMapFactory].getName}' DMPROPERTIES('index_columns'='name')")
    sql(s"create datamap ggdatamap2 on table datamap_test using '${classOf[CGDataMapFactory].getName}' DMPROPERTIES('index_columns'='city')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE datamap_test OPTIONS('header'='false')")
    checkAnswer(sql("select * from datamap_test where name='n502670' and city='c2670'"),
      sql("select * from normal_test where name='n502670' and city='c2670'"))
  }

  test("test invisible datamap during query") {
    val tableName = "datamap_test"
    val dataMapName1 = "datamap1"
    val dataMapName2 = "datamap2"
    sql(s"DROP TABLE IF EXISTS $tableName")
    sql(
      s"""
         | CREATE TABLE $tableName(id INT, name STRING, city STRING, age INT)
         | STORED BY 'org.apache.carbondata.format'
         | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    // register datamap writer
    sql(
      s"""
         | CREATE DATAMAP $dataMapName1
         | ON TABLE $tableName
         | USING '${classOf[CGDataMapFactory].getName}'
         | DMPROPERTIES('index_columns'='name')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $dataMapName2
         | ON TABLE $tableName
         | USING '${classOf[CGDataMapFactory].getName}'
         | DMPROPERTIES('index_columns'='city')
       """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE $tableName OPTIONS('header'='false')")
    val df1 = sql(s"EXPLAIN EXTENDED SELECT * FROM $tableName WHERE name='n502670' AND city='c2670'").collect()
    assert(df1(0).getString(0).contains("CG DataMap"))
    assert(df1(0).getString(0).contains(dataMapName1))
    assert(df1(0).getString(0).contains(dataMapName2))

    // make datamap1 invisible
    sql(s"SET ${CarbonCommonConstants.CARBON_DATAMAP_VISIBLE}default.$tableName.$dataMapName1 = false")
    val df2 = sql(s"EXPLAIN EXTENDED SELECT * FROM $tableName WHERE name='n502670' AND city='c2670'").collect()
    val e = intercept[Exception] {
      assert(df2(0).getString(0).contains(dataMapName1))
    }
    assert(e.getMessage.contains("did not contain \"" + dataMapName1))
    assert(df2(0).getString(0).contains(dataMapName2))
    checkAnswer(sql(s"SELECT * FROM $tableName WHERE name='n502670' AND city='c2670'"),
      sql("SELECT * FROM normal_test WHERE name='n502670' AND city='c2670'"))

    // also make datamap2 invisible
    sql(s"SET ${CarbonCommonConstants.CARBON_DATAMAP_VISIBLE}default.$tableName.$dataMapName2 = false")
    checkAnswer(sql(s"SELECT * FROM $tableName WHERE name='n502670' AND city='c2670'"),
      sql("SELECT * FROM normal_test WHERE name='n502670' AND city='c2670'"))
    val df3 = sql(s"EXPLAIN EXTENDED SELECT * FROM $tableName WHERE name='n502670' AND city='c2670'").collect()
    val e31 = intercept[Exception] {
      assert(df3(0).getString(0).contains(dataMapName1))
    }
    assert(e31.getMessage.contains("did not contain \"" + dataMapName1))
    val e32 = intercept[Exception] {
      assert(df3(0).getString(0).contains(dataMapName2))
    }
    assert(e32.getMessage.contains("did not contain \"" + dataMapName2))

    // make datamap1,datamap2 visible
    sql(s"SET ${CarbonCommonConstants.CARBON_DATAMAP_VISIBLE}default.$tableName.$dataMapName1 = true")
    sql(s"SET ${CarbonCommonConstants.CARBON_DATAMAP_VISIBLE}default.$tableName.$dataMapName1 = true")
    checkAnswer(sql(s"SELECT * FROM $tableName WHERE name='n502670' AND city='c2670'"),
      sql("SELECT * FROM normal_test WHERE name='n502670' AND city='c2670'"))
    val df4 = sql(s"EXPLAIN EXTENDED SELECT * FROM $tableName WHERE name='n502670' AND city='c2670'").collect()
    assert(df4(0).getString(0).contains(dataMapName1))
    val e41 = intercept[Exception] {
      assert(df3(0).getString(0).contains(dataMapName2))
    }
    assert(e41.getMessage.contains("did not contain \"" + dataMapName2))
  }

  test("test datamap storage in system folder") {
    sql("DROP TABLE IF EXISTS datamap_store_test")
    sql(
      """
        | CREATE TABLE datamap_store_test(id INT, name STRING, city STRING, age INT)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)

    val dataMapProvider = classOf[CGDataMapFactory].getName
    sql(
      s"""
         |create datamap test_cg_datamap on table datamap_store_test
         |using '$dataMapProvider'
         |dmproperties('index_columns'='name')
       """.stripMargin)

    val loc = DiskBasedDMSchemaStorageProvider.getSchemaPath(systemFolderStoreLocation, "test_cg_datamap")

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

    val dataMapProvider = classOf[CGDataMapFactory].getName
    sql(
      s"""
         |create datamap test_cg_datamap1 on table datamap_store_test1
         |using '$dataMapProvider'
         |dmproperties('index_columns'='name')
       """.stripMargin)

    val loc = DiskBasedDMSchemaStorageProvider.getSchemaPath(systemFolderStoreLocation, "test_cg_datamap1")

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

    val dataMapProvider = classOf[CGDataMapFactory].getName
    sql(
      s"""
         |create datamap test_cg_datamap2 on table datamap_store_test2
         |using '$dataMapProvider'
         |dmproperties('index_columns'='name')
       """.stripMargin)

    val loc = DiskBasedDMSchemaStorageProvider.getSchemaPath(systemFolderStoreLocation,"test_cg_datamap2")

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
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS,
        CarbonCommonConstants.ENABLE_QUERY_STATISTICS_DEFAULT)
  }

}
