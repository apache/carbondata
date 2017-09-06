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

import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.datamap.{DataMapStoreManager, TableDataMap}
import org.apache.carbondata.core.datamap.dev.DataMapWriter
import org.apache.carbondata.core.datastore.page.ColumnPage
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.datatype.DataType
import org.apache.carbondata.core.util.CarbonProperties


class DataMapMinMaxWriter extends QueryTest with BeforeAndAfterAll {

  def buildTestData(numRows: Int): DataFrame = {
    import sqlContext.implicits._
    sqlContext.sparkContext.parallelize(1 to numRows)
      .map(x => ("a", "b", x))
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
    // register datamap writer
    TableDataMap dataMapMinMax = DataMapStoreManager.getInstance().createAndRegisterDataMap(
      AbsoluteTableIdentifier.from(storeLocation, "default", "carbonMinMax"),
      classOf[DataMapMinMaxClass].getName,
      "test")

    val df = buildTestData(33000)

    // save dataframe to carbon file
    df.write
      .format("carbondata")
      .option("tableName", "carbonMinMax")
      .mode(SaveMode.Overwrite)
      .save()


    assert(DataMapWriterSuite.callbackSeq.head.contains("block start"))
    assert(DataMapWriterSuite.callbackSeq.last.contains("block end"))
    assert(DataMapWriterSuite.callbackSeq.slice(1, DataMapWriterSuite.callbackSeq.length - 1) == Seq(
      "blocklet start 0",
      "add page data: blocklet 0, page 0",
      "add page data: blocklet 0, page 1",
      "blocklet end: 0"
    ))
    DataMapWriterSuite.callbackSeq = Seq()
  }

  test("test write datamap 2 blocklet") {
    // register datamap writer
    DataMapStoreManager.getInstance().createAndRegisterDataMap(
      AbsoluteTableIdentifier.from(storeLocation, "default", "carbon2"),
      classOf[C2DataMapFactory].getName,
      "test")

    CarbonProperties.getInstance()
      .addProperty("carbon.blockletgroup.size.in.mb", "1")

    val df = buildTestData(300000)

    // save dataframe to carbon file
    df.write
      .format("carbondata")
      .option("tableName", "carbon2")
      .mode(SaveMode.Overwrite)
      .save()

    assert(DataMapWriterSuite.callbackSeq.head.contains("block start"))
    assert(DataMapWriterSuite.callbackSeq.last.contains("block end"))
    assert(DataMapWriterSuite.callbackSeq.slice(1, DataMapWriterSuite.callbackSeq.length - 1) == Seq(
      "blocklet start 0",
      "add page data: blocklet 0, page 0",
      "add page data: blocklet 0, page 1",
      "add page data: blocklet 0, page 2",
      "add page data: blocklet 0, page 3",
      "add page data: blocklet 0, page 4",
      "add page data: blocklet 0, page 5",
      "add page data: blocklet 0, page 6",
      "add page data: blocklet 0, page 7",
      "blocklet end: 0",
      "blocklet start 1",
      "add page data: blocklet 1, page 0",
      "add page data: blocklet 1, page 1",
      "blocklet end: 1"
    ))
    DataMapWriterSuite.callbackSeq = Seq()
  }

  override def afterAll {
    dropTable()
  }
}

object DataMapMinMaxWriter {

  val dataMapMinMaxWriter = new DataMapWriter {

    var callbackSeq: Seq[String] = Seq[String]()

    override def onPageAdded(
        blockletId: Int,
        pageId: Int,
        pages: Array[ColumnPage]): Unit = {
      assert(pages.length == 1)
      assert(pages(0).getDataType == DataType.STRING)
      val bytes: Array[Byte] = pages(0).getByteArrayPage()(0)
      assert(bytes.sameElements(Seq(0, 1, 'b'.toByte)))
      callbackSeq :+= s"add page data: blocklet $blockletId, page $pageId"
    }

    def writeMinMax(filePath: String):Unit = {
      updateMinMaxIndex(filePath)
    }

    override def onBlockletEnd(blockletId: Int): Unit = {
      callbackSeq :+= s"blocklet end: $blockletId"
      writeMinMax(filePath);
    }

    override def onBlockEnd(blockId: String): Unit = {
      callbackSeq :+= s"block end $blockId"
    }

    override def onBlockletStart(blockletId: Int): Unit = {
      callbackSeq :+= s"blocklet start $blockletId"
    }

    override def onBlockStart(blockId: String): Unit = {
      callbackSeq :+= s"block start $blockId"
    }

  }
}

