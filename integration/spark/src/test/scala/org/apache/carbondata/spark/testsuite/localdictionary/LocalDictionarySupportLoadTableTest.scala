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
package org.apache.carbondata.spark.testsuite.localdictionary

import java.io.{File, PrintWriter}
import java.util
import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.cache.dictionary.DictionaryByteArrayWrapper
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.block.TableBlockInfo
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk
import org.apache.carbondata.core.datastore.chunk.reader.CarbonDataReaderFactory
import org.apache.carbondata.core.datastore.chunk.reader.dimension.v3.DimensionChunkReaderV3
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.datastore.page.encoding.DefaultEncodingFactory
import org.apache.carbondata.core.metadata.ColumnarFormatVersion
import org.apache.carbondata.core.util.{CarbonMetadataUtil, CarbonProperties, DataFileFooterConverterV3}

class LocalDictionarySupportLoadTableTest extends QueryTest with BeforeAndAfterAll {

  val file1 = resourcesPath + "/local_dictionary_source1.csv"

  val file2 = resourcesPath + "/local_dictionary_complex_data.csv"

  val storePath = warehouse + "/local2/Fact/Part0/Segment_0"

  override protected def beforeAll(): Unit = {
    CarbonProperties.getInstance.addProperty(CarbonCommonConstants.BLOCKLET_SIZE, "10000")
    createFile(file1)
    createComplexData(file2)
    sql("drop table if exists local2")
  }

  test("test LocalDictionary Load For FallBackScenario"){
    sql("drop table if exists local2")
    sql(
      "CREATE TABLE local2(name string) STORED AS carbondata tblproperties" +
      "('local_dictionary_threshold'='2001','local_dictionary_include'='name')")
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')")
    assert(!checkForLocalDictionary(getDimRawChunk(0)))
  }

  test("test successful local dictionary generation"){
    sql("drop table if exists local2")
    sql(
      "CREATE TABLE local2(name string) STORED AS carbondata tblproperties" +
      "('local_dictionary_enable'='true','local_dictionary_threshold'='9001','local_dictionary_include'='name')")
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')")
    assert(checkForLocalDictionary(getDimRawChunk(0)))
  }

  test("test successful local dictionary generation for default configs") {
    sql("drop table if exists local2")
    sql("CREATE TABLE local2(name string) STORED AS carbondata tblproperties('local_dictionary_enable'='true')")
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')")
    assert(checkForLocalDictionary(getDimRawChunk(0)))
  }

  test("test local dictionary generation for local dictionary include") {
    sql("drop table if exists local2")
    sql("CREATE TABLE local2(name string) STORED AS carbondata " +
        "TBLPROPERTIES ('local_dictionary_enable'='true', 'local_dictionary_include'='name')")
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')")
    assert(checkForLocalDictionary(getDimRawChunk(0)))
  }

  test("test local dictionary generation for local dictioanry exclude"){
    sql("drop table if exists local2")
    sql(
      "CREATE TABLE local2(name string) STORED AS carbondata tblproperties" +
      "('local_dictionary_enable'='true')")
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')")
    assert(checkForLocalDictionary(getDimRawChunk(0)))
  }

  test("test local dictionary generation when it is disabled"){
    sql("drop table if exists local2")
    sql(
      "CREATE TABLE local2(name string) STORED AS carbondata tblproperties" +
      "('local_dictionary_enable'='false','local_dictionary_include'='name')")
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')")
    assert(!checkForLocalDictionary(getDimRawChunk(0)))
  }

  test("test local dictionary generation for invalid threshold configurations"){
    sql("drop table if exists local2")
    sql(
      "CREATE TABLE local2(name string) STORED AS carbondata tblproperties" +
      "('local_dictionary_enable'='true','local_dictionary_include'='name','local_dictionary_threshold'='300000')")
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')")
    assert(checkForLocalDictionary(getDimRawChunk(0)))
  }

  test("test local dictionary generation for include and exclude"){
    sql("drop table if exists local2")
    sql(
      "CREATE TABLE local2(name string, age string) STORED AS carbondata tblproperties" +
      "('local_dictionary_enable'='true','local_dictionary_include'='name', 'local_dictionary_exclude'='age')")
    sql("insert into table local2 values('vishal', '30')")
    assert(checkForLocalDictionary(getDimRawChunk(0)))
    assert(!checkForLocalDictionary(getDimRawChunk(1)))
  }

  test("test local dictionary generation for complex type"){
    sql("drop table if exists local2")
    sql(
      "CREATE TABLE local2(name struct<i:string,s:string>) STORED AS carbondata tblproperties" +
      "('local_dictionary_enable'='true','local_dictionary_include'='name')")
    sql("load data inpath '" + file2 +
        "' into table local2 OPTIONS('header'='false','COMPLEX_DELIMITER_LEVEL_1'='$', " +
        "'COMPLEX_DELIMITER_LEVEL_2'=':')")
    assert(!checkForLocalDictionary(getDimRawChunk(0)))
    assert(checkForLocalDictionary(getDimRawChunk(1)))
    assert(checkForLocalDictionary(getDimRawChunk(2)))
  }

  test("test local dictionary generation for map type") {
    sql("drop table if exists local2")
    sql(
      "CREATE TABLE local2(name map<string,string>) STORED AS carbondata tblproperties" +
      "('local_dictionary_enable'='true','local_dictionary_include'='name')")
    sql("insert into local2 values(map('Manish','Gupta','Manish','Nalla','Shardul','Singh','Vishal','Kumar'))")
    checkAnswer(sql("select * from local2"), Seq(
      Row(Map("Manish" -> "Nalla", "Shardul" -> "Singh", "Vishal" -> "Kumar"))))
    assert(!checkForLocalDictionary(getDimRawChunk(0)))
    assert(!checkForLocalDictionary(getDimRawChunk(1)))
    assert(checkForLocalDictionary(getDimRawChunk(2)))
    assert(checkForLocalDictionary(getDimRawChunk(3)))
  }

  test("test local dictionary data validation") {
    sql("drop table if exists local_query_enable")
    sql("drop table if exists local_query_disable")
    sql(
      "CREATE TABLE local_query_enable(name string) STORED AS carbondata tblproperties" +
      "('local_dictionary_enable'='false','local_dictionary_include'='name')")
    sql("load data inpath '" + file1 + "' into table local_query_enable OPTIONS('header'='false')")
    sql(
      "CREATE TABLE local_query_disable(name string) STORED AS carbondata tblproperties" +
      "('local_dictionary_enable'='true','local_dictionary_include'='name')")
    sql("load data inpath '" + file1 + "' into table local_query_disable OPTIONS('header'='false')")
    checkAnswer(sql("select name from local_query_enable"), sql("select name from local_query_disable"))
  }

  test("test to validate local dictionary values"){
    sql("drop table if exists local2")
    sql("CREATE TABLE local2(name string) STORED AS carbondata tblproperties('local_dictionary_enable'='true')")
    sql("load data inpath '" + resourcesPath + "/localdictionary.csv" + "' into table local2")
    val dimRawChunk = getDimRawChunk(0)
    val dictionaryData = Array("vishal", "kumar", "akash", "praveen", "brijoo")
    try
      assert(validateDictionary(dimRawChunk.get(0), dictionaryData))
    catch {
      case e: Exception =>
        assert(false)
    }
  }

  override protected def afterAll(): Unit = {
    sql("drop table if exists local2")
    deleteFile(file1)
    deleteFile(file2)
    CarbonProperties.getInstance
      .addProperty(CarbonCommonConstants.BLOCKLET_SIZE,
        CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL)
  }

  /**
   * create the csv data file
   *
   * @param fileName
   */
  private def createFile(fileName: String, line: Int = 9000, start: Int = 0): Unit = {
    val writer = new PrintWriter(new File(fileName))
    val data: util.ArrayList[String] = new util.ArrayList[String]()
    for (i <- start until line) {
      data.add("n" + i)
    }
    Collections.sort(data)
    data.asScala.foreach { eachdata =>
      writer.println(eachdata)
    }
    writer.close()
  }

  /**
   * create the csv for complex data
   *
   * @param fileName
   */
  private def createComplexData(fileName: String, line: Int = 9000, start: Int = 0): Unit = {
    val writer = new PrintWriter(new File(fileName))
    val data: util.ArrayList[String] = new util.ArrayList[String]()
    for (i <- start until line) {
      data.add("n" + i + "$" + (i + 1))
    }
    Collections.sort(data)
    data.asScala.foreach { eachdata =>
      writer.println(eachdata)
    }
    writer.close()
  }

  /**
   * delete csv file after test execution
   *
   * @param fileName
   */
  private def deleteFile(fileName: String): Unit = {
    val file: File = new File(fileName)
    if (file.exists) file.delete
  }

  private def checkForLocalDictionary(dimensionRawColumnChunks: util
  .List[DimensionRawColumnChunk]): Boolean = {
    var isLocalDictionaryGenerated = false
    import scala.collection.JavaConversions._
    for (dimensionRawColumnChunk <- dimensionRawColumnChunks) {
      if (dimensionRawColumnChunk.getDataChunkV3
        .isSetLocal_dictionary) {
        isLocalDictionaryGenerated = true
      }
    }
    isLocalDictionaryGenerated
  }

  /**
   * this method returns true if local dictionary is created for all the blocklets or not
   *
   * @return
   */
  private def getDimRawChunk(blockindex: Int): util.ArrayList[DimensionRawColumnChunk] = {
    val dataFiles = FileFactory.getCarbonFile(storePath)
      .listFiles(new CarbonFileFilter() {
        override def accept(file: CarbonFile): Boolean = {
          if (file.getName
            .endsWith(CarbonCommonConstants.FACT_FILE_EXT)) {
            true
          } else {
            false
          }
        }
      })
    val dimensionRawColumnChunks = read(dataFiles(0).getAbsolutePath,
      blockindex)
    dimensionRawColumnChunks
  }

  private def read(filePath: String, blockIndex: Int) = {
    val carbonDataFiles = new File(filePath)
    val dimensionRawColumnChunks = new
        util.ArrayList[DimensionRawColumnChunk]
    val offset = carbonDataFiles.length
    val converter = new DataFileFooterConverterV3
    val fileReader = FileFactory.getFileHolder(FileFactory.getFileType(filePath))
    val actualOffset = fileReader.readLong(carbonDataFiles.getAbsolutePath, offset - 8)
    val blockInfo = new TableBlockInfo(carbonDataFiles.getAbsolutePath,
      actualOffset,
      "0",
      new Array[String](0),
      carbonDataFiles.length,
      ColumnarFormatVersion.V3,
      null)
    val dataFileFooter = converter.readDataFileFooter(blockInfo)
    val blockletList = dataFileFooter.getBlockletList
    import scala.collection.JavaConversions._
    for (blockletInfo <- blockletList) {
      val dimensionColumnChunkReader =
        CarbonDataReaderFactory
          .getInstance
          .getDimensionColumnChunkReader(ColumnarFormatVersion.V3,
            blockletInfo,
            carbonDataFiles.getAbsolutePath,
            false).asInstanceOf[DimensionChunkReaderV3]
      dimensionRawColumnChunks
        .add(dimensionColumnChunkReader.readRawDimensionChunk(fileReader, blockIndex))
    }
    dimensionRawColumnChunks
  }

  private def validateDictionary(rawColumnPage: DimensionRawColumnChunk,
      data: Array[String]): Boolean = {
    val local_dictionary = rawColumnPage.getDataChunkV3.local_dictionary
    if (null != local_dictionary) {
      val compressorName = CarbonMetadataUtil.getCompressorNameFromChunkMeta(
        rawColumnPage.getDataChunkV3.getData_chunk_list.get(0).getChunk_meta)
      val encodings = local_dictionary.getDictionary_meta.encoders
      val encoderMetas = local_dictionary.getDictionary_meta.getEncoder_meta
      val encodingFactory = DefaultEncodingFactory.getInstance
      val decoder = encodingFactory.createDecoder(encodings, encoderMetas, compressorName)
      val dictionaryPage = decoder
        .decode(local_dictionary.getDictionary_data, 0, local_dictionary.getDictionary_data.length)
      val dictionaryMap = new
          util.HashMap[DictionaryByteArrayWrapper, Integer]
      val usedDictionaryValues = util.BitSet
        .valueOf(CompressorFactory.getInstance.getCompressor(compressorName)
          .unCompressByte(local_dictionary.getDictionary_values))
      var index = 0
      var i = usedDictionaryValues.nextSetBit(0)
      while ( { i >= 0 }) {
        dictionaryMap
          .put(new DictionaryByteArrayWrapper(dictionaryPage.getBytes({ index += 1; index - 1 })),
            i)
        i = usedDictionaryValues.nextSetBit(i + 1)
      }
      for (i <- data.indices) {
        if (null == dictionaryMap.get(new DictionaryByteArrayWrapper(data(i).getBytes))) {
          return false
        }
      }
      return true
    }
    false
  }

}
