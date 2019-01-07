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
package org.apache.carbondata.presto

import java.util.Optional

import com.facebook.presto.spi.block.{Block, VariableWidthBlock}
import io.airlift.slice.Slices._

import org.apache.carbondata.core.cache.{Cache, CacheProvider, CacheType}
import org.apache.carbondata.core.cache.dictionary.{Dictionary, DictionaryChunksWrapper, DictionaryColumnUniqueIdentifier}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.datatype.{DataType, DataTypes}
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport

/**
 * This is the class to decode dictionary encoded column data back to its original value.
 */
class CarbonDictionaryDecodeReadSupport[T] extends CarbonReadSupport[T] {
  private var dictionaries: Array[Dictionary] = _
  private var dataTypes: Array[DataType] = _

  /**
   * This initialization is done inside executor task
   * for column dictionary involved in decoding.
   *
   * @param carbonColumns column list
   */

  override def initialize(carbonColumns: Array[CarbonColumn], carbonTable: CarbonTable) {

    dictionaries = new Array[Dictionary](carbonColumns.length)
    dataTypes = new Array[DataType](carbonColumns.length)

    carbonColumns.zipWithIndex.foreach {
      case (carbonColumn, index) => if (carbonColumn.hasEncoding(Encoding.DICTIONARY) &&
                                        !carbonColumn.hasEncoding(Encoding.DIRECT_DICTIONARY) &&
                                        !carbonColumn.isComplex) {
        val cacheProvider: CacheProvider = CacheProvider.getInstance
        val forwardDictionaryCache: Cache[DictionaryColumnUniqueIdentifier, Dictionary] =
          cacheProvider
            .createCache(CacheType.FORWARD_DICTIONARY)
        dataTypes(index) = carbonColumn.getDataType
        val dictionaryPath: String = carbonTable.getTableInfo.getFactTable.getTableProperties
          .get(CarbonCommonConstants.DICTIONARY_PATH)
        dictionaries(index) = forwardDictionaryCache
          .get(new DictionaryColumnUniqueIdentifier(carbonTable.getAbsoluteTableIdentifier,
            carbonColumn.getColumnIdentifier, dataTypes(index), dictionaryPath))
      } else {
        dataTypes(index) = carbonColumn.getDataType
      }
    }

  }

  /**
   * Function to create the SliceArrayBlock with dictionary Data
   *
   * @param dictionaryData
   * @return
   */
  private def createDictionaryBlock(dictionaryData: Dictionary): Block = {
    val chunks: DictionaryChunksWrapper = dictionaryData.getDictionaryChunks
    val positionCount = chunks.getSize

   // In dictionary there will be only one null and the key value will be 1 by default in carbon,
   // hence the isNullVector will be populated only once with null value it has no bearing on
   // actual data.

    val offsetVector : Array[Int] = new Array[Int](positionCount + 2 )
    val isNullVector: Array[Boolean] = new Array[Boolean](positionCount + 1)
    // the first value is just a filler as we always start with index 1 in carbon
    isNullVector(0) = true
    isNullVector(1) = true
    var count = 0
    var byteArray = new Array[Byte](0)
    // The Carbondata key starts from 1 so we need a filler at 0th position hence adding filler to
    // offset, hence 0th Position -> 0
    offsetVector(0) = 0
    while (chunks.hasNext) {
      val value: Array[Byte] = chunks.next
      if (count == 0) {
        // 1 index is actually Null to map to carbondata null values .
        // 1st Position -> 0 (For actual Null)
        offsetVector(count + 1) = 0
        // 2nd Postion -> 0 as the byte[] is still null so starting point will be 0 only
        offsetVector(count + 2) = 0
      } else {
        byteArray = byteArray ++ value
        offsetVector(count + 2) = byteArray.length
      }
      count += 1
    }
    new VariableWidthBlock(positionCount + 1,
      wrappedBuffer(byteArray, 0, byteArray.length),
      offsetVector,
      Optional.ofNullable(isNullVector))
  }

  override def readRow(data: Array[AnyRef]): T = {
    throw new RuntimeException("UnSupported Method")
  }

  def getDictionaries: Array[Dictionary] = {
    dictionaries
  }

  def getDataTypes: Array[DataType] = {
    dataTypes
  }

  /**
   * to book keep the dictionary cache or update access count for each
   * column involved during decode, to facilitate LRU cache policy if memory
   * threshold is reached
   */
  override def close() {
    dictionaries
      .foreach(dictionary => if (Option(dictionary).isDefined) {
        CarbonUtil
          .clearDictionaryCache(dictionary)
      })
  }
}
