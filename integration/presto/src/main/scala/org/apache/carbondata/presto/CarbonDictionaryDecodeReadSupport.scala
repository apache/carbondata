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

import com.facebook.presto.spi.block.SliceArrayBlock
import io.airlift.slice.{Slice, Slices}
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
  private var dictionarySliceArray: Array[SliceArrayBlock] = _

  /**
   * This initialization is done inside executor task
   * for column dictionary involved in decoding.
   *
   * @param carbonColumns column list
   */

  override def initialize(carbonColumns: Array[CarbonColumn], carbonTable: CarbonTable) {

    dictionaries = new Array[Dictionary](carbonColumns.length)
    dataTypes = new Array[DataType](carbonColumns.length)
    dictionarySliceArray = new Array[SliceArrayBlock](carbonColumns.length)

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
        // in case of string data type create dictionarySliceArray same as that of presto code
        if (dataTypes(index).equals(DataTypes.STRING)) {
          dictionarySliceArray(index) = createSliceArrayBlock(dictionaries(index))
        }
      }

      else {
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
  private def createSliceArrayBlock(dictionaryData: Dictionary): SliceArrayBlock = {
    val chunks: DictionaryChunksWrapper = dictionaryData.getDictionaryChunks
    val sliceArray = new Array[Slice](chunks.getSize + 1)
    // Initialize Slice Array with Empty Slice as per Presto's code
    sliceArray(0) = Slices.EMPTY_SLICE
    var count = 1
    while (chunks.hasNext) {
      {
        val value: Array[Byte] = chunks.next
        sliceArray(count) = wrappedBuffer(value, 0, value.length)
        count += 1
      }
    }
    new SliceArrayBlock(sliceArray.length, sliceArray, true)
  }

  override def readRow(data: Array[AnyRef]): T = {
    throw new RuntimeException("UnSupported Method")
  }

  /**
   * Function to get the SliceArrayBlock with dictionary Data
   *
   * @param columnNo
   * @return
   */
  def getSliceArrayBlock(columnNo: Int): SliceArrayBlock = {
    dictionarySliceArray(columnNo)
  }

  def getDictionaries: Array[Dictionary] = {
    dictionaries
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
