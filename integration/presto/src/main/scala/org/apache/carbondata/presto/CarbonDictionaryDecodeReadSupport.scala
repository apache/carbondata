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

import org.apache.carbondata.core.cache.dictionary.Dictionary
import org.apache.carbondata.core.metadata.datatype.DataType
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
      case (carbonColumn, index) =>
        dataTypes(index) = carbonColumn.getDataType
    }

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
