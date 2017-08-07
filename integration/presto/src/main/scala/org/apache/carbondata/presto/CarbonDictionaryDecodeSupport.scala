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

import org.apache.carbondata.core.cache.{Cache, CacheProvider, CacheType}
import org.apache.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.datatype.DataType
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn


class CarbonDictionaryDecodeReaderSupport[T] {

  def initialize(carbonColumns: Array[CarbonColumn],
      absoluteTableIdentifier: AbsoluteTableIdentifier): Array[(DataType, Dictionary, Int)] = {

    carbonColumns.zipWithIndex.filter(dictChecker(_)).map { carbonColumnWithIndex =>
      val (carbonColumn, index) = carbonColumnWithIndex
      val forwardDictionaryCache: Cache[DictionaryColumnUniqueIdentifier, Dictionary] =
        CacheProvider.getInstance()
          .createCache(CacheType.FORWARD_DICTIONARY, absoluteTableIdentifier
            .getStorePath)
      val dict: Dictionary = forwardDictionaryCache
        .get(new DictionaryColumnUniqueIdentifier(absoluteTableIdentifier.getCarbonTableIdentifier,
          carbonColumn.getColumnIdentifier,
          carbonColumn.getDataType))
      (carbonColumn.getDataType, dict, index)
    }
  }

  private def dictChecker(carbonColumWithIndex: (CarbonColumn, Int)): Boolean = {
    val (carbonColumn, _) = carbonColumWithIndex
    if (!carbonColumn.hasEncoding(Encoding.DIRECT_DICTIONARY) && !carbonColumn.isComplex &&
        carbonColumn.hasEncoding(Encoding.DICTIONARY)) {
      true
    } else {
      false
    }
  }

  def readRow(data: Array[Object],
      dictionaries: Array[(DataType, Dictionary, Int)]): Array[Object] = {
    dictionaries.foreach { (dictionary: (DataType, Dictionary, Int)) =>
      val (_, dict, position) = dictionary
      data(position) = dict.getDictionaryValueForKey(data(position).asInstanceOf[Int])
    }
    data
  }

}
