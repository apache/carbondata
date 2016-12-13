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
package org.apache.carbondata.spark.tasks

import org.apache.carbondata.common.factory.CarbonCommonFactory
import org.apache.carbondata.core.cache.dictionary.Dictionary
import org.apache.carbondata.core.writer.sortindex.{CarbonDictionarySortIndexWriter, CarbonDictionarySortInfo, CarbonDictionarySortInfoPreparator}
import org.apache.carbondata.spark.rdd.DictionaryLoadModel

/**
 * This task writes sort index file
 *
 * @param model
 * @param index
 * @param dictionary
 * @param distinctValues
 * @param carbonDictionarySortIndexWriter
 */
class SortIndexWriterTask(model: DictionaryLoadModel,
    index: Int,
    dictionary: Dictionary,
    distinctValues: java.util.List[String],
    var carbonDictionarySortIndexWriter: CarbonDictionarySortIndexWriter = null) {
  def execute() {
    try {
      if (distinctValues.size() > 0) {
        val preparator: CarbonDictionarySortInfoPreparator = new CarbonDictionarySortInfoPreparator
        val dictService = CarbonCommonFactory.getDictionaryService
        val dictionarySortInfo: CarbonDictionarySortInfo =
          preparator.getDictionarySortInfo(distinctValues, dictionary,
            model.primDimensions(index).getDataType)
        carbonDictionarySortIndexWriter =
          dictService.getDictionarySortIndexWriter(model.table, model.columnIdentifier(index),
            model.hdfsLocation)
        carbonDictionarySortIndexWriter.writeSortIndex(dictionarySortInfo.getSortIndex)
        carbonDictionarySortIndexWriter
          .writeInvertedSortIndex(dictionarySortInfo.getSortIndexInverted)
      }
    } finally {
      if (null != carbonDictionarySortIndexWriter) {
        carbonDictionarySortIndexWriter.close()
      }
    }
  }
}
