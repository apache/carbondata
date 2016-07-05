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
package org.carbondata.spark.tasks

import scala.collection.mutable

import org.carbon.common.transaction.Task

import org.carbondata.common.factory.CarbonCommonFactory
import org.carbondata.core.cache.dictionary.Dictionary
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.writer.CarbonDictionaryWriter
import org.carbondata.spark.rdd.DictionaryLoadModel
import org.carbondata.spark.rdd.DictionaryStats

/**
 *
 * @param valuesBuffer
 * @param dictionary
 * @param model
 * @param columnIndex
 * @param writer
 */
class DictionaryWriterTask(valuesBuffer: mutable.HashSet[String],
    dictionary: Dictionary,
    model: DictionaryLoadModel, columnIndex: Int,
    var writer: CarbonDictionaryWriter = null) extends Task[DictionaryStats] {

  /**
   * execute the task
   *
   * @return statistics of dictionary transaction
   */
  def execute(): DictionaryStats = {
    val start = System.currentTimeMillis()
    val values = valuesBuffer.toArray
    java.util.Arrays.sort(values, Ordering[String])
    var distinctValueCount: Int = 0
    val dictService = CarbonCommonFactory.getDictionaryService
    writer = dictService.getDictionaryWriter(
      model.table,
      model.columnIdentifier(columnIndex),
      model.hdfsLocation)
    val distinctValues: java.util.List[String] = new java.util.ArrayList()

    try {
      if (!model.dictFileExists(columnIndex)) {
        writer.write(CarbonCommonConstants.MEMBER_DEFAULT_VAL)
        distinctValues.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL)
        distinctValueCount += 1
      }

      if (values.length >= 1) {
        var preValue = values(0)
        if (model.dictFileExists(columnIndex)) {
          if (dictionary.getSurrogateKey(values(0)) == CarbonCommonConstants
            .INVALID_SURROGATE_KEY) {
            writer.write(values(0))
            distinctValues.add(values(0))
            distinctValueCount += 1
          }
          for (i <- 1 until values.length) {
            if (preValue != values(i)) {
              if (dictionary.getSurrogateKey(values(i)) ==
                  CarbonCommonConstants.INVALID_SURROGATE_KEY) {
                writer.write(values(i))
                distinctValues.add(values(i))
                preValue = values(i)
                distinctValueCount += 1
              }
            }
          }

        } else {
          writer.write(values(0))
          distinctValues.add(values(0))
          distinctValueCount += 1
          for (i <- 1 until values.length) {
            if (preValue != values(i)) {
              writer.write(values(i))
              distinctValues.add(values(i))
              preValue = values(i)
              distinctValueCount += 1
            }
          }
        }
      }
    } finally {
       writer.close()
    }

    DictionaryStats(distinctValues, (System.currentTimeMillis() - start), 0)
  }

  /**
   * commit the transaction
   */
  def commit() {
    if (null != writer) {
      writer.commit()
    }
  }

  def rollback() {
    // do nothing
  }
}
