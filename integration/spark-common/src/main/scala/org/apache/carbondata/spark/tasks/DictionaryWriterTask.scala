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

import java.io.IOException

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration

import org.apache.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.{CarbonTableIdentifier, ColumnIdentifier}
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.service.CarbonCommonFactory
import org.apache.carbondata.core.util.DataTypeUtil
import org.apache.carbondata.core.writer.CarbonDictionaryWriter

/**
 *
 * @param valuesBuffer
 * @param dictionary
 * @param carbonTableIdentifier
 * @param dictionaryColumnUniqueIdentifier
 * @param carbonStoreLocation
 * @param columnSchema
 * @param isDictionaryFileExist
 * @param writer
 */
class DictionaryWriterTask(valuesBuffer: mutable.HashSet[String],
    dictionary: Dictionary,
    carbonTableIdentifier: CarbonTableIdentifier,
    dictionaryColumnUniqueIdentifier: DictionaryColumnUniqueIdentifier,
    carbonStoreLocation: String,
    columnSchema: ColumnSchema,
    isDictionaryFileExist: Boolean,
    var writer: CarbonDictionaryWriter = null,
    configuration: Configuration) {

  /**
   * execute the task
   *
   * @return distinctValueList and time taken to write
   */
  def execute(): java.util.List[String] = {
    val values = valuesBuffer.toArray
    java.util.Arrays.sort(values, Ordering[String])
    val dictService = CarbonCommonFactory.getDictionaryService
    writer = dictService.getDictionaryWriter(
      carbonTableIdentifier,
      dictionaryColumnUniqueIdentifier,
      carbonStoreLocation, configuration)
    val distinctValues: java.util.List[String] = new java.util.ArrayList()

    try {
      if (!isDictionaryFileExist) {
        writer.write(CarbonCommonConstants.MEMBER_DEFAULT_VAL)
        distinctValues.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL)
      }

      if (values.length >= 1) {
        if (isDictionaryFileExist) {
          for (value <- values) {
            val parsedValue = DataTypeUtil.normalizeColumnValueForItsDataType(value,
              columnSchema)
            if (null != parsedValue && dictionary.getSurrogateKey(parsedValue) ==
              CarbonCommonConstants.INVALID_SURROGATE_KEY) {
              writer.write(parsedValue)
              distinctValues.add(parsedValue)
            }
          }

        } else {
          for (value <- values) {
            val parsedValue = DataTypeUtil.normalizeColumnValueForItsDataType(value,
              columnSchema)
            if (null != parsedValue) {
              writer.write(parsedValue)
              distinctValues.add(parsedValue)
            }
          }
        }
      }
    } catch {
      case ex: IOException =>
        throw ex
    } finally {
      if (null != writer) {
        writer.close()
      }
    }
    distinctValues
  }

  /**
   * update dictionary metadata
   */
  def updateMetaData() {
    if (null != writer) {
      writer.commit()
    }
  }
}
