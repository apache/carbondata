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

package org.apache.carbondata.spark.load

import java.text.SimpleDateFormat

import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, MalformedCarbonCommandException}
import org.apache.carbondata.processing.loading.sort.SortScopeOptions

object ValidateUtil {

  /**
   * validates both timestamp and date for illegal values
   *
   * @param dateTimeLoadFormat
   * @param dateTimeLoadOption
   */
  def validateDateTimeFormat(dateTimeLoadFormat: String, dateTimeLoadOption: String): Unit = {
    // allowing empty value to be configured for dateformat option.
    if (dateTimeLoadFormat != null && dateTimeLoadFormat.trim != "") {
      try {
        new SimpleDateFormat(dateTimeLoadFormat)
      } catch {
        case _: IllegalArgumentException =>
          throw new MalformedCarbonCommandException(s"Error: Wrong option: $dateTimeLoadFormat is" +
                                                    s" provided for option $dateTimeLoadOption")
      }
    }
  }

  def validateSortScope(carbonTable: CarbonTable, sortScope: String): Unit = {
    if (sortScope != null) {
      // Don't support use global sort on partitioned table.
      if (carbonTable.getPartitionInfo(carbonTable.getTableName) != null &&
          sortScope.equalsIgnoreCase(SortScopeOptions.SortScope.GLOBAL_SORT.toString)) {
        throw new MalformedCarbonCommandException("Don't support use global sort on partitioned " +
          "table.")
      }
    }
  }

  def validateGlobalSortPartitions(globalSortPartitions: String): Unit = {
    if (globalSortPartitions != null) {
      try {
        val num = globalSortPartitions.toInt
        if (num <= 0) {
          throw new MalformedCarbonCommandException("'GLOBAL_SORT_PARTITIONS' should be greater " +
            "than 0.")
        }
      } catch {
        case e: NumberFormatException => throw new MalformedCarbonCommandException(e.getMessage)
      }
    }
  }
}
