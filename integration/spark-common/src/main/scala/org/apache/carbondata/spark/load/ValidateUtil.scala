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

import scala.collection.JavaConverters._

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.loading.sort.SortScopeOptions
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException

object ValidateUtil {
  def validateDateFormat(dateFormat: String, table: CarbonTable, tableName: String): Unit = {
    val dimensions = table.getDimensionByTableName(tableName).asScala
    // allowing empty value to be configured for dateformat option.
    if (dateFormat != null && dateFormat.trim != "") {
        val dateFormats: Array[String] = dateFormat.split(CarbonCommonConstants.COMMA)
        for (singleDateFormat <- dateFormats) {
          val dateFormatSplits: Array[String] = singleDateFormat.split(":", 2)
          val columnName = dateFormatSplits(0).trim.toLowerCase
          if (!dimensions.exists(_.getColName.equals(columnName))) {
            throw new MalformedCarbonCommandException("Error: Wrong Column Name " +
              dateFormatSplits(0) +
              " is provided in Option DateFormat.")
          }
          if (dateFormatSplits.length < 2 || dateFormatSplits(1).trim.isEmpty) {
            throw new MalformedCarbonCommandException("Error: Option DateFormat is not provided " +
              "for " + "Column " + dateFormatSplits(0) +
              ".")
          }
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
