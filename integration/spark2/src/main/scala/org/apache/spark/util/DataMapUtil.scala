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

package org.apache.spark.util

import java.io.IOException
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.execution.command.{DataMapField, Field}

import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider.MV
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}

/**
 * Utility class for keeping all the utility methods common for pre-aggregate and mv datamap
 */
object DataMapUtil {

  def inheritTablePropertiesFromMainTable(parentTable: CarbonTable,
      fields: Seq[Field],
      fieldRelationMap: scala.collection.mutable.LinkedHashMap[Field, DataMapField],
      tableProperties: mutable.Map[String, String]): Unit = {
    var neworder = Seq[String]()
    val parentOrder = parentTable.getSortColumns(parentTable.getTableName).asScala
    parentOrder.foreach(parentcol =>
      fields.filter(col => fieldRelationMap(col).aggregateFunction.isEmpty &&
                           fieldRelationMap(col).columnTableRelationList.size == 1 &&
                           parentcol.equals(fieldRelationMap(col).
                             columnTableRelationList.get(0).parentColumnName))
        .map(cols => neworder :+= cols.column))
    tableProperties.put(CarbonCommonConstants.SORT_COLUMNS, neworder.mkString(","))
    tableProperties.put("sort_scope", parentTable.getTableInfo.getFactTable.
      getTableProperties.asScala.getOrElse("sort_scope", CarbonCommonConstants
      .LOAD_SORT_SCOPE_DEFAULT))
    tableProperties
      .put(CarbonCommonConstants.TABLE_BLOCKSIZE, parentTable.getBlockSizeInMB.toString)
    tableProperties.put(CarbonCommonConstants.FLAT_FOLDER,
      parentTable.getTableInfo.getFactTable.getTableProperties.asScala.getOrElse(
        CarbonCommonConstants.FLAT_FOLDER, CarbonCommonConstants.DEFAULT_FLAT_FOLDER))

    // Datamap table name and columns are automatically added prefix with parent table name
    // in carbon. For convenient, users can type column names same as the ones in select statement
    // when config dmproperties, and here we update column names with prefix.
    // If longStringColumn is not present in dm properties then we take long_string_columns from
    // the parent table.
    var longStringColumn = tableProperties.get(CarbonCommonConstants.LONG_STRING_COLUMNS)
    if (longStringColumn.isEmpty) {
      val longStringColumnInParents = parentTable.getTableInfo.getFactTable.getTableProperties
        .asScala
        .getOrElse(CarbonCommonConstants.LONG_STRING_COLUMNS, "").split(",").map(_.trim)
      val varcharDatamapFields = scala.collection.mutable.ArrayBuffer.empty[String]
      fieldRelationMap foreach (fields => {
        val aggFunc = fields._2.aggregateFunction
        val relationList = fields._2.columnTableRelationList
        // check if columns present in datamap are long_string_col in parent table. If they are
        // long_string_columns in parent, make them long_string_columns in datamap
        if (aggFunc.isEmpty && relationList.size == 1 && longStringColumnInParents
          .contains(relationList.head.head.parentColumnName)) {
          varcharDatamapFields += relationList.head.head.parentColumnName
        }
      })
      if (!varcharDatamapFields.isEmpty) {
        longStringColumn = Option(varcharDatamapFields.mkString(","))
      }
    }

    if (longStringColumn != None) {
      val fieldNames = fields.map(_.column)
      val newLongStringColumn = longStringColumn.get.split(",").map(_.trim).map { colName =>
        val newColName = parentTable.getTableName.toLowerCase() + "_" + colName
        if (!fieldNames.contains(newColName)) {
          throw new MalformedDataMapCommandException(
            CarbonCommonConstants.LONG_STRING_COLUMNS.toUpperCase() + ":" + colName
            + " does not in datamap")
        }
        newColName
      }
      tableProperties.put(CarbonCommonConstants.LONG_STRING_COLUMNS,
        newLongStringColumn.mkString(","))
    }

    // inherit the local dictionary properties of main parent table
    tableProperties
      .put(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE,
        parentTable.getTableInfo.getFactTable.getTableProperties.asScala
          .getOrElse(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE, "false"))
    tableProperties
      .put(CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD,
        parentTable.getTableInfo.getFactTable.getTableProperties.asScala
          .getOrElse(CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD,
            CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD_DEFAULT))
    val parentDictInclude = parentTable.getTableInfo.getFactTable.getTableProperties.asScala
      .getOrElse(CarbonCommonConstants.LOCAL_DICTIONARY_INCLUDE, "").split(",")

    val parentDictExclude = parentTable.getTableInfo.getFactTable.getTableProperties.asScala
      .getOrElse(CarbonCommonConstants.LOCAL_DICTIONARY_EXCLUDE, "").split(",")

    val newDictInclude =
      parentDictInclude.flatMap(parentcol =>
        fields.collect {
          case col if fieldRelationMap(col).aggregateFunction.isEmpty &&
                      fieldRelationMap(col).columnTableRelationList.size == 1 &&
                      parentcol.equals(fieldRelationMap(col).
                        columnTableRelationList.get.head.parentColumnName) =>
            col.column
        })

    val newDictExclude = parentDictExclude.flatMap(parentcol =>
      fields.collect {
        case col if fieldRelationMap(col).aggregateFunction.isEmpty &&
                    fieldRelationMap(col).columnTableRelationList.size == 2 &&
                    parentcol.equals(fieldRelationMap(col).
                      columnTableRelationList.get.head.parentColumnName) =>
          col.column
      })
    if (newDictInclude.nonEmpty) {
      tableProperties
        .put(CarbonCommonConstants.LOCAL_DICTIONARY_INCLUDE, newDictInclude.mkString(","))
    }
    if (newDictExclude.nonEmpty) {
      tableProperties
        .put(CarbonCommonConstants.LOCAL_DICTIONARY_EXCLUDE, newDictExclude.mkString(","))
    }
  }

  /**
   * Return true if MV datamap present in the specified table
   */
  @throws[IOException]
  def hasMVDataMap(carbonTable: CarbonTable): Boolean = {
    val dataMapSchemaList = DataMapStoreManager.getInstance.
      getDataMapSchemasOfTable(carbonTable).asScala
    dataMapSchemaList.foreach { dataMapSchema =>
      if (dataMapSchema.getProviderName.equalsIgnoreCase(MV.toString)) {
        return true
      }
    }
    false
  }
}
