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

package org.apache.spark.sql.execution.command.cache

import scala.collection.mutable
import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.DataCommand
import org.apache.spark.sql.types.{LongType, StringType}

import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.cache.dictionary.AbstractColumnDictionaryInfo
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexWrapper

/**
 * SHOW CACHE
 */
case class CarbonDataShowCacheCommand(tableIdentifier: Option[TableIdentifier])
  extends DataCommand {

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("database", StringType, nullable = false)(),
      AttributeReference("table", StringType, nullable = false)(),
      AttributeReference("index size", LongType, nullable = false)(),
      AttributeReference("dictionary size", LongType, nullable = false)())
  }

  override protected def opName: String = "SHOW CACHE"

  def showAllTablesCache(sparkSession: SparkSession): Seq[Row] = {
    val currentDatabase = sparkSession.sessionState.catalog.getCurrentDatabase
    val cache = CacheProvider.getInstance().getCarbonCache()
    if (cache == null) {
      Seq(Row("ALL", "ALL", 0L, 0L),
        Row(currentDatabase, "ALL", 0L, 0L))
    } else {
      val tableIdents = sparkSession.sessionState.catalog.listTables(currentDatabase).toArray
      val dbLocation = CarbonEnv.getDatabaseLocation(currentDatabase, sparkSession)
      val tempLocation = dbLocation.replace(
        CarbonCommonConstants.WINDOWS_FILE_SEPARATOR, CarbonCommonConstants.FILE_SEPARATOR)
      val tablePaths = tableIdents.map { tableIdent =>
        (tempLocation + CarbonCommonConstants.FILE_SEPARATOR +
         tableIdent.table + CarbonCommonConstants.FILE_SEPARATOR,
          tableIdent.database.get + "." + tableIdent.table)
      }

      val dictIds = tableIdents.flatMap { tableIdent =>
        CarbonEnv
          .getCarbonTable(tableIdent)(sparkSession)
          .getAllDimensions
          .asScala
          .filter(_.isGlobalDictionaryEncoding)
          .toArray
          .map(dim => (dim.getColumnId, tableIdent.database.get + "." + tableIdent.table))
      }

      // all databases
      var (allIndexSize, allDictSize) = (0L, 0L)
      // current database
      var (dbIndexSize, dbDictSize) = (0L, 0L)
      val tableMapIndexSize = mutable.HashMap[String, Long]()
      val tableMapDictSize = mutable.HashMap[String, Long]()
      val cacheIterator = cache.getCacheMap.entrySet().iterator()
      while (cacheIterator.hasNext) {
        val entry = cacheIterator.next()
        val cache = entry.getValue
        if (cache.isInstanceOf[BlockletDataMapIndexWrapper]) {
          allIndexSize = allIndexSize + cache.getMemorySize
          val indexPath = entry.getKey.replace(
            CarbonCommonConstants.WINDOWS_FILE_SEPARATOR, CarbonCommonConstants.FILE_SEPARATOR)
          val tablePath = tablePaths.find(path => indexPath.startsWith(path._1))
          if (tablePath.isDefined) {
            dbIndexSize = dbIndexSize + cache.getMemorySize
            val memorySize = tableMapIndexSize.get(tablePath.get._2)
            if (memorySize.isEmpty) {
              tableMapIndexSize.put(tablePath.get._2, cache.getMemorySize)
            } else {
              tableMapIndexSize.put(tablePath.get._2, memorySize.get + cache.getMemorySize)
            }
          }
        } else if (cache.isInstanceOf[AbstractColumnDictionaryInfo]) {
          allDictSize = allDictSize + cache.getMemorySize
          val dictId = dictIds.find(id => entry.getKey.startsWith(id._1))
          if (dictId.isDefined) {
            dbDictSize = dbDictSize + cache.getMemorySize
            val memorySize = tableMapDictSize.get(dictId.get._2)
            if (memorySize.isEmpty) {
              tableMapDictSize.put(dictId.get._2, cache.getMemorySize)
            } else {
              tableMapDictSize.put(dictId.get._2, memorySize.get + cache.getMemorySize)
            }
          }
        }
      }
      if (tableMapIndexSize.isEmpty && tableMapDictSize.isEmpty) {
        Seq(Row("ALL", "ALL", allIndexSize, allDictSize),
          Row(currentDatabase, "ALL", 0L, 0L))
      } else {
        val tableList = tableMapIndexSize
          .map(_._1)
          .toSeq
          .union(tableMapDictSize.map(_._1).toSeq)
          .distinct
          .sorted
          .map { uniqueName =>
            val values = uniqueName.split("\\.")
            val indexSize = tableMapIndexSize.getOrElse(uniqueName, 0L)
            val dictSize = tableMapDictSize.getOrElse(uniqueName, 0L)
            Row(values(0), values(1), indexSize, dictSize)
          }

        Seq(Row("ALL", "ALL", allIndexSize, allDictSize),
          Row(currentDatabase, "ALL", dbIndexSize, dbDictSize)) ++ tableList
      }
    }
  }

  def showTableCache(sparkSession: SparkSession, tableIdent: TableIdentifier): Seq[Row] = {
    val cache = CacheProvider.getInstance().getCarbonCache()
    if (cache == null) {
      Seq(Row(tableIdent.database.get, tableIdent.table, 0L, 0L))
    } else {
      var indexSize = 0L
      var dictSize = 0L
      val dbLocation = CarbonEnv
        .getDatabaseLocation(tableIdent.database.get, sparkSession)
        .replace(CarbonCommonConstants.WINDOWS_FILE_SEPARATOR,
          CarbonCommonConstants.FILE_SEPARATOR)
      val tablePath = dbLocation + CarbonCommonConstants.FILE_SEPARATOR +
                      tableIdent.table + CarbonCommonConstants.FILE_SEPARATOR
      // dictionary column ids
      val dictIds = CarbonEnv
        .getCarbonTable(tableIdent)(sparkSession)
        .getAllDimensions
        .asScala
        .filter(_.isGlobalDictionaryEncoding)
        .map(_.getColumnId)
        .toArray

      val cacheIterator = cache.getCacheMap.entrySet().iterator()
      while (cacheIterator.hasNext) {
        val entry = cacheIterator.next()
        val cache = entry.getValue

        if (cache.isInstanceOf[BlockletDataMapIndexWrapper]) {
          // index
          val indexPath = entry.getKey.replace(CarbonCommonConstants.WINDOWS_FILE_SEPARATOR,
            CarbonCommonConstants.FILE_SEPARATOR)
          if (indexPath.startsWith(tablePath)) {
            indexSize = indexSize + cache.getMemorySize
          }
        } else if (cache.isInstanceOf[AbstractColumnDictionaryInfo]) {
          // dictionary
          val dictId = dictIds.find(id => entry.getKey.startsWith(id))
          if (dictId.isDefined) {
            dictSize = dictSize + cache.getMemorySize
          }
        }
      }
      Seq(Row(tableIdent.database.get, tableIdent.table, indexSize, dictSize))
    }
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    if (tableIdentifier.isEmpty) {
      showAllTablesCache(sparkSession)
    } else {
      val dbName = CarbonEnv.getDatabaseName(tableIdentifier.get.database)(sparkSession)
      val table = sparkSession.sessionState.catalog.listTables(dbName)
        .find(_.table.equalsIgnoreCase(tableIdentifier.get.table))
      if (table.isEmpty) {
        throw new NoSuchTableException(dbName, tableIdentifier.get.table)
      }
      showTableCache(sparkSession, table.get)
    }
  }
}
