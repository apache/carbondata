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
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.command.{Checker, MetadataCommand}
import org.apache.spark.sql.types.StringType

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.cache.dictionary.AbstractColumnDictionaryInfo
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexWrapper
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.datamap.bloom.BloomCacheKeyValue
import org.apache.carbondata.events.{OperationContext, OperationListenerBus, ShowTableCacheEvent}
import org.apache.carbondata.spark.util.CommonUtil.bytesToDisplaySize


case class CarbonShowCacheCommand(tableIdentifier: Option[TableIdentifier],
    internalCall: Boolean = false)
  extends MetadataCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def output: Seq[AttributeReference] = {
    if (tableIdentifier.isEmpty) {
      Seq(
        AttributeReference("Database", StringType, nullable = false)(),
        AttributeReference("Table", StringType, nullable = false)(),
        AttributeReference("Index size", StringType, nullable = false)(),
        AttributeReference("Datamap size", StringType, nullable = false)(),
        AttributeReference("Dictionary size", StringType, nullable = false)())
    } else {
      Seq(
        AttributeReference("Field", StringType, nullable = false)(),
        AttributeReference("Size", StringType, nullable = false)(),
        AttributeReference("Comment", StringType, nullable = false)())
    }
  }

  override protected def opName: String = "SHOW CACHE"

  def getAllTablesCache(sparkSession: SparkSession): Seq[Row] = {
    val currentDatabase = sparkSession.sessionState.catalog.getCurrentDatabase
    val cache = CacheProvider.getInstance().getCarbonCache
    if (cache == null) {
      Seq(
        Row("ALL", "ALL", 0L, 0L, 0L),
        Row(currentDatabase, "ALL", 0L, 0L, 0L))
    } else {
      var allCarbonTables = mutable.ArrayBuffer[CarbonTable]()
      sparkSession.sessionState.catalog.listTables(currentDatabase).foreach {
        tableIdent =>
          try {
            allCarbonTables += CarbonEnv.getCarbonTable(tableIdent)(sparkSession)
          } catch {
            case ex: NoSuchTableException =>
              LOGGER.debug("Ignoring non-carbon table " + tableIdent.table)
          }
      }
      val carbonTables = allCarbonTables.filter {
        carbonTable =>
          !carbonTable.isChildDataMap
      }

      // All tables of current database
      var (dbDatamapSize, dbDictSize) = (0L, 0L)
      val tableList = carbonTables.flatMap {
        carbonTable =>
          try {
            val tableResult = getTableCache(sparkSession, carbonTable)
            var (indexSize, datamapSize) = (tableResult(0).getLong(1), 0L)
            tableResult.drop(2).foreach {
              row =>
                indexSize += row.getLong(1)
                datamapSize += row.getLong(2)
            }
            val dictSize = tableResult(1).getLong(1)

            dbDictSize += dictSize
            dbDatamapSize += datamapSize

            val tableName = if (!carbonTable.isTransactionalTable) {
              carbonTable.getTableName + " (external table)"
            }
            else {
              carbonTable.getTableName
            }
            Seq((currentDatabase, tableName, indexSize, datamapSize, dictSize))
          } catch {
            case ex: UnsupportedOperationException =>
              Seq.empty
          }
      }.collect {
        case (db, table, indexSize, datamapSize, dictSize) if !((indexSize == 0) &&
                                                                (datamapSize == 0) &&
                                                                (dictSize == 0)) =>
          Row(db, table, indexSize, datamapSize, dictSize)
      }

      val tablePaths = allCarbonTables.map {
        carbonTable =>
          carbonTable.getTablePath + CarbonCommonConstants.FILE_SEPARATOR
      }

      // Scan whole cache and fill the entries for All-Database-All-Tables
      // and Current-Database-All-Tables
      var (allIndexSize, allDatamapSize, allDictSize) = (0L, 0L, 0L)
      var dbIndexSize = 0L
      cache.getCacheMap.asScala.foreach {
        case (key, cacheable) =>
          cacheable match {
            case _: BlockletDataMapIndexWrapper =>
              allIndexSize += cacheable.getMemorySize
              if (tablePaths.exists { path => key.startsWith(path) }) {
                dbIndexSize += cacheable.getMemorySize
              }
            case _: BloomCacheKeyValue.CacheValue =>
              allDatamapSize += cacheable.getMemorySize
            case _: AbstractColumnDictionaryInfo =>
              allDictSize += cacheable.getMemorySize
          }
      }

      Seq(
        Row("ALL", "ALL", allIndexSize, allDatamapSize, allDictSize),
        Row(currentDatabase, "ALL", dbIndexSize, dbDatamapSize, dbDictSize)
      ) ++ tableList
    }
  }

  def getTableCache(sparkSession: SparkSession, carbonTable: CarbonTable): Seq[Row] = {
    val cache = CacheProvider.getInstance().getCarbonCache
    val allIndexFiles: List[String] = CacheUtil.getAllIndexFiles(carbonTable)
    if (cache == null) {
      var comments = 0 + "/" + allIndexFiles.size + " index files cached"
      if (!carbonTable.isTransactionalTable) {
        comments += " (external table)"
      }
      return Seq(
        Row("Index", 0L, comments),
        Row("Dictionary", 0L, "")
      )
    }

    val showTableCacheEvent = ShowTableCacheEvent(carbonTable, sparkSession, internalCall)
    val operationContext = new OperationContext
    // datamapName -> (datamapProviderName, indexSize, datamapSize)
    val currentTableSizeMap = scala.collection.mutable.Map[String, (String, String, Long, Long)]()
    operationContext.setProperty(carbonTable.getTableUniqueName, currentTableSizeMap)
    OperationListenerBus.getInstance.fireEvent(showTableCacheEvent, operationContext)

    // Get all Index files for the specified table in cache
    val indexFilesInCache: List[String] = allIndexFiles.filter {
      indexFile =>
        cache.get(indexFile) != null
    }
    val sizeOfIndexFilesInCache: Long = indexFilesInCache.map {
      indexFile =>
        cache.get(indexFile).getMemorySize
    }.sum

    // Extract dictionary keys for the table and create cache keys from those
    val dictKeys = CacheUtil.getAllDictCacheKeys(carbonTable)
    val sizeOfDictInCache = dictKeys.collect {
      case dictKey if cache.get(dictKey) != null =>
        cache.get(dictKey).getMemorySize
    }.sum

    // Assemble result for all the datamaps for the table
    val otherDatamaps = operationContext.getProperty(carbonTable.getTableUniqueName)
      .asInstanceOf[mutable.Map[String, (String, Long, Long)]]
    val otherDatamapsResults: Seq[Row] = otherDatamaps.map {
      case (name, (provider, indexSize, dmSize)) =>
        Row(name, indexSize, dmSize, provider)
    }.toSeq

    var comments = indexFilesInCache.size + "/" + allIndexFiles.size + " index files cached"
    if (!carbonTable.isTransactionalTable) {
      comments += " (external table)"
    }
    Seq(
      Row("Index", sizeOfIndexFilesInCache, comments),
      Row("Dictionary", sizeOfDictInCache, "")
    ) ++ otherDatamapsResults
  }

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    if (tableIdentifier.isEmpty) {
      /**
       * Assemble result for database
       */
      val result = getAllTablesCache(sparkSession)
      result.map {
        row =>
          Row(row.get(0), row.get(1), bytesToDisplaySize(row.getLong(2)),
            bytesToDisplaySize(row.getLong(3)), bytesToDisplaySize(row.getLong(4)))
      }
    } else {
      /**
       * Assemble result for table
       */
      val carbonTable = CarbonEnv.getCarbonTable(tableIdentifier.get)(sparkSession)
      Checker
        .validateTableExists(tableIdentifier.get.database, tableIdentifier.get.table, sparkSession)
      val rawResult = getTableCache(sparkSession, carbonTable)
      val result = rawResult.slice(0, 2) ++
                   rawResult.drop(2).map {
                     row =>
                       Row(row.get(0), row.getLong(1) + row.getLong(2), row.get(3))
                   }
      result.map {
        row =>
          Row(row.get(0), bytesToDisplaySize(row.getLong(1)), row.get(2))
      }
    }
  }
}
