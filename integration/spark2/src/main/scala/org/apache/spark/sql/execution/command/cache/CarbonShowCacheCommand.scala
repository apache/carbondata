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
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexWrapper
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.datamap.bloom.BloomCacheKeyValue

/**
 * SHOW CACHE
 */
case class CarbonShowCacheCommand(tableIdentifier: Option[TableIdentifier])
  extends DataCommand {

  override def output = {
    if(tableIdentifier.isEmpty) {
      Seq(
        AttributeReference("Database", StringType, nullable = false)(),
        AttributeReference("Table", StringType, nullable = false)(),
        AttributeReference("Index size", LongType, nullable = false)(),
        AttributeReference("Datamap size", LongType, nullable = false)(),
        AttributeReference("Dictionary size", LongType, nullable = false)())
    } else {
      Seq(
        AttributeReference("Field", StringType, nullable = false)(),
        AttributeReference("Size", StringType, nullable = false)())
    }
  }

  override protected def opName: String = "SHOW CACHE"

  def showAllTablesCache(sparkSession: SparkSession): Seq[Row] = {
    val currentDatabase = sparkSession.sessionState.catalog.getCurrentDatabase
    val cache = CacheProvider.getInstance().getCarbonCache()
    if (cache == null) {
      Seq(Row("ALL", "ALL", 0L, 0L, 0L),
        Row(currentDatabase, "ALL", 0L, 0L, 0L))
    } else {
      val tableIdents = sparkSession.sessionState.catalog.listTables(currentDatabase).toArray
      val dbLocation = CarbonEnv.getDatabaseLocation(currentDatabase, sparkSession)
      val tempLocation = dbLocation.replace(
        CarbonCommonConstants.WINDOWS_FILE_SEPARATOR, CarbonCommonConstants.FILE_SEPARATOR)
      val tablePaths = tableIdents.map { tableIdent =>
        (tempLocation + CarbonCommonConstants.FILE_SEPARATOR +
         tableIdent.table + CarbonCommonConstants.FILE_SEPARATOR,
          CarbonEnv.getDatabaseName(tableIdent.database)(sparkSession) + "." + tableIdent.table)
      }

      val dictIds = tableIdents
        .map { tableIdent =>
          var table: CarbonTable = null
          try {
            table = CarbonEnv.getCarbonTable(tableIdent)(sparkSession)
          } catch {
            case _ =>
          }
          table
        }
        .filter(_ != null)
        .flatMap { table =>
          table
            .getAllDimensions
            .asScala
            .filter(_.isGlobalDictionaryEncoding)
            .toArray
            .map(dim => (dim.getColumnId, table.getDatabaseName + "." + table.getTableName))
        }

      // all databases
      var (allIndexSize, allDatamapSize, allDictSize) = (0L, 0L, 0L)
      // current database
      var (dbIndexSize, dbDatamapSize, dbDictSize) = (0L, 0L, 0L)
      val tableMapIndexSize = mutable.HashMap[String, Long]()
      val tableMapDatamapSize = mutable.HashMap[String, Long]()
      val tableMapDictSize = mutable.HashMap[String, Long]()
      val cacheIterator = cache.getCacheMap.entrySet().iterator()
      while (cacheIterator.hasNext) {
        val entry = cacheIterator.next()
        val cache = entry.getValue
        if (cache.isInstanceOf[BlockletDataMapIndexWrapper]) {
          // index
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
        } else if (cache.isInstanceOf[BloomCacheKeyValue.CacheValue]) {
          // bloom datamap
          allDatamapSize = allDatamapSize + cache.getMemorySize
          val shardPath = entry.getKey.replace(CarbonCommonConstants.WINDOWS_FILE_SEPARATOR,
            CarbonCommonConstants.FILE_SEPARATOR)
          val tablePath = tablePaths.find(path => shardPath.contains(path._1))
          if (tablePath.isDefined) {
            dbDatamapSize = dbDatamapSize + cache.getMemorySize
            val memorySize = tableMapIndexSize.get(tablePath.get._2)
            if (memorySize.isEmpty) {
              tableMapDatamapSize.put(tablePath.get._2, cache.getMemorySize)
            } else {
              tableMapDatamapSize.put(tablePath.get._2, memorySize.get + cache.getMemorySize)
            }
          }
        } else if (cache.isInstanceOf[AbstractColumnDictionaryInfo]) {
          // dictionary
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
      if (tableMapIndexSize.isEmpty && tableMapDatamapSize.isEmpty && tableMapDictSize.isEmpty) {
        Seq(Row("ALL", "ALL", allIndexSize, allDatamapSize, allDictSize),
          Row(currentDatabase, "ALL", 0L, 0L, 0L))
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
            val datamapSize = tableMapDatamapSize.getOrElse(uniqueName, 0L)
            val dictSize = tableMapDictSize.getOrElse(uniqueName, 0L)
            Row(values(0), values(1), indexSize, datamapSize, dictSize)
          }

        Seq(Row("ALL", "ALL", allIndexSize, allDatamapSize, allDictSize),
          Row(currentDatabase, "ALL", dbIndexSize, dbDatamapSize, dbDictSize)) ++ tableList
      }
    }
  }

  def showTableCache(sparkSession: SparkSession, tableIdent: TableIdentifier): Seq[Row] = {
    val cache = CacheProvider.getInstance().getCarbonCache()
    val carbonTable = CarbonEnv.getCarbonTable(tableIdent)(sparkSession)
    if(carbonTable.isChildDataMap) {
      throw new UnsupportedOperationException("Operation not allowed on child table.")
    }
    if (cache == null) {
      Seq.empty
    } else {
      val dbLocation = CarbonEnv
        .getDatabaseLocation(tableIdent.database.get, sparkSession)
        .replace(CarbonCommonConstants.WINDOWS_FILE_SEPARATOR, CarbonCommonConstants.FILE_SEPARATOR)
      val tablePath = dbLocation + CarbonCommonConstants.FILE_SEPARATOR +
                      tableIdent.table + CarbonCommonConstants.FILE_SEPARATOR

      // Path -> Name, Type
      val datamapName = mutable.Map[String, (String, String)]()
      // Path -> Size
      val datamapSize = mutable.Map[String, Long]()
      // parent table
      datamapName.put(tablePath, ("", ""))
      datamapSize.put(tablePath, 0)
      // children tables
      for( schema <- carbonTable.getTableInfo.getDataMapSchemaList.asScala ) {
        val path = dbLocation + CarbonCommonConstants.FILE_SEPARATOR + tableIdent.table + "_" +
                   schema.getDataMapName + CarbonCommonConstants.FILE_SEPARATOR
        val name = schema.getDataMapName
        val dmType = schema.getProviderName
        datamapName.put(path, (name, dmType))
        datamapSize.put(path, 0)
      }
      // index schemas
      for( schema <- DataMapStoreManager.getInstance().getDataMapSchemasOfTable(carbonTable).asScala) {
        val path = dbLocation + CarbonCommonConstants.FILE_SEPARATOR + tableIdent.table +
                   CarbonCommonConstants.FILE_SEPARATOR + schema.getDataMapName +
                   CarbonCommonConstants.FILE_SEPARATOR
        val name = schema.getDataMapName
        val dmType = schema.getProviderName
        datamapName.put(path, (name, dmType))
        datamapSize.put(path, 0)
      }

      var dictSize = 0L

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

          val pathEntry = datamapSize.filter(entry => indexPath.startsWith(entry._1))
          if(pathEntry.nonEmpty) {
            val (path, size) = pathEntry.iterator.next()
            datamapSize.put(path, size + cache.getMemorySize)
          }
        } else if (cache.isInstanceOf[BloomCacheKeyValue.CacheValue]) {
          // bloom datamap
          val shardPath = entry.getKey.replace(CarbonCommonConstants.WINDOWS_FILE_SEPARATOR,
            CarbonCommonConstants.FILE_SEPARATOR)
          val pathEntry = datamapSize.filter(entry => shardPath.contains(entry._1))
          if(pathEntry.nonEmpty) {
            val (path, size) = pathEntry.iterator.next()
            datamapSize.put(path, size + cache.getMemorySize)
          }
        } else if (cache.isInstanceOf[AbstractColumnDictionaryInfo]) {
          // dictionary
          val dictId = dictIds.find(id => entry.getKey.startsWith(id))
          if (dictId.isDefined) {
            dictSize = dictSize + cache.getMemorySize
          }
        }
      }

      var result = Seq(
        Row("Index", humanReadableFormat(datamapSize.get(tablePath).get)),
        Row("Dictionary", humanReadableFormat(dictSize))
      )
      for((path, size) <- datamapSize) {
        if(path != tablePath) {
          val (dmName, dmType) = datamapName.get(path).get
          result = result :+ Row(dmName + " (" + dmType + ")", humanReadableFormat(size))
        }
      }
      result
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

  def humanReadableFormat(size: Long): String = {
    var sizeDouble: Double = size
    if(sizeDouble < 1024) {
      return sizeDouble + " Bytes"
    }

    sizeDouble /= 1024
    if(sizeDouble < 1024) {
      return sizeDouble + " KB"
    }

    sizeDouble /= 1024
    if(sizeDouble < 1024) {
      return sizeDouble + " MB"
    }

    sizeDouble /= 1024
    sizeDouble + " GB"
  }
}
