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

import org.apache.commons.io.FileUtils.byteCountToDisplaySize
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.MetadataCommand
import org.apache.spark.sql.types.{LongType, StringType}

import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.cache.dictionary.AbstractColumnDictionaryInfo
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexWrapper
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.datamap.bloom.BloomCacheKeyValue
import org.apache.carbondata.processing.merger.CarbonDataMergerUtil

/**
 * SHOW CACHE
 */
case class CarbonShowCacheCommand(tableIdentifier: Option[TableIdentifier])
  extends MetadataCommand {

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

  def showAllTablesCache(sparkSession: SparkSession): Seq[Row] = {
    val currentDatabase = sparkSession.sessionState.catalog.getCurrentDatabase
    val cache = CacheProvider.getInstance().getCarbonCache()
    if (cache == null) {
      Seq(
        Row("ALL", "ALL", byteCountToDisplaySize(0L),
          byteCountToDisplaySize(0L), byteCountToDisplaySize(0L)),
        Row(currentDatabase, "ALL", byteCountToDisplaySize(0L),
          byteCountToDisplaySize(0L), byteCountToDisplaySize(0L)))
    } else {
      val carbonTables = CarbonEnv.getInstance(sparkSession).carbonMetaStore
        .listAllTables(sparkSession)
        .filter { table =>
        table.getDatabaseName.equalsIgnoreCase(currentDatabase)
      }
      val tablePaths = carbonTables
        .map { table =>
          (table.getTablePath + CarbonCommonConstants.FILE_SEPARATOR,
            table.getDatabaseName + "." + table.getTableName)
      }

      val dictIds = carbonTables
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
            val memorySize = tableMapDatamapSize.get(tablePath.get._2)
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
        Seq(
          Row("ALL", "ALL", byteCountToDisplaySize(allIndexSize),
            byteCountToDisplaySize(allDatamapSize), byteCountToDisplaySize(allDictSize)),
          Row(currentDatabase, "ALL", byteCountToDisplaySize(0),
            byteCountToDisplaySize(0), byteCountToDisplaySize(0)))
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
            Row(values(0), values(1), byteCountToDisplaySize(indexSize),
              byteCountToDisplaySize(datamapSize), byteCountToDisplaySize(dictSize))
          }

        Seq(
          Row("ALL", "ALL", byteCountToDisplaySize(allIndexSize),
            byteCountToDisplaySize(allDatamapSize), byteCountToDisplaySize(allDictSize)),
          Row(currentDatabase, "ALL", byteCountToDisplaySize(dbIndexSize),
            byteCountToDisplaySize(dbDatamapSize), byteCountToDisplaySize(dbDictSize))
        ) ++ tableList
      }
    }
  }

  def showTableCache(sparkSession: SparkSession, carbonTable: CarbonTable): Seq[Row] = {
    val cache = CacheProvider.getInstance().getCarbonCache()
    if (cache == null) {
      Seq.empty
    } else {
      val tablePath = carbonTable.getTablePath + CarbonCommonConstants.FILE_SEPARATOR
      var numIndexFilesCached = 0

      // Path -> Name, Type
      val datamapName = mutable.Map[String, (String, String)]()
      // Path -> Size
      val datamapSize = mutable.Map[String, Long]()
      // parent table
      datamapName.put(tablePath, ("", ""))
      datamapSize.put(tablePath, 0)
      // children tables
      for( schema <- carbonTable.getTableInfo.getDataMapSchemaList.asScala ) {
        val childTableName = carbonTable.getTableName + "_" + schema.getDataMapName
        val childTable = CarbonEnv
          .getCarbonTable(Some(carbonTable.getDatabaseName), childTableName)(sparkSession)
        val path = childTable.getTablePath + CarbonCommonConstants.FILE_SEPARATOR
        val name = schema.getDataMapName
        val dmType = schema.getProviderName
        datamapName.put(path, (name, dmType))
        datamapSize.put(path, 0)
      }
      // index schemas
      for (schema <- DataMapStoreManager.getInstance().getDataMapSchemasOfTable(carbonTable)
        .asScala) {
        val path = tablePath + schema.getDataMapName + CarbonCommonConstants.FILE_SEPARATOR
        val name = schema.getDataMapName
        val dmType = schema.getProviderName
        datamapName.put(path, (name, dmType))
        datamapSize.put(path, 0)
      }

      var dictSize = 0L

      // dictionary column ids
      val dictIds = carbonTable
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
          if(indexPath.startsWith(tablePath)) {
            numIndexFilesCached += 1
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

      // get all index files
      val absoluteTableIdentifier = AbsoluteTableIdentifier.from(tablePath)
      val numIndexFilesAll = CarbonDataMergerUtil.getValidSegmentList(absoluteTableIdentifier)
        .asScala.map {
          segment =>
            segment.getCommittedIndexFile
        }.flatMap {
        indexFilesMap => indexFilesMap.keySet().toArray
      }.size

      var result = Seq(
        Row("Index", byteCountToDisplaySize(datamapSize.get(tablePath).get),
          numIndexFilesCached + "/" + numIndexFilesAll + " index files cached"),
        Row("Dictionary", byteCountToDisplaySize(dictSize), "")
      )
      for ((path, size) <- datamapSize) {
        if (path != tablePath) {
          val (dmName, dmType) = datamapName.get(path).get
          result = result :+ Row(dmName, byteCountToDisplaySize(size), dmType)
        }
      }
      result
    }
  }

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    if (tableIdentifier.isEmpty) {
      showAllTablesCache(sparkSession)
    } else {
      val carbonTable = CarbonEnv.getCarbonTable(tableIdentifier.get)(sparkSession)
      if (carbonTable.isChildDataMap) {
        throw new UnsupportedOperationException("Operation not allowed on child table.")
      }
      showTableCache(sparkSession, carbonTable)
    }
  }
}
