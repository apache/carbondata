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
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexWrapper
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapFactory
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.datamap.bloom.BloomCacheKeyValue
import org.apache.carbondata.events.{OperationContext, OperationListenerBus, ShowTableCacheEvent}
import org.apache.carbondata.indexserver.IndexServer
import org.apache.carbondata.spark.util.CarbonScalaUtil
import org.apache.carbondata.spark.util.CommonUtil.bytesToDisplaySize


case class CarbonShowCacheCommand(tableIdentifier: Option[TableIdentifier],
    internalCall: Boolean = false)
  extends MetadataCommand {

  private lazy val cacheResult: Seq[(String, Int, Long, String)] = {
    tableIdentifier match {
      case Some(identifier) =>
        val tableUniqueName = s"${
          identifier.database.getOrElse(SparkSession.getActiveSession
            .get.catalog.currentDatabase)
        }_${ identifier.table }"
        executeJobToGetCache(List(tableUniqueName))
      case None => Seq()
    }
  }

  private val LOGGER = LogServiceFactory.getLogService(classOf[CarbonShowCacheCommand].getName)

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

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    if (tableIdentifier.isEmpty) {
      /**
       * Assemble result for database
       */
      getAllTablesCache(sparkSession)
    } else {
      /**
       * Assemble result for table
       */
      val carbonTable = CarbonEnv.getCarbonTable(tableIdentifier.get)(sparkSession)
      Checker
        .validateTableExists(tableIdentifier.get.database, tableIdentifier.get.table, sparkSession)
      val numberOfIndexFiles = CacheUtil.getAllIndexFiles(carbonTable).size
      val driverRawResults = getTableCacheFromDriver(sparkSession, carbonTable, numberOfIndexFiles)
      val indexRawResults = if (CarbonProperties.getInstance().isDistributedPruningEnabled
      (tableIdentifier.get.database.getOrElse(sparkSession.catalog.currentDatabase),
        tableIdentifier.get.table)) {
        getTableCacheFromIndexServer(carbonTable, numberOfIndexFiles)(sparkSession)
      } else { Seq() }
      val result = driverRawResults.slice(0, 2) ++
                   driverRawResults.drop(2).map { row =>
                     Row(row.get(0), row.getLong(1) + row.getLong(2), row.get(3))
                   }
      val serverResults = indexRawResults.slice(0, 2) ++
                          indexRawResults.drop(2).map { row =>
                            Row(row.get(0), row.getLong(1) + row.getLong(2), row.get(3))
                          }
      Seq(Row("DRIVER CACHE", "", "")) ++ result.map {
        row =>
          Row(row.get(0), bytesToDisplaySize(row.getLong(1)), row.get(2))
      } ++ (serverResults match {
        case Nil => Seq()
        case list =>
          Seq(Row("-----------", "-----------", "-----------"), Row("INDEX CACHE", "", "")) ++
          list.map {
          row => Row(row.get(0), bytesToDisplaySize(row.getLong(1)), row.get(2))
        }
      })
    }
  }

  def getAllTablesCache(sparkSession: SparkSession): Seq[Row] = {
    val currentDatabase = sparkSession.sessionState.catalog.getCurrentDatabase
    val cache = CacheProvider.getInstance().getCarbonCache
    val isDistributedPruningEnabled = CarbonProperties.getInstance()
      .isDistributedPruningEnabled("", "")
    if (cache == null && !isDistributedPruningEnabled) {
      return makeEmptyCacheRows(currentDatabase)
    }
    var carbonTables = mutable.ArrayBuffer[CarbonTable]()
    sparkSession.sessionState.catalog.listTables(currentDatabase).foreach {
      tableIdent =>
        try {
          val carbonTable = CarbonEnv.getCarbonTable(tableIdent)(sparkSession)
          if (!carbonTable.isChildDataMap) {
            carbonTables += carbonTable
          }
        } catch {
          case _: NoSuchTableException =>
            LOGGER.debug("Ignoring non-carbon table " + tableIdent.table)
        }
    }
    val indexServerRows = if (isDistributedPruningEnabled) {
      carbonTables.flatMap {
        mainTable =>
          try {
            makeRows(getTableCacheFromIndexServer(mainTable)(sparkSession), mainTable)
          } catch {
            case ex: UnsupportedOperationException => Seq()
          }
      }
    } else { Seq() }

    val driverRows = if (cache != null) {
      carbonTables.flatMap {
        carbonTable =>
          try {
            makeRows(getTableCacheFromDriver(sparkSession, carbonTable), carbonTable)
          } catch {
            case ex: UnsupportedOperationException => Seq()
          }
      }
    } else { Seq() }

    val (driverdbIndexSize, driverdbDatamapSize, driverdbDictSize) = calculateDBIndexAndDatamapSize(
      driverRows)
    val (indexAllIndexSize, indexAllDatamapSize, indexAllDictSize) = calculateDBIndexAndDatamapSize(
      indexServerRows)
    val (indexdbIndexSize, indexdbDatamapSize) = getIndexServerCacheSizeForCurrentDB(
      currentDatabase)


    val driverDisplayRows = if (cache != null) {
      val tablePaths = carbonTables.map {
        carbonTable =>
          carbonTable.getTablePath
      }
      val (driverIndexSize, driverDatamapSize, allDictSize) = getAllDriverCacheSize(tablePaths
        .toList)
      if (driverRows.nonEmpty) {
        val rows = (Seq(
          Row("ALL", "ALL", driverIndexSize, driverDatamapSize, allDictSize),
          Row(currentDatabase, "ALL", driverdbIndexSize, driverdbDatamapSize, driverdbDictSize)
        ) ++ driverRows).collect {
          case row if row.getLong(2) != 0L || row.getLong(3) != 0L || row.getLong(4) != 0L =>
            Row(row(0), row(1), bytesToDisplaySize(row.getLong(2)),
              bytesToDisplaySize(row.getLong(3)), bytesToDisplaySize(row.getLong(4)))
        }
        Seq(Row("DRIVER CACHE", "", "", "", "")) ++ rows
      } else {
        makeEmptyCacheRows(currentDatabase)
      }
    } else {
      makeEmptyCacheRows(currentDatabase)
    }

    //      val (serverIndexSize, serverDataMapSize) = getAllIndexServerCacheSize
    val indexDisplayRows = if (indexServerRows.nonEmpty) {
      val rows = (Seq(
        Row("ALL", "ALL", indexAllIndexSize, indexAllDatamapSize, indexAllDictSize),
        Row(currentDatabase, "ALL", indexdbIndexSize, indexdbDatamapSize, driverdbDictSize)
      ) ++ indexServerRows).collect {
        case row if row.getLong(2) != 0L || row.getLong(3) != 0L || row.getLong(4) != 0L =>
          Row(row.get(0), row.get(1), bytesToDisplaySize(row.getLong(2)),
            bytesToDisplaySize(row.getLong(3)), bytesToDisplaySize(row.getLong(4)))
      }
      Seq(Row("INDEX SERVER CACHE", "", "", "", "")) ++ rows
    } else {
      Seq()
    }
    driverDisplayRows ++ indexDisplayRows
  }

  def getTableCacheFromDriver(sparkSession: SparkSession, carbonTable: CarbonTable,
      numOfIndexFiles: Int = 0): Seq[Row] = {
    val cache = CacheProvider.getInstance().getCarbonCache
    if (cache != null) {
      val childTableList = getChildTableList(carbonTable)(sparkSession)
      val parentMetaCacheInfo = collectDriverMetaCacheInfo(carbonTable, false)(sparkSession) match {
        case head :: _ => head
        case Nil => ("", 0, 0L, "")
      }
      val parentDictionary = getDictionarySize(carbonTable)(sparkSession)
      val childMetaCacheInfos = childTableList.flatMap {
        childTable =>
          val dbName = childTable._1.substring(0, childTable._1.indexOf("_"))
          val tableName = childTable._1
            .substring(childTable._1.indexOf("_") + 1, childTable._1.length)
          if (childTable._2
            .equalsIgnoreCase(DataMapClassProvider.BLOOMFILTER.getShortName)) {
            val datamaps = DataMapStoreManager.getInstance().getDataMapSchemasOfTable(carbonTable)
              .asScala
            val bloomDataMaps = datamaps.collect {
              case datamap if datamap.getProviderName
                .equalsIgnoreCase(DataMapClassProvider.BLOOMFILTER.getShortName) =>
                datamap
            }.toList

            // Get datamap keys
            val datamapKeys = bloomDataMaps.flatMap {
              datamap =>
                CacheUtil
                  .getBloomCacheKeys(carbonTable, datamap)
            }

            // calculate the memory size if key exists in cache
            val datamapSize = datamapKeys.collect {
              case key if cache.get(key) != null =>
                cache.get(key).getMemorySize
            }.sum
            Seq(Row(childTable._1, 0L, datamapSize, childTable._2))
          } else {
            val childCarbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
            val childMetaCacheInfo = collectDriverMetaCacheInfo(childCarbonTable)(sparkSession)
            childMetaCacheInfo.map {
              childMeta => Row(childMeta._1, childMeta._3, 0L, childTable._2)
            }.toSeq
          }
      }
      var comments = parentMetaCacheInfo._2 + s"/$numOfIndexFiles index files cached"
      if (!carbonTable.isTransactionalTable) {
        comments += " (external table)"
      }
      Seq(
        Row("Index", parentMetaCacheInfo._3, comments),
        Row("Dictionary", parentDictionary, "")
      ) ++ childMetaCacheInfos
    } else {
      Seq(
        Row("Index", 0L, ""),
        Row("Dictionary", 0L, "")
      )
    }
  }

  override protected def opName: String = "SHOW CACHE"

  private def makeEmptyCacheRows(currentDatabase: String) = {
    Seq(
      Row("ALL", "ALL", bytesToDisplaySize(0), bytesToDisplaySize(0), bytesToDisplaySize(0)),
      Row(currentDatabase, "ALL", bytesToDisplaySize(0), bytesToDisplaySize(0),
        bytesToDisplaySize(0)))
  }

  private def calculateDBIndexAndDatamapSize(rows: Seq[Row]): (Long, Long, Long) = {
    rows.map {
      row =>
        (row(2).asInstanceOf[Long], row(3).asInstanceOf[Long], row.get(4).asInstanceOf[Long])
    }.fold((0L, 0L, 0L)) {
      case (a, b) =>
        (a._1 + b._1, a._2 + b._2, a._3 + b._3)
    }
  }

  private def makeRows(tableResult: Seq[Row], carbonTable: CarbonTable) = {
    var (indexSize, datamapSize) = (tableResult(0).getLong(1), 0L)
    tableResult.drop(2).foreach {
      row =>
        indexSize += row.getLong(1)
        datamapSize += row.getLong(2)
    }
    val dictSize = tableResult(1).getLong(1)
    Seq(Row(carbonTable.getDatabaseName, carbonTable.getTableName,
      indexSize,
      datamapSize,
      dictSize))
  }

  private def getTableCacheFromIndexServer(mainTable: CarbonTable, numberOfIndexFiles: Int = 0)
    (sparkSession: SparkSession): Seq[Row] = {
    val childTables = getChildTableList(mainTable)(sparkSession)
    val cache = executeJobToGetCache(childTables.map(_._1) ++ List(mainTable.getTableUniqueName))
    val (mainTableFiles, mainTableCache) = getTableCache(cache, mainTable.getTableUniqueName)
    val childMetaCacheInfos = childTables.flatMap {
      childTable =>
        val tableName = childTable._1
        if (childTable._2
          .equalsIgnoreCase(DataMapClassProvider.BLOOMFILTER.getShortName) || childTable._2
          .equalsIgnoreCase(DataMapClassProvider.MV.getShortName)) {
          Seq(Row(tableName, 0L, getTableCache(cache, tableName)._2, childTable._2))
        } else {
          val childCache = getTableCache(cache, childTable._1)._2
          Seq(Row(tableName, childCache, 0L, childTable._2))
        }
    }
    var comments = mainTableFiles + s"/$numberOfIndexFiles index files cached"
    if (!mainTable.isTransactionalTable) {
      comments += " (external table)"
    }
    Seq(
      Row("Index", mainTableCache, comments),
      Row("Dictionary", getDictionarySize(mainTable)(sparkSession), "")
    ) ++ childMetaCacheInfos

  }

  private def executeJobToGetCache(tableUniqueNames: List[String]): Seq[(String, Int, Long,
    String)] = {
    val (result, time) = CarbonScalaUtil.logTime {
      IndexServer.getClient.showCache(tableUniqueNames.mkString(",")).map(_.split(":"))
        .groupBy(_.head).map { t =>
        var sum = 0L
        var length = 0
        var provider = ""
        t._2.foreach {
          arr =>
            sum += arr(2).toLong
            length += arr(1).toInt
            provider = arr(3)
        }
        (t._1, length, sum, provider)
      }
    }
    LOGGER.info(s"Time taken to get cache results from Index Server is $time ms")
    result.toList
  }

  private def getTableCache(cache: Seq[(String, Int, Long, String)], tableName: String) = {
    val (_, indexFileLength, cacheSize, _) = cache.find(_._1 == tableName)
      .getOrElse(("", 0, 0L, ""))
    (indexFileLength, cacheSize)
  }

  private def getChildTableList(carbonTable: CarbonTable)
    (sparkSession: SparkSession): List[(String, String)] = {
    val showTableCacheEvent = ShowTableCacheEvent(carbonTable, sparkSession, internalCall)
    val operationContext = new OperationContext
    // datamapName -> (datamapProviderName, indexSize, datamapSize)
    operationContext.setProperty(carbonTable.getTableUniqueName, List())
    OperationListenerBus.getInstance.fireEvent(showTableCacheEvent, operationContext)
    operationContext.getProperty(carbonTable.getTableUniqueName)
      .asInstanceOf[List[(String, String)]]
  }

  private def getDictionarySize(carbonTable: CarbonTable)(sparkSession: SparkSession): Long = {
    val dictKeys = CacheUtil.getAllDictCacheKeys(carbonTable)
    val cache = CacheProvider.getInstance().getCarbonCache
    dictKeys.collect {
      case dictKey if cache != null && cache.get(dictKey) != null =>
        cache.get(dictKey).getMemorySize
    }.sum
  }

  private def getAllDriverCacheSize(tablePaths: List[String]) = {
    val cache = CacheProvider.getInstance().getCarbonCache
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
    (allIndexSize, allDatamapSize, allDictSize)
  }

  private def collectDriverMetaCacheInfo(carbonTable: CarbonTable, collectChild: Boolean = true)
    (sparkSession: SparkSession): List[(String, Int, Long, String)] = {
    if (CacheProvider.getInstance().getCarbonCache != null) {
      val tableCacheInfo = collectDriverMetaCacheInfo(carbonTable.getTableUniqueName,
        collectChild: Boolean)
      tableCacheInfo
    } else {
      List()
    }
  }

  private def collectDriverMetaCacheInfo(tableName: String,
      collectChild: Boolean): List[(String, Int, Long, String)] = {
    val dataMaps = DataMapStoreManager.getInstance().getAllDataMaps.asScala
    dataMaps.collect {
      case (table, tableDataMaps) if table.isEmpty ||
                                     (tableName.nonEmpty && tableName.equalsIgnoreCase(table)) =>
        val filteredDataMaps = tableDataMaps.asScala.filter(tableDataMap =>
          collectChild || tableDataMap.getDataMapFactory.isInstanceOf[BlockletDataMapFactory])
        val sizeAndIndexLengths = filteredDataMaps
          .map { dataMap =>
            if (!dataMap.getDataMapSchema.getProviderName
              .equals(DataMapClassProvider.BLOOMFILTER.getShortName)) {
              s"$table:${ dataMap.getDataMapFactory.getCacheSize }:${
                DataMapClassProvider.BLOOMFILTER.getShortName
              }"
            } else {
              s"${ dataMap.getDataMapSchema.getDataMapName }:${
                dataMap.getDataMapFactory.getCacheSize
              }:${ dataMap.getDataMapSchema.getProviderName }"
            }
          }
        sizeAndIndexLengths.map {
          sizeAndLength =>
            val array = sizeAndLength.split(":")
            (array(0), array(1).toInt, array(2).toLong, array(3))
        }
    }.flatten.toList
  }

  private def getIndexServerCacheSizeForCurrentDB(currentDB: String): (Long, Long) = {
    var (allIndexSize, allDatamapSize) = (0L, 0L)
    cacheResult.foreach {
      case (tableUniqueName, _, sum, provider) if tableUniqueName.split("_")(0)
        .equalsIgnoreCase(currentDB) =>
        provider match {
          case "Bloom" =>
            allIndexSize += sum
          case _ =>
            allDatamapSize += sum
        }
    }
    (allIndexSize, allDatamapSize)
  }

}

