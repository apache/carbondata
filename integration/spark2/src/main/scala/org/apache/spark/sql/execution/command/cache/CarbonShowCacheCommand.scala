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
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.command.{Checker, MetadataCommand}
import org.apache.spark.sql.types.StringType

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexWrapper
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapFactory
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events.{OperationContext, OperationListenerBus, ShowTableCacheEvent}
import org.apache.carbondata.indexserver.IndexServer
import org.apache.carbondata.spark.util.CarbonScalaUtil
import org.apache.carbondata.spark.util.CommonUtil.bytesToDisplaySize

case class CarbonShowCacheCommand(showExecutorCache: Boolean,
    tableIdentifier: Option[TableIdentifier],
    internalCall: Boolean = false)
  extends MetadataCommand {

  private lazy val cacheResult: Seq[(String, Int, Long, String)] = {
    executeJobToGetCache(List(), showExecutorCache)
  }

  private val LOGGER = LogServiceFactory.getLogService(classOf[CarbonShowCacheCommand].getName)

  override def output: Seq[AttributeReference] = {
    if (tableIdentifier.isEmpty) {
      val isDistributedPruningEnabled = CarbonProperties.getInstance()
        .isDistributedPruningEnabled("", "")
      if (showExecutorCache) {
        if (isDistributedPruningEnabled) {
          Seq(
            AttributeReference("Executor ID", StringType, nullable = false)(),
            AttributeReference("Index Size", StringType, nullable = false)())
        } else {
          Seq()
        }
      } else {
        Seq(
          AttributeReference("Identifier", StringType, nullable = false)(),
          AttributeReference("Index size", StringType, nullable = false)(),
          AttributeReference("Datamap size", StringType, nullable = false)(),
          AttributeReference("Cache Location", StringType, nullable = false)())
      }
    } else {
      Seq(
        AttributeReference("Field", StringType, nullable = false)(),
        AttributeReference("Size", StringType, nullable = false)(),
        AttributeReference("Comment", StringType, nullable = false)(),
        AttributeReference("Cache Location", StringType, nullable = false)())
    }
  }

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    if (tableIdentifier.isEmpty) {
      /**
       * Assemble result for database
       */
      if (!showExecutorCache) {
        getAllTablesCache(sparkSession)
      }
      /**
       * Assemble result for all Index Server executors
       */
      else {
        getAllExecutorCache(sparkSession)
      }
    } else {
      /**
       * Assemble result for table
       */
      val carbonTable = CarbonEnv.getCarbonTable(tableIdentifier.get)(sparkSession)
      Checker
        .validateTableExists(tableIdentifier.get.database, tableIdentifier.get.table, sparkSession)
      val numberOfIndexFiles = CacheUtil.getAllIndexFiles(carbonTable)(sparkSession).size
      val driverRawResults = getTableCacheFromDriver(sparkSession, carbonTable, numberOfIndexFiles)
      val indexRawResults = if (CarbonProperties.getInstance().isDistributedPruningEnabled
      (tableIdentifier.get.database.getOrElse(sparkSession.catalog.currentDatabase),
        tableIdentifier.get.table)) {
        getTableCacheFromIndexServer(carbonTable,
          numberOfIndexFiles)(showExecutorCache)(sparkSession)
      } else { Seq() }
      val result = driverRawResults.slice(0, 1) ++
                   driverRawResults.drop(1).map { row =>
                     Row(row.get(0), row.getLong(1) + row.getLong(2), row.get(3))
                   }
      val serverResults = indexRawResults.slice(0, 1) ++
                          indexRawResults.drop(1).map { row =>
                            Row(row.get(0), row.getLong(1) + row.getLong(2), row.get(3))
                          }
      result.map {
        row =>
          Row(row.get(0), bytesToDisplaySize(row.getLong(1)), row.get(2), "DRIVER")
      } ++ (serverResults match {
        case Nil => Seq()
        case list =>
          list.map {
          row => Row(row.get(0), bytesToDisplaySize(row.getLong(1)), row.get(2), "INDEX SERVER")
        }
      })
    }
  }

  def getAllExecutorCache(sparkSession: SparkSession): Seq[Row] = {
    val isDistributedPruningEnabled = CarbonProperties.getInstance()
      .isDistributedPruningEnabled("", "")
    if (!isDistributedPruningEnabled) {
      // Block here. this feature is only with index server enabled
      throw new UnsupportedOperationException(
        "Show Executor Metacache is only avalable with Index Server Enabled")
    } else {
      // get all the executor details from the index server
      try {
        val executorCacheValue = executeJobToGetCache(List(), showExecutorCache)
        val result = executorCacheValue.flatMap {
          iterator =>
            Seq(Row(iterator._1, bytesToDisplaySize(iterator._3)))
        }
        result
      }
      catch {
        case ex: Exception =>
          LOGGER.error("Error while getting cache from the Index Server", ex)
          Seq()
      }
    }
  }

  def getAllTablesCache(sparkSession: SparkSession): Seq[Row] = {
    val currentDatabase = sparkSession.sessionState.catalog.getCurrentDatabase
    val cache = CacheProvider.getInstance().getCarbonCache
    val isDistributedPruningEnabled = CarbonProperties.getInstance()
      .isDistributedPruningEnabled("", "")
    if (!isDistributedPruningEnabled) {
      if (cache == null || cache.getCurrentSize == 0) {
        return Seq()
      }
    }
    var carbonTables = mutable.ArrayBuffer[CarbonTable]()
    sparkSession.sessionState.catalog.listTables(currentDatabase).foreach {
      tableIdent =>
        try {
          val carbonTable = CarbonEnv.getCarbonTable(tableIdent)(sparkSession)
          if (!carbonTable.isChildTableForMV) {
            carbonTables += carbonTable
          }
        } catch {
          case ex: AnalysisException =>
            LOGGER.debug("Unable to access Carbon table object for table" + tableIdent.table)
          case _: NoSuchTableException =>
            LOGGER.debug("Ignoring non-carbon table " + tableIdent.table)
        }
    }
    val indexServerRows = if (isDistributedPruningEnabled) {
      carbonTables.flatMap {
        mainTable =>
          try {
            makeRows(getTableCacheFromIndexServer(mainTable)(showExecutorCache)(sparkSession),
              mainTable)
          } catch {
            case ex: UnsupportedOperationException => Seq()
          }
      }
    } else {
      Seq()
    }

    val driverRows = if (cache != null) {
      carbonTables.flatMap {
        carbonTable =>
          try {
            makeRows(getTableCacheFromDriver(sparkSession, carbonTable), carbonTable)
          } catch {
            case ex: UnsupportedOperationException => Seq()
          }
      }
    } else {
      Seq()
    }

    val (indexAllIndexSize, indexAllDatamapSize) = if (isDistributedPruningEnabled) {
      getIndexServerCacheSizeForCurrentDB
    } else {
      (0L, 0L)
    }
    val driverDisplayRows = if (cache != null) {
      val tablePaths = carbonTables.map {
        carbonTable =>
          carbonTable.getTablePath
      }
      val (driverIndexSize, driverDatamapSize) = getAllDriverCacheSize(tablePaths.toList)
      if (driverIndexSize + driverDatamapSize != 0 && driverRows.nonEmpty) {
        (Seq(Row("TOTAL", driverIndexSize, driverDatamapSize, "DRIVER")) ++
         driverRows).collect {
          case row if row.getLong(1) != 0L || row.getLong(2) != 0L =>
            Row(row(0), bytesToDisplaySize(row.getLong(1)),
              bytesToDisplaySize(row.getLong(2)), "DRIVER")
        }
      } else {
        Seq()
      }
    } else {
      Seq()
    }

    val indexDisplayRows = if (indexAllIndexSize + indexAllDatamapSize != 0 &&
                               indexServerRows.nonEmpty) {
      (Seq(Row("TOTAL", indexAllIndexSize, indexAllDatamapSize, "INDEX SERVER")) ++
       indexServerRows).collect {
        case row if row.getLong(1) != 0L || row.getLong(2) != 0L =>
          Row(row.get(0), bytesToDisplaySize(row.getLong(1)),
            bytesToDisplaySize(row.getLong(2)), "INDEX SERVER")
      }
    } else {
      Seq()
    }
    driverDisplayRows ++ indexDisplayRows
  }

  def getTableCacheFromDriver(sparkSession: SparkSession, carbonTable: CarbonTable,
      numOfIndexFiles: Int = 0): Seq[Row] = {
    if (CacheProvider.getInstance().getCarbonCache != null) {
      val childTableList = getChildTableList(carbonTable)(sparkSession)
      val (parentMetaCacheInfo, dataMapCacheInfo) = collectDriverMetaCacheInfo(carbonTable
        .getTableUniqueName, carbonTable.getTableId) match {
        case list =>
          val parentCache = list
            .filter(_._4.equalsIgnoreCase(BlockletDataMapFactory.DATA_MAP_SCHEMA
              .getProviderName)) match {
            case Nil => ("", 0, 0L, "")
            case head :: _ => head
          }
          val dataMapList = list
            .filter(!_._4.equalsIgnoreCase(BlockletDataMapFactory.DATA_MAP_SCHEMA
              .getProviderName))
          (parentCache, dataMapList)
        case Nil => (("", 0, 0L, ""), Nil)
      }
      val childMetaCacheInfos = childTableList.flatMap {
        childTable =>
          val tableArray = childTable._1.split("-")
          val dbName = tableArray(0)
          val tableName = tableArray(1)
          val tableId = childTable._3
          val childMetaCacheInfo = collectDriverMetaCacheInfo(s"${ dbName }_$tableName", tableId)
          childMetaCacheInfo.collect {
            case childMeta if childMeta._3 != 0 =>
              Row(childMeta._1, childMeta._3, 0L, childTable._2)
          }
      } ++ dataMapCacheInfo.collect {
        case childMeta if childMeta._3 != 0 =>
          Row(childMeta._1, childMeta._3, 0L, childMeta._4)
      }
      var comments = parentMetaCacheInfo._2 + s"/$numOfIndexFiles index files cached"
      if (!carbonTable.isTransactionalTable) {
        comments += " (external table)"
      }
      Seq(
        Row("Index", parentMetaCacheInfo._3, comments, "")
      ) ++ childMetaCacheInfos
    } else {
      Seq(
        Row("Index", 0L, "", "")
      )
    }
  }

  override protected def opName: String = "SHOW CACHE"

  private def makeRows(tableResult: Seq[Row], carbonTable: CarbonTable) = {
    var (indexSize, datamapSize) = (tableResult.head.getLong(1), 0L)
    tableResult.drop(2).foreach {
      row =>
        indexSize += row.getLong(1)
        datamapSize += row.getLong(2)
    }
    if (indexSize == 0 && datamapSize == 0) {
      Seq()
    } else {
      Seq(Row(carbonTable.getDatabaseName + "." + carbonTable.getTableName,
        indexSize,
        datamapSize))
    }
  }

  private def getTableCacheFromIndexServer(mainTable: CarbonTable,
      numberOfIndexFiles: Int = 0)(executorCache: Boolean)(sparkSession: SparkSession): Seq[Row] = {
    val childTables = getChildTableList(mainTable)(sparkSession)
    val cache = if (tableIdentifier.nonEmpty) {
      executeJobToGetCache(childTables.map(_._3) ++ List(mainTable.getTableId), executorCache)
    } else {
      cacheResult
    }
    val (mainTableFiles, mainTableCache) = getTableCache(cache, mainTable.getTableUniqueName)
    val childMetaCacheInfos = childTables.flatMap {
      childTable =>
        val tableName = childTable._1.replace("-", "_")
        if (childTable._2
          .equalsIgnoreCase(DataMapClassProvider.BLOOMFILTER.getShortName)) {
          val childCache = getTableCache(cache, tableName)._2
          if (childCache != 0) {
            Seq(Row(tableName, 0L, childCache, childTable._2))
          } else {
            Seq.empty
          }
        } else {
          val childCache = getTableCache(cache, tableName)._2
          if (childCache != 0) {
            Seq(Row(tableName, childCache, 0L, childTable._2))
          } else {
            Seq.empty
          }
        }
    }
    var comments = mainTableFiles + s"/$numberOfIndexFiles index files cached"
    if (!mainTable.isTransactionalTable) {
      comments += " (external table)"
    }
    Seq(
      Row("Index", mainTableCache, comments)
    ) ++ childMetaCacheInfos

  }

  private def executeJobToGetCache(
      tableIds: List[String],
      executorCache: Boolean
  ): Seq[(String, Int, Long, String)] = {
    try {
      val (result, time) = CarbonScalaUtil.logTime {
        IndexServer.getClient.showCache(tableIds.mkString(","), executorCache).map(_.split(":"))
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
    } catch {
      case ex: Exception =>
        LOGGER.error("Error while getting cache details from index server", ex)
        Seq()
    }
  }

  private def getTableCache(
      cache: Seq[(String, Int, Long, String)],
      tableName: String
  ): (Int, Long) = {
    val (_, indexFileLength, cacheSize, _) = cache.find(_._1 == tableName)
      .getOrElse(("", 0, 0L, ""))
    (indexFileLength, cacheSize)
  }

  private def getChildTableList(carbonTable: CarbonTable)
    (sparkSession: SparkSession): List[(String, String, String)] = {
    val showTableCacheEvent = ShowTableCacheEvent(carbonTable, sparkSession, internalCall)
    val operationContext = new OperationContext
    // datamapName -> (datamapProviderName, indexSize, datamapSize)
    operationContext.setProperty(carbonTable.getTableUniqueName, List())
    OperationListenerBus.getInstance.fireEvent(showTableCacheEvent, operationContext)
    operationContext.getProperty(carbonTable.getTableUniqueName)
      .asInstanceOf[List[(String, String, String)]]
  }

  private def getAllDriverCacheSize(tablePaths: List[String]): (Long, Long) = {
    val cache = CacheProvider.getInstance().getCarbonCache
    // Scan whole cache and fill the entries for All-Database-All-Tables
    // and Current-Database-All-Tables
    var (allIndexSize, allDatamapSize) = (0L, 0L)
    var dbIndexSize = 0L
    cache.getCacheMap.asScala.foreach {
      case (key, cacheable) =>
        cacheable match {
          case _: BlockletDataMapIndexWrapper =>
            allIndexSize += cacheable.getMemorySize
            if (tablePaths.exists { path => key.startsWith(path) }) {
              dbIndexSize += cacheable.getMemorySize
            }
          case _ =>
            allDatamapSize += cacheable.getMemorySize
        }
    }
    (allIndexSize, allDatamapSize)
  }

  private def collectDriverMetaCacheInfo(tableName: String,
      tableId: String): List[(String, Int, Long, String)] = {
    val dataMaps = DataMapStoreManager.getInstance().getAllDataMaps.asScala
    dataMaps.collect {
      case (table, tableDataMaps) if table.isEmpty ||
                                     (tableId.nonEmpty && tableId.equalsIgnoreCase(table)) =>
        val sizeAndIndexLengths = tableDataMaps.asScala
          .collect { case dataMap if
          dataMap.getDataMapSchema.getDataMapName.equalsIgnoreCase(tableName) ||
          dataMap.getDataMapFactory.getCarbonTable.getTableUniqueName.equalsIgnoreCase(tableName) =>
            if (dataMap.getDataMapFactory.isInstanceOf[BlockletDataMapFactory]) {
              s"$tableName:${ dataMap.getDataMapFactory.getCacheSize }:${
                dataMap.getDataMapSchema.getProviderName}"
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

  private def getIndexServerCacheSizeForCurrentDB: (Long, Long) = {
    var (allIndexSize, allDatamapSize) = (0L, 0L)
    val bloomFilterIdentifier = DataMapClassProvider.BLOOMFILTER.getShortName
    cacheResult.foreach {
      case (_, _, sum, provider) =>
        provider.toLowerCase match {
          case `bloomFilterIdentifier` =>
            allDatamapSize += sum
          case _ =>
            allIndexSize += sum
        }
    }
    (allIndexSize, allDatamapSize)
  }

}
