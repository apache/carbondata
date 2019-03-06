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

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.MetadataCommand

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.cache.dictionary.AbstractColumnDictionaryInfo
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexWrapper
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.datamap.bloom.BloomCacheKeyValue
import org.apache.carbondata.events.{DropCacheEvent, OperationContext, OperationListenerBus}

case class CarbonDropCacheCommand(tableIdentifier: TableIdentifier, internalCall: Boolean = false)
  extends MetadataCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val carbonTable = CarbonEnv.getCarbonTable(tableIdentifier)(sparkSession)
    clearCache(carbonTable, sparkSession)
    Seq.empty
  }

  def clearCache(carbonTable: CarbonTable, sparkSession: SparkSession): Unit = {
    LOGGER.info("Drop cache request received for table " + carbonTable.getTableName)

    val dropCacheEvent = DropCacheEvent(
      carbonTable,
      sparkSession,
      internalCall
    )
    val operationContext = new OperationContext
    OperationListenerBus.getInstance.fireEvent(dropCacheEvent, operationContext)

    val cache = CacheProvider.getInstance().getCarbonCache
    if (cache != null) {
      val tablePath = carbonTable.getTablePath + CarbonCommonConstants.FILE_SEPARATOR

      // Dictionary IDs
      val dictIds = carbonTable.getAllDimensions.asScala.filter(_.isGlobalDictionaryEncoding)
        .map(_.getColumnId).toArray

      // Remove elements from cache
      val keysToRemove = ListBuffer[String]()
      val cacheIterator = cache.getCacheMap.entrySet().iterator()
      while (cacheIterator.hasNext) {
        val entry = cacheIterator.next()
        val cache = entry.getValue

        if (cache.isInstanceOf[BlockletDataMapIndexWrapper]) {
          // index
          val indexPath = entry.getKey.replace(CarbonCommonConstants.WINDOWS_FILE_SEPARATOR,
            CarbonCommonConstants.FILE_SEPARATOR)
          if (indexPath.startsWith(tablePath)) {
            keysToRemove += entry.getKey
          }
        } else if (cache.isInstanceOf[BloomCacheKeyValue.CacheValue]) {
          // bloom datamap
          val shardPath = entry.getKey.replace(CarbonCommonConstants.WINDOWS_FILE_SEPARATOR,
            CarbonCommonConstants.FILE_SEPARATOR)
          if (shardPath.contains(tablePath)) {
            keysToRemove += entry.getKey
          }
        } else if (cache.isInstanceOf[AbstractColumnDictionaryInfo]) {
          // dictionary
          val dictId = dictIds.find(id => entry.getKey.startsWith(id))
          if (dictId.isDefined) {
            keysToRemove += entry.getKey
          }
        }
      }
      cache.removeAll(keysToRemove.asJava)
    }

    LOGGER.info("Drop cache request received for table " + carbonTable.getTableName)
  }

  override protected def opName: String = "DROP CACHE"

}
