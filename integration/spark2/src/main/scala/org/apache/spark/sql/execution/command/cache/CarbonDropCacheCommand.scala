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

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.MetadataCommand

import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.cache.dictionary.AbstractColumnDictionaryInfo
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexWrapper
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.datamap.bloom.BloomCacheKeyValue

case class CarbonDropCacheCommand(tableIdentifier: TableIdentifier) extends MetadataCommand {

  def clearCache(sparkSession: SparkSession, carbonTable: CarbonTable): Unit = {
    val tableName = carbonTable.getTableName
    val databaseName = carbonTable.getDatabaseName
    val cache = CacheProvider.getInstance().getCarbonCache
    if (cache != null) {
      val dbLocation = CarbonEnv
        .getDatabaseLocation(tableIdentifier.database
          .getOrElse(sparkSession.catalog.currentDatabase), sparkSession)
        .replace(CarbonCommonConstants.WINDOWS_FILE_SEPARATOR,
          CarbonCommonConstants.FILE_SEPARATOR)

      // If table has children tables as well, add them to tablePathsBuffer
      val tablePathsBuffer = scala.collection.mutable.ArrayBuffer.empty[String]
      tablePathsBuffer += dbLocation + CarbonCommonConstants.FILE_SEPARATOR +
                          tableName + CarbonCommonConstants.FILE_SEPARATOR
      if (carbonTable.hasDataMapSchema) {
        val childrenSchemas = carbonTable.getTableInfo.getDataMapSchemaList.asScala
          .filter(_.getRelationIdentifier != null)
        for (childSchema <- childrenSchemas) {
          tablePathsBuffer += dbLocation + CarbonCommonConstants.FILE_SEPARATOR +
                              childSchema.getRelationIdentifier.getTableName +
                              CarbonCommonConstants.FILE_SEPARATOR
        }
      }
      val tablePaths = tablePathsBuffer.toArray

      // Dictionary IDs
      val dictIds = carbonTable.getAllDimensions.asScala.filter(_.isGlobalDictionaryEncoding)
        .map(_.getColumnId).toArray

      // Remove elements from cache
      val cacheIterator = cache.getCacheMap.entrySet().iterator()
      while (cacheIterator.hasNext) {
        val entry = cacheIterator.next()
        val cache = entry.getValue

        if (cache.isInstanceOf[BlockletDataMapIndexWrapper]) {
          // index
          val indexPath = entry.getKey.replace(CarbonCommonConstants.WINDOWS_FILE_SEPARATOR,
            CarbonCommonConstants.FILE_SEPARATOR)
          val validTablePath = tablePaths.find(tablePath => indexPath.startsWith(tablePath))
          if (validTablePath.isDefined) {
            cache.invalidate()
            cacheIterator.remove()
          }
        } else if (cache.isInstanceOf[BloomCacheKeyValue.CacheValue]) {
          // bloom datamap
          val shardPath = entry.getKey.replace(CarbonCommonConstants.WINDOWS_FILE_SEPARATOR,
            CarbonCommonConstants.FILE_SEPARATOR)
          val validTablePath = tablePaths.find(tablePath => shardPath.contains(tablePath))
          if (validTablePath.isDefined) {
            cache.invalidate()
            cacheIterator.remove()
          }
        } else if (cache.isInstanceOf[AbstractColumnDictionaryInfo]) {
          // dictionary
          val dictId = dictIds.find(id => entry.getKey.startsWith(id))
          if (dictId.isDefined) {
            cache.invalidate()
            cacheIterator.remove()
          }
        }
      }
    }
  }

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val carbonTable = CarbonEnv.getCarbonTable(tableIdentifier)(sparkSession)
    if (carbonTable.isChildDataMap) {
      throw new UnsupportedOperationException("Operation not allowed on child table.")
    }

    clearCache(sparkSession, carbonTable)
    Seq.empty
  }

  override protected def opName: String = "DROP CACHE"

}
