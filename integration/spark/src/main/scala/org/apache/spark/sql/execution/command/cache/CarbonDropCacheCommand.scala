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

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.index.{IndexStoreManager, IndexUtil}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events.{DropTableCacheEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.view.MVManagerInSpark

case class CarbonDropCacheCommand(tableIdentifier: TableIdentifier, internalCall: Boolean = false)
  extends MetadataCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val carbonTable = CarbonEnv.getCarbonTable(tableIdentifier)(sparkSession)
    clearCache(carbonTable, sparkSession)
    Seq.empty
  }

  def clearCache(carbonTable: CarbonTable, sparkSession: SparkSession): Unit = {
    LOGGER.info("Drop cache request received for table " + carbonTable.getTableUniqueName)

    val dropCacheEvent = DropTableCacheEvent(carbonTable, sparkSession, internalCall)
    val operationContext = new OperationContext
    OperationListenerBus.getInstance.fireEvent(dropCacheEvent, operationContext)

    val cache = CacheProvider.getInstance().getCarbonCache
    // Clea cache from IndexServer
    if (CarbonProperties.getInstance().isDistributedPruningEnabled(carbonTable.getDatabaseName,
      carbonTable.getTableName)) {
      LOGGER.info("Clearing cache from IndexServer")
      IndexUtil.executeClearIndexJob(carbonTable, IndexUtil.DISTRIBUTED_JOB_NAME)
    }
    if (cache != null) {
      LOGGER.info("Clearing cache from driver side")
      IndexStoreManager.getInstance().clearIndex(carbonTable.getAbsoluteTableIdentifier)
    }
    val viewManager = MVManagerInSpark.get(sparkSession)
    val viewsOnTable = viewManager.getSchemasOnTable(carbonTable)
    if (!viewsOnTable.isEmpty) {
      viewsOnTable.asScala.foreach {
        viewSchema =>
          val viewIdentifier = new TableIdentifier(
            viewSchema.getIdentifier.getTableName,
            Option(viewSchema.getIdentifier.getDatabaseName)
          )
          CarbonDropCacheCommand(viewIdentifier, internalCall = true).run(sparkSession)
      }
    }
    LOGGER.info("Drop cache request served for table " + carbonTable.getTableUniqueName)
  }

  override protected def opName: String = "DROP METACACHE"
}
