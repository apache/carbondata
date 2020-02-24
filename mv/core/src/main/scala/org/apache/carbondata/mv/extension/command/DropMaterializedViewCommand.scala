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

package org.apache.carbondata.mv.extension.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.AtomicRunnableCommand

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.{DataMapProvider, DataMapStoreManager}
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.datamap.DataMapManager
import org.apache.carbondata.events._

/**
 * Drop Materialized View Command implementation
 * It will drop the MV table, and unregister the schema in [[DataMapStoreManager]]
 */
case class DropMaterializedViewCommand(
    mvName: String,
    ifExistsSet: Boolean,
    forceDrop: Boolean = false)
  extends AtomicRunnableCommand {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  private var provider: DataMapProvider = _
  var mainTable: CarbonTable = _
  var mvSchema: DataMapSchema = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    setAuditInfo(Map("mvName" -> mvName))
    try {
      dropSchema(sparkSession)
    } catch {
      case e: Exception =>
        if (!ifExistsSet) {
          throw e
        }
    }
    Seq.empty
  }

  // drop MV schema in underlying store
  private def dropSchema(sparkSession: SparkSession): Unit = {
    LOGGER.info("Trying to drop materialized view schema")
    try {
      mvSchema = DataMapStoreManager.getInstance().getDataMapSchema(mvName)
      if (mvSchema != null) {
        val operationContext = new OperationContext()
        val storeLocation = CarbonProperties.getInstance().getSystemFolderLocation
        val preExecEvent = UpdateDataMapPreExecutionEvent(sparkSession, storeLocation, null)
        OperationListenerBus.getInstance().fireEvent(preExecEvent, operationContext)

        DataMapStatusManager.dropDataMap(mvName)

        val postExecEvent = UpdateDataMapPostExecutionEvent(sparkSession, storeLocation, null)
        OperationListenerBus.getInstance().fireEvent(postExecEvent, operationContext)

        provider = DataMapManager.get.getDataMapProvider(null, mvSchema, sparkSession)
        provider.cleanMeta()
      }
    } catch {
      case e: Exception =>
        if (!ifExistsSet) {
          throw e
        }
    }
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    // delete the table folder
    if (provider != null) {
      provider.cleanData()
    }
    Seq.empty
  }

  override protected def opName: String = "DROP MATERIALIZED VIEW"
}
