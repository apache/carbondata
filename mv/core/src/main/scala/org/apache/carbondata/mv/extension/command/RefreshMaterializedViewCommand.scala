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

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.DataCommand

import org.apache.carbondata.common.exceptions.sql.MalformedMaterializedViewException
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.datamap.DataMapManager
import org.apache.carbondata.events.{UpdateDataMapPostExecutionEvent, _}

/**
 * Refresh Materialized View Command implementation
 * This command refresh the MV table incrementally and make it synchronized with the main
 * table. After sync, MV state is changed to enabled.
 */
case class RefreshMaterializedViewCommand(
    mvName: String) extends DataCommand {

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    import scala.collection.JavaConverters._
    val schemas = DataMapStoreManager.getInstance().getAllDataMapSchemas
    val schemaOption = schemas.asScala.find(p => p.getDataMapName.equalsIgnoreCase(mvName))
    if (schemaOption.isEmpty) {
        throw new MalformedMaterializedViewException(s"Materialized view $mvName does not exist")
    }
    val mvSchema = schemaOption.get
    val mvTable = CarbonEnv.getCarbonTable(
      Option(mvSchema.getRelationIdentifier.getDatabaseName),
      mvSchema.getRelationIdentifier.getTableName
    )(sparkSession)

    setAuditTable(mvTable)

    val provider = DataMapManager.get().getDataMapProvider(mvTable, mvSchema, sparkSession)
    provider.rebuild()

    // After rebuild successfully enable the MV table.
    val operationContext = new OperationContext()
    val storeLocation = CarbonProperties.getInstance().getSystemFolderLocation
    val preExecEvent = UpdateDataMapPreExecutionEvent(sparkSession, storeLocation,
      new TableIdentifier(mvTable.getTableName, Some(mvTable.getDatabaseName)))
    OperationListenerBus.getInstance().fireEvent(preExecEvent, operationContext)
    DataMapStatusManager.enableDataMap(mvName)
    val postExecEvent = UpdateDataMapPostExecutionEvent(sparkSession, storeLocation,
      new TableIdentifier(mvTable.getTableName, Some(mvTable.getDatabaseName)))
    OperationListenerBus.getInstance().fireEvent(postExecEvent, operationContext)
    Seq.empty
  }

  override protected def opName: String = "REFRESH MATERIALIZED VIEW"
}
