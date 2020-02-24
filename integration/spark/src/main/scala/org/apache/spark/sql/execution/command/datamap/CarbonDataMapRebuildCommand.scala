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

package org.apache.spark.sql.execution.command.datamap

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.DataCommand

import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.datamap.DataMapManager
import org.apache.carbondata.events.{UpdateDataMapPostExecutionEvent, _}

/**
 * Rebuild the datamaps through sync with main table data. After sync with parent table's it enables
 * the datamap.
 */
case class CarbonDataMapRebuildCommand(
    dataMapName: String,
    tableIdentifier: Option[TableIdentifier]) extends DataCommand {

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    import scala.collection.JavaConverters._
    val schemaOption = CarbonDataMapShowCommand(tableIdentifier).getAllDataMaps(sparkSession)
      .asScala
      .find(p => p.getDataMapName.equalsIgnoreCase(dataMapName))
    if (schemaOption.isEmpty) {
      if (tableIdentifier.isDefined) {
        throw new MalformedDataMapCommandException(
          s"Datamap with name $dataMapName does not exist on table ${tableIdentifier.get.table}")
      } else {
        throw new MalformedDataMapCommandException(
          s"Datamap with name $dataMapName does not exist on any table")
      }
    }
    val schema = schemaOption.get
    if (!schema.isLazy && schema.isIndexDataMap) {
      throw new MalformedDataMapCommandException(
        s"Non-lazy datamap $dataMapName does not support rebuild")
    }

    val table = tableIdentifier match {
      case Some(identifier) =>
        CarbonEnv.getCarbonTable(identifier)(sparkSession)
      case _ =>
        CarbonEnv.getCarbonTable(
          Option(schema.getRelationIdentifier.getDatabaseName),
          schema.getRelationIdentifier.getTableName
        )(sparkSession)
    }

    setAuditTable(table)

    val provider = DataMapManager.get().getDataMapProvider(table, schema, sparkSession)
    provider.rebuild()

    // After rebuild successfully enable the datamap.
    val operationContext: OperationContext = new OperationContext()
    val systemFolderLocation: String = CarbonProperties.getInstance().getSystemFolderLocation
    val updateDataMapPreExecutionEvent: UpdateDataMapPreExecutionEvent =
      new UpdateDataMapPreExecutionEvent(sparkSession,
        systemFolderLocation,
        new TableIdentifier(table.getTableName, Some(table.getDatabaseName)))
    OperationListenerBus.getInstance().fireEvent(updateDataMapPreExecutionEvent,
      operationContext)
    DataMapStatusManager.enableDataMap(dataMapName)
    val updateDataMapPostExecutionEvent: UpdateDataMapPostExecutionEvent =
      new UpdateDataMapPostExecutionEvent(sparkSession,
        systemFolderLocation,
        new TableIdentifier(table.getTableName, Some(table.getDatabaseName)))
    OperationListenerBus.getInstance().fireEvent(updateDataMapPostExecutionEvent,
      operationContext)
    Seq.empty
  }

  override protected def opName: String = "REBUILD DATAMAP"
}
