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

package org.apache.spark.sql.execution.command.view

import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.AtomicRunnableCommand
import org.apache.spark.sql.execution.command.table.CarbonDropTableCommand

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.events.{OperationContext, OperationListenerBus}
import org.apache.carbondata.view.{MaterializedViewCatalogInSpark, MaterializedViewManagerInSpark, UpdateMaterializedViewPostExecutionEvent, UpdateMaterializedViewPreExecutionEvent}

/**
 * Drop Materialized View Command implementation
 * It will drop the MV table, and unregister the schema in [[MaterializedViewManagerInSpark]]
 */
case class CarbonDropMaterializedViewCommand(
    databaseNameOption: Option[String],
    name: String,
    ifExistsSet: Boolean,
    forceDrop: Boolean = false)
  extends AtomicRunnableCommand {

  private val logger = CarbonDropMaterializedViewCommand.LOGGER

  private var dropTableCommand: CarbonDropTableCommand = _

  override def processMetadata(session: SparkSession): Seq[Row] = {
    setAuditInfo(Map("mvName" -> name))
    val viewManager = MaterializedViewManagerInSpark.get(session)
    try {
      logger.info("Trying to drop materialized view schema")
      val databaseName =
        databaseNameOption.getOrElse(session.sessionState.catalog.getCurrentDatabase)
      val schema = viewManager.getSchema(databaseName, name)
      if (schema != null) {
        // Drop mv status.
        val identifier = TableIdentifier(name, Option(databaseName))
        val operationContext = new OperationContext()
        OperationListenerBus.getInstance().fireEvent(
          UpdateMaterializedViewPreExecutionEvent(session, identifier),
          operationContext)
        viewManager.onDrop(databaseName, name)
        OperationListenerBus.getInstance().fireEvent(
          UpdateMaterializedViewPostExecutionEvent(session, identifier),
          operationContext)

        // Drop mv table.
        val dropTableCommand = CarbonDropTableCommand(
          ifExistsSet = true,
          Option(databaseName),
          name,
          dropChildTable = true,
          isInternalCall = true)
        dropTableCommand.processMetadata(session)

        // Drop mv schema.
        try {
          viewManager.deleteSchema(databaseName, name)
        } finally {
          val viewCatalog = viewManager.getCatalog()
            .asInstanceOf[MaterializedViewCatalogInSpark]
          if (viewCatalog != null) {
            viewCatalog.deregisterSchema(schema.getIdentifier)
          }
        }

        this.dropTableCommand = dropTableCommand
      }
    } catch {
      case exception: Exception =>
        if (!ifExistsSet) {
          throw exception
        }
    }
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    // delete the table folder
    if (this.dropTableCommand != null) {
      this.dropTableCommand.processData(sparkSession)
    }
    Seq.empty
  }

  override protected def opName: String = "DROP MATERIALIZED VIEW"
}

object CarbonDropMaterializedViewCommand {

  private val LOGGER: Logger = LogServiceFactory.getLogService(
    classOf[CarbonDropMaterializedViewCommand].getCanonicalName)

}
