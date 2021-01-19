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

import org.apache.carbondata.common.exceptions.sql.MalformedMVCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events.withEvents
import org.apache.carbondata.view.{MVCatalogInSpark, MVHelper, MVManagerInSpark, UpdateMVPostExecutionEvent, UpdateMVPreExecutionEvent}

/**
 * Drop Materialized View Command implementation
 * It will drop the MV table, and unregister the schema in [[MVManagerInSpark]]
 */
case class CarbonDropMVCommand(
    databaseNameOption: Option[String],
    name: String,
    ifExistsSet: Boolean,
    forceDrop: Boolean = false,
    isLockAcquiredOnFactTable: String = null)
  extends AtomicRunnableCommand {

  private val logger = CarbonDropMVCommand.LOGGER

  private var dropTableCommand: CarbonDropTableCommand = _

  override def processMetadata(session: SparkSession): Seq[Row] = {
    setAuditInfo(Map("mvName" -> name))
    val viewManager = MVManagerInSpark.get(session)
    try {
      logger.info("Trying to drop materialized view schema")
      val databaseName =
        databaseNameOption.getOrElse(session.sessionState.catalog.getCurrentDatabase)
      val schema = viewManager.getSchema(databaseName, name)
      if (schema != null) {
        // Drop mv status.
        val databaseLocation = viewManager.getDatabaseLocation(databaseName)
        val systemDirectoryPath = CarbonProperties.getInstance()
          .getSystemFolderLocationPerDatabase(FileFactory
            .getCarbonFile(databaseLocation)
            .getCanonicalPath)
        val identifier = TableIdentifier(name, Option(databaseName))
        withEvents(UpdateMVPreExecutionEvent(session, systemDirectoryPath, identifier),
          UpdateMVPostExecutionEvent(session, systemDirectoryPath, identifier)) {
          viewManager.onDrop(databaseName, name)
        }
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
            .asInstanceOf[MVCatalogInSpark]
          if (viewCatalog != null) {
            viewCatalog.deregisterSchema(schema.getIdentifier)
          }
        }

        // Update the related mv table's property to mv fact tables
        MVHelper.addOrModifyMVTablesMap(session, schema, isMVDrop = true,
          isLockAcquiredOnFactTable = isLockAcquiredOnFactTable)

        this.dropTableCommand = dropTableCommand
      } else {
        if (!ifExistsSet) {
          throw new MalformedMVCommandException(
            s"Materialized view with name ${ databaseName }.${ name } does not exists")
        } else {
          return Seq.empty
        }
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

object CarbonDropMVCommand {

  private val LOGGER: Logger = LogServiceFactory.getLogService(
    classOf[CarbonDropMVCommand].getCanonicalName)

}
