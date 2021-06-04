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

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.DataCommand

import org.apache.carbondata.common.exceptions.sql.MalformedMVCommandException
import org.apache.carbondata.core.view.MVStatus
import org.apache.carbondata.events.withEvents
import org.apache.carbondata.view.{MVHelper, MVManagerInSpark, MVRefresher, RefreshMVPostExecutionEvent, RefreshMVPreExecutionEvent}

/**
 * Refresh Materialized View Command implementation
 * This command refresh the MV table incrementally and make it synchronized with the main
 * table. After sync, MV state is changed to enabled.
 */
case class CarbonRefreshMVCommand(
    databaseNameOption: Option[String],
    mvName: String) extends DataCommand {

  override def processData(session: SparkSession): Seq[Row] = {
    val databaseName =
      databaseNameOption.getOrElse(session.sessionState.catalog.getCurrentDatabase)
    val viewManager = MVManagerInSpark.get(session)
    var schema = viewManager.getSchema(databaseName, mvName)
    if (schema == null) {
      // schema can be null when MV is registered i.e. in case of compatibility scenarios
      // with old store. So check and get schema if exists in the system folder.
      schema = viewManager.getSchema(databaseName, mvName, true)
      if (schema == null) {
        throw new MalformedMVCommandException(
          s"Materialized view $databaseName.$mvName does not exist")
      }
      val viewCatalog = MVManagerInSpark.getOrReloadMVCatalog(session)
      if (!viewCatalog.getAllSchemas.exists(_.viewSchema.getIdentifier.getTableName
          .equals(schema.getIdentifier.getTableName))) {
        try {
          viewCatalog.registerSchema(schema)
        } catch {
          case e: Exception =>
            throw new Exception(
              "Error while registering schema for mv: " + schema.getIdentifier.getTableName, e)
        }
      }
    }

    // refresh table property of parent table if needed
    MVHelper.addOrModifyMVTablesMap(session, schema, isRefreshMV = true)

    val table = CarbonEnv.getCarbonTable(Option(databaseName), mvName)(session)
    setAuditTable(table)

    MVRefresher.refresh(schema, session)

    // After rebuild successfully enable the MV table.
    val identifier = TableIdentifier(mvName, Option(databaseName))
    withEvents(RefreshMVPreExecutionEvent(session, identifier),
      RefreshMVPostExecutionEvent(session, identifier)) {
      viewManager.setStatus(schema.getIdentifier, MVStatus.ENABLED)
    }
    Seq.empty
  }

  override protected def opName: String = "REFRESH MATERIALIZED VIEW"
}
