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

package org.apache.spark.sql.hive.execution.command

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.table.CarbonDropTableCommand

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonCommonConstantsInternal, CarbonLoadOptionConstants}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil, SessionParams}

case class CarbonDropDatabaseCommand(command: DropDatabaseCommand)
  extends RunnableCommand {

  override val output: Seq[Attribute] = command.output

  override def run(sparkSession: SparkSession): Seq[Row] = {
    var rows: Seq[Row] = Seq()
    val dbName = command.databaseName
    var tablesInDB: Seq[TableIdentifier] = null
    if (sparkSession.sessionState.catalog.listDatabases().exists(_.equalsIgnoreCase(dbName))) {
      tablesInDB = sparkSession.sessionState.catalog.listTables(dbName)
    }
    var databaseLocation = ""
    try {
      databaseLocation = CarbonEnv.getDatabaseLocation(dbName, sparkSession)
    } catch {
      case e: NoSuchDatabaseException =>
        // if database not found and ifExists true return empty
        if (command.ifExists) {
          return rows
        }
    }
    // DropHiveDB command will fail if cascade is false and one or more table exists in database
    if (command.cascade && tablesInDB != null) {
      tablesInDB.foreach { tableName =>
        CarbonDropTableCommand(true, tableName.database, tableName.table).run(sparkSession)
      }
    }
    rows = command.run(sparkSession)
    CarbonUtil.dropDatabaseDirectory(databaseLocation)
    rows
  }
}

case class CarbonSetCommand(command: SetCommand)
  extends MetadataCommand {

  override val output: Seq[Attribute] = command.output

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val sessionParams = CarbonEnv.getInstance(sparkSession).carbonSessionInfo.getSessionParams
    command.kv match {
      case Some((key, Some(value))) =>
        CarbonSetCommand.validateAndSetValue(sessionParams, key, value)
      case _ =>
    }
    command.run(sparkSession)
  }

  override protected def opName: String = "SET"

}

object CarbonSetCommand {
  def validateAndSetValue(sessionParams: SessionParams, key: String, value: String): Unit = {
    val isCarbonProperty: Boolean = CarbonProperties.getInstance().isCarbonProperty(key)
    if (key.startsWith(CarbonCommonConstants.CARBON_INPUT_SEGMENTS)) {
      if (key.split("\\.").length == 5) {
        sessionParams.addProperty(key.toLowerCase(), value)
      }
      else {
        throw new MalformedCarbonCommandException(
          "property should be in \" carbon.input.segments.<database_name>" +
          ".<table_name>=<seg_id list> \" format.")
      }
    } else if (key.startsWith(CarbonCommonConstants.VALIDATE_CARBON_INPUT_SEGMENTS)) {
      sessionParams.addProperty(key.toLowerCase(), value)
    } else if (key.startsWith(CarbonCommonConstantsInternal.QUERY_ON_PRE_AGG_STREAMING)) {
      sessionParams.addProperty(key.toLowerCase(), value)
    } else if (key.startsWith(CarbonCommonConstants.CARBON_DATAMAP_VISIBLE)) {
      if (key.split("\\.").length == 6) {
        sessionParams.addProperty(key.toLowerCase, value)
      } else {
        throw new MalformedCarbonCommandException("property should be in " +
          "\" carbon.datamap.visible.<database_name>.<table_name>.<datamap_name>" +
          " = <true/false> \" format")
      }
    } else if (key.startsWith(CarbonCommonConstants.CARBON_LOAD_DATAMAPS_PARALLEL)) {
      if (key.split("\\.").length == 6) {
        sessionParams.addProperty(key.toLowerCase(), value)
      }
      else {
        throw new MalformedCarbonCommandException(
          "property should be in \" carbon.load.datamaps.parallel.<database_name>" +
          ".<table_name>=<true/false> \" format.")
      }
    } else if (key.startsWith(CarbonLoadOptionConstants.CARBON_TABLE_LOAD_SORT_SCOPE)) {
      if (key.split("\\.").length == 7) {
        sessionParams.addProperty(key.toLowerCase(), value)
      }
      else {
        throw new MalformedCarbonCommandException(
          "property should be in \" carbon.table.load.sort.scope.<database_name>" +
          ".<table_name>=<sort_sope> \" format.")
      }
    } else if (key.startsWith(CarbonCommonConstants.CARBON_ENABLE_INDEX_SERVER)) {
      val keySplits = key.split("\\.")
      if (keySplits.length == 6 || keySplits.length == 4) {
        sessionParams.addProperty(key.toString, value)
      }
    }
    else if (isCarbonProperty) {
      sessionParams.addProperty(key, value)
    }
  }

  def unsetValue(sessionParams: SessionParams, key: String): Unit = {
    sessionParams.removeProperty(key)
  }
}

case class CarbonResetCommand()
  extends RunnableCommand {
  override val output = ResetCommand.output

  override def run(sparkSession: SparkSession): Seq[Row] = {
    CarbonEnv.getInstance(sparkSession).carbonSessionInfo.getSessionParams.clear()
    ResetCommand.run(sparkSession)
  }
}
