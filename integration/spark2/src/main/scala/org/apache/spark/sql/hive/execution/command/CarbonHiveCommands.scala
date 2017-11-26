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

import org.apache.spark.sql.{CarbonEnv, GetDB, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.command._

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil, SessionParams}
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException

case class CarbonDropDatabaseCommand(command: DropDatabaseCommand)
  extends RunnableCommand {

  override val output: Seq[Attribute] = command.output

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val dbName = command.databaseName
    var tablesInDB: Seq[TableIdentifier] = null
    if (sparkSession.sessionState.catalog.listDatabases().exists(_.equalsIgnoreCase(dbName))) {
      tablesInDB = sparkSession.sessionState.catalog.listTables(dbName)
    }
    var databaseLocation = ""
    try {
      databaseLocation = GetDB.getDatabaseLocation(dbName, sparkSession,
        CarbonProperties.getStorePath)
    } catch {
      case e: NoSuchDatabaseException =>
        // ignore the exception as exception will be handled by hive command.run
      databaseLocation = CarbonProperties.getStorePath
    }
    // DropHiveDB command will fail if cascade is false and one or more table exists in database
    if (command.cascade && tablesInDB != null) {
      tablesInDB.foreach { tableName =>
        CarbonDropTableCommand(true, tableName.database, tableName.table).run(sparkSession)
      }
    }
    CarbonUtil.dropDatabaseDirectory(dbName.toLowerCase, databaseLocation)
    val rows = command.run(sparkSession)
    rows
  }
}

case class CarbonSetCommand(command: SetCommand)
  extends RunnableCommand {

  override val output: Seq[Attribute] = command.output

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sessionParms = CarbonEnv.getInstance(sparkSession).carbonSessionInfo.getSessionParams
    command.kv match {
      case Some((key, Some(value))) =>
        CarbonSetCommand.validateAndSetValue(sessionParms, key, value)
      case _ =>

    }
    command.run(sparkSession)
  }
}

object CarbonSetCommand {
  def validateAndSetValue(sessionParams: SessionParams, key: String, value: String): Unit = {

    val isCarbonProperty: Boolean = CarbonProperties.getInstance().isCarbonProperty(key)
    if (isCarbonProperty) {
      sessionParams.addProperty(key, value)
    }
    else if (key.startsWith(CarbonCommonConstants.CARBON_INPUT_SEGMENTS)) {
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
    }
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
