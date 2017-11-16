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

package org.apache.spark.sql.execution.command.management

import org.apache.spark.sql.{CarbonEnv, GetDB, Row, SparkSession}
import org.apache.spark.sql.execution.command.{Checker, DataProcessCommand, RunnableCommand}
import org.apache.spark.sql.hive.CarbonRelation

import org.apache.carbondata.api.CarbonStore
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events.{CleanFilesPostEvent, CleanFilesPreEvent, OperationContext, OperationListenerBus}

case class CleanFilesCommand(
    databaseNameOp: Option[String],
    tableName: String, forceTableClean: Boolean = false)
  extends RunnableCommand with DataProcessCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    processData(sparkSession)
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    if (forceTableClean) {
      val dbName = GetDB.getDatabaseName(databaseNameOp, sparkSession)
      val databaseLocation = GetDB.getDatabaseLocation(dbName, sparkSession,
        CarbonProperties.getStorePath)
      // TODO: TAABLEPATH
      CarbonStore.cleanFiles(
        dbName,
        tableName,
        databaseLocation,
        null,
        forceTableClean)
    } else {
      val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
      val cleanFilesPreEvent: CleanFilesPreEvent =
        CleanFilesPreEvent(carbonTable,
          sparkSession)
      OperationListenerBus.getInstance.fireEvent(cleanFilesPreEvent)

      CarbonStore.cleanFiles(
        GetDB.getDatabaseName(databaseNameOp, sparkSession),
        tableName,
        CarbonProperties.getStorePath,
        carbonTable,
        forceTableClean)

      val cleanFilesPostEvent: CleanFilesPostEvent =
        CleanFilesPostEvent(carbonTable,
          sparkSession)
      OperationListenerBus.getInstance.fireEvent(cleanFilesPreEvent)
    }
    Seq.empty
  }
}
