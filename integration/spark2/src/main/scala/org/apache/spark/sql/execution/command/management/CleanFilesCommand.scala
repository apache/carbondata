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
import org.apache.carbondata.events.ListenerBus
import org.apache.carbondata.events.CleanFilesPostEvent

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
      CarbonStore.cleanFiles(
        GetDB.getDatabaseName(databaseNameOp, sparkSession),
        tableName,
        CarbonEnv.getInstance(sparkSession).storePath,
        null,
        forceTableClean)
    } else {
      val catalog = CarbonEnv.getInstance(sparkSession).carbonMetastore
      val relation = catalog
        .lookupRelation(databaseNameOp, tableName)(sparkSession).asInstanceOf[CarbonRelation]
      val carbonTable = relation.tableMeta.carbonTable
      CarbonStore.cleanFiles(
        GetDB.getDatabaseName(databaseNameOp, sparkSession),
        tableName,
        relation.asInstanceOf[CarbonRelation].tableMeta.storePath,
        carbonTable,
        forceTableClean)

      val cleanFilesPostEvent: CleanFilesPostEvent =
        CleanFilesPostEvent(carbonTable,
          sparkSession)
      ListenerBus.getInstance.fireEvent(cleanFilesPostEvent)
    }
    Seq.empty
  }
}
