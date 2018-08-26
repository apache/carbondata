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

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{GlobalLimit, LogicalPlan}
import org.apache.spark.sql.execution.command.{AtomicRunnableCommand, DataCommand}
import org.apache.spark.storage.StorageLevel

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, ThreadLocalSessionInfo}

case class CarbonInsertIntoCommand(
    relation: CarbonDatasourceHadoopRelation,
    child: LogicalPlan,
    overwrite: Boolean,
    partition: Map[String, Option[String]])
  extends AtomicRunnableCommand {

  var loadCommand: CarbonLoadDataCommand = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getName)
    def containsLimit(plan: LogicalPlan): Boolean = {
      plan find {
        case limit: GlobalLimit => true
        case other => false
      } isDefined
    }

    ThreadLocalSessionInfo
      .setConfigurationToCurrentThread(sparkSession.sessionState.newHadoopConf())
    val isPersistEnabledUserValue = CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.CARBON_INSERT_PERSIST_ENABLED,
        CarbonCommonConstants.CARBON_INSERT_PERSIST_ENABLED_DEFAULT)
    val isPersistRequired =
      isPersistEnabledUserValue.equalsIgnoreCase("true") || containsLimit(child)
    val df =
      if (isPersistRequired) {
        LOGGER.info("Persist enabled for Insert operation")
        Dataset.ofRows(sparkSession, child).persist(
          StorageLevel.fromString(
            CarbonProperties.getInstance.getInsertIntoDatasetStorageLevel))
      } else {
        Dataset.ofRows(sparkSession, child)
      }
    val header = relation.tableSchema.get.fields.map(_.name).mkString(",")
    loadCommand = CarbonLoadDataCommand(
      databaseNameOp = Some(relation.carbonRelation.databaseName),
      tableName = relation.carbonRelation.tableName,
      factPathFromUser = null,
      dimFilesPath = Seq(),
      options = scala.collection.immutable.Map("fileheader" -> header),
      isOverwriteTable = overwrite,
      inputSqlString = null,
      dataFrame = Some(df),
      updateModel = None,
      tableInfoOp = None,
      internalOptions = Map.empty,
      partition = partition)
    val load = loadCommand.processMetadata(sparkSession)
    if (isPersistRequired) {
      df.unpersist()
    }
    load
  }
  override def processData(sparkSession: SparkSession): Seq[Row] = {
    if (null != loadCommand) {
      loadCommand.processData(sparkSession)
    } else {
      Seq.empty
    }
  }
}
