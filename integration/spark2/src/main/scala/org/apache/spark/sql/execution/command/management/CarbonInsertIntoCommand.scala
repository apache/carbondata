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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{GlobalLimit, LogicalPlan, Project}
import org.apache.spark.sql.execution.command.{AtomicRunnableCommand, DataCommand}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.storage.StorageLevel

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, ThreadLocalSessionInfo}
import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, NamedExpression}


case class CarbonInsertIntoCommand(
    relation: CarbonDatasourceHadoopRelation,
    child: LogicalPlan,
    overwrite: Boolean,
    partition: Map[String, Option[String]])
  extends AtomicRunnableCommand {

  var loadCommand: CarbonLoadDataCommand = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    setAuditTable(relation.carbonTable.getDatabaseName, relation.carbonTable.getTableName)
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
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
    var isCarbonToCarbonInsert : Boolean = false
    child.collect {
      case l: LogicalRelation =>
        l.relation match {
          case relation: CarbonDatasourceHadoopRelation =>
            isCarbonToCarbonInsert = true;
            relation.setInsertIntoCarbon
          case _ =>
        }
    }
    val newChild =
    if (isCarbonToCarbonInsert) {
      val columnSchemas = relation.carbonTable.getTableInfo.getFactTable.getListOfColumns.asScala
       child.transformDown {
        case p :Project =>
          // rearrange the project as per columnSchema
          val oldList = p.projectList
          if (columnSchemas.size != oldList.size) {
            // TODO: handle this scenario !
            throw new RuntimeException("TODO: yet to handle this scenario")
          }
          var newList : Seq[NamedExpression] = Seq.empty
          for (columnSchema <- columnSchemas) {
            newList = newList :+ oldList(columnSchema.getSchemaOrdinal)
          }
          Project(newList, p.child)
      }
    } else {
      child
    }

    val df =
      if (isPersistRequired) {
        LOGGER.info("Persist enabled for Insert operation")
        Dataset.ofRows(sparkSession, newChild).persist(
          StorageLevel.fromString(
            CarbonProperties.getInstance.getInsertIntoDatasetStorageLevel))
      } else {
        if (!isCarbonToCarbonInsert) {
          Dataset.ofRows(sparkSession, newChild)
        } else {
          // use RDD instead of dataFrame
          null
        }
      }
    val header = relation.tableSchema.get.fields.map(_.name).mkString(",")
    var scanResultRdd : RDD[InternalRow] = null
    if (isCarbonToCarbonInsert) {
      scanResultRdd = sparkSession.sessionState.executePlan(newChild).toRdd
    }
    loadCommand = CarbonLoadDataCommand(
      databaseNameOp = Some(relation.carbonRelation.databaseName),
      tableName = relation.carbonRelation.tableName,
      factPathFromUser = null,
      dimFilesPath = Seq(),
      options = scala.collection.immutable.Map("fileheader" -> header),
      isOverwriteTable = overwrite,
      inputSqlString = null,
      dataFrame = Some(df),
      scanRDD = Some(scanResultRdd),
      updateModel = None,
      tableInfoOp = None,
      internalOptions = Map.empty,
      partition = partition, isCarbonToCarbonInsert = isCarbonToCarbonInsert)
    val load = loadCommand.processMetadata(sparkSession)
    if (isPersistRequired) {
      df.unpersist()
    }
    load
  }
  override def processData(sparkSession: SparkSession): Seq[Row] = {
    if (null != loadCommand) {
      val rows = loadCommand.processData(sparkSession)
      setAuditInfo(loadCommand.auditInfo)
      rows
    } else {
      Seq.empty
    }
  }

  override protected def opName: String = {
    if (overwrite) {
      "INSERT OVERWRITE"
    } else {
      "INSERT INTO"
    }
  }
}
