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

package org.apache.spark.sql.secondaryindex.events

import scala.collection.JavaConverters._

import org.apache.log4j.Logger
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.execution.command.AlterTableDropColumnModel
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.secondaryindex.command.DropIndexCommand
import org.apache.spark.sql.secondaryindex.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.events.{AlterTableDropColumnPreEvent, Event, OperationContext, OperationEventListener}

class AlterTableDropColumnEventListener extends OperationEventListener {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case alterTableDropColumnPreEvent: AlterTableDropColumnPreEvent =>
        LOGGER.info("alter table drop column event listener called")
        val carbonTable = alterTableDropColumnPreEvent.carbonTable
        val dbName = carbonTable.getDatabaseName
        val tableName = carbonTable.getTableName
        val tablePath = carbonTable.getTablePath
        val sparkSession = alterTableDropColumnPreEvent.sparkSession
        val alterTableDropColumnModel = alterTableDropColumnPreEvent.alterTableDropColumnModel
        dropApplicableSITables(dbName,
          tableName,
          tablePath,
          alterTableDropColumnModel)(sparkSession)
    }
  }

  private def dropApplicableSITables(dbName: String,
      tableName: String,
      tablePath: String,
      alterTableDropColumnModel: AlterTableDropColumnModel)
    (sparkSession: SparkSession) {
    var indexTableToDrop: Seq[String] = Seq.empty
    val catalog = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    val parentCarbonTable = catalog.lookupRelation(Some(dbName), tableName)(sparkSession)
      .asInstanceOf[CarbonRelation].carbonTable
    CarbonInternalScalaUtil.getIndexesMap(parentCarbonTable).asScala
      .foreach(indexTable => {
        var colSize = 0
        indexTable._2.asScala.foreach(column =>
          if (alterTableDropColumnModel.columns.contains(column)) {
            colSize += 1
          })
        if (colSize > 0) {
          if (colSize == indexTable._2.size) {
            indexTableToDrop ++= Seq(indexTable._1)
          } else {
            sys
              .error(s"Index Table [${
                indexTable
                  ._1
              }] exists with combination of provided drop column(s) and other columns, drop " +
                     s"index table & retry")
          }
        }
      })
    indexTableToDrop.foreach { indexTable =>
      DropIndexCommand(ifExistsSet = true, Some(dbName), indexTable.toLowerCase, tableName)
        .run(sparkSession)
    }
  }
}
