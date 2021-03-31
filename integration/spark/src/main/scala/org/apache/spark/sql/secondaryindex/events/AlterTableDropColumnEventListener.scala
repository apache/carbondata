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
import org.apache.spark.sql.execution.command.index.DropIndexCommand
import org.apache.spark.sql.hive.CarbonRelation

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.index.IndexType
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
        checkIfDropColumnExistsInSI(dbName,
          tableName,
          tablePath,
          alterTableDropColumnModel)(sparkSession)
    }
  }

  private def checkIfDropColumnExistsInSI(dbName: String,
      tableName: String,
      tablePath: String,
      alterTableDropColumnModel: AlterTableDropColumnModel)
    (sparkSession: SparkSession) {
    val catalog = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    val parentCarbonTable = catalog.lookupRelation(Some(dbName), tableName)(sparkSession)
      .asInstanceOf[CarbonRelation].carbonTable
    val secondaryIndexMap =
      parentCarbonTable.getIndexesMap.get(IndexType.SI.getIndexProviderName)
    if (null == secondaryIndexMap) {
      // if secondary index map is empty, return
      return
    }
    secondaryIndexMap.asScala.foreach(indexTable => {
      val indexColumns = indexTable._2.asScala(CarbonCommonConstants.INDEX_COLUMNS).split(",")
      val colSize = alterTableDropColumnModel.columns.intersect(indexColumns).size
      if (colSize > 0) {
        sys
          .error(s"The provided column(s) are present in index table. Please drop the " +
                 s"index table [${ indexTable._1 }] first and then retry the drop column operation")

      }
    })
  }
}
