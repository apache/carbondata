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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.hive._
import org.apache.spark.sql.secondaryindex.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.events.{AlterTableRenamePostEvent, Event, OperationContext, OperationEventListener}

class AlterTableRenameEventListener extends OperationEventListener with Logging {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case alterTableRenamePreEvent: AlterTableRenamePostEvent =>
        LOGGER.info("alter table rename Pre event listener called")
        val alterTableRenameModel = alterTableRenamePreEvent.alterTableRenameModel
        val carbonTable = alterTableRenamePreEvent.carbonTable
        val sparkSession = alterTableRenamePreEvent.sparkSession
        val oldDatabaseName = carbonTable.getDatabaseName
        val newTableName = alterTableRenameModel.newTableIdentifier.table
        val metastore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
        val table: CarbonTable = metastore
          .lookupRelation(Some(oldDatabaseName), newTableName)(sparkSession)
          .asInstanceOf[CarbonRelation].carbonTable
        CarbonInternalScalaUtil.getIndexesMap(table)
          .asScala.map {
          entry =>
            CarbonSessionCatalogUtil.getClient(sparkSession).runSqlHive(
              s"ALTER TABLE $oldDatabaseName.${
                entry
                  ._1
              } SET SERDEPROPERTIES ('parentTableName'='$newTableName')")
        }
    }
  }
}
