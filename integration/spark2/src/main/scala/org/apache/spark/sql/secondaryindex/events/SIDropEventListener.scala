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

import java.io.File

import scala.collection.JavaConverters._

import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.secondaryindex.hive.CarbonInternalMetastore
import org.apache.spark.sql.secondaryindex.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.events.{DropTablePreEvent, Event, OperationContext, OperationEventListener}

class SIDropEventListener extends OperationEventListener with Logging {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case dropTablePreEvent: DropTablePreEvent =>
        LOGGER.info("drop table pre event-listener called")
        val parentCarbonTable = dropTablePreEvent.carbonTable
        if(parentCarbonTable.isIndexTable) {
          sys.error(s"Drop table is not permitted on Index Table [${
            parentCarbonTable.getDatabaseName
          }.${ parentCarbonTable.getTableName }]")
        }
        try {
          val tableIdentifier = new TableIdentifier(parentCarbonTable.getTableName,
            Some(parentCarbonTable.getDatabaseName))
          val tablePath = dropTablePreEvent.carbonTable.getTablePath
          val sparkSession = dropTablePreEvent.sparkSession
          val metastore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          var isValidDeletion = false

          CarbonInternalScalaUtil.getIndexesTables(parentCarbonTable).asScala
            .foreach { tableName => {

              val carbonTable = metastore
                .lookupRelation(Some(parentCarbonTable.getDatabaseName),
                  tableName)(sparkSession)
                .asInstanceOf[CarbonRelation].carbonTable
              val ifExistsSet = dropTablePreEvent.ifExistsSet
              val indexesMap = CarbonInternalScalaUtil.getIndexesMap(carbonTable)
              if (null != indexesMap) {
                try {
                  val indexTableIdentifier = TableIdentifier(tableName,
                    Some(parentCarbonTable.getDatabaseName))
                  CarbonInternalMetastore
                    .deleteIndexSilent(indexTableIdentifier,
                      carbonTable.getTablePath,
                      parentCarbonTable)(sparkSession)
                  isValidDeletion = true
                } catch {
                  case ex: Exception =>
                    LOGGER
                      .error(
                        s"Dropping Index table ${ tableIdentifier.database }.${
                          tableIdentifier
                            .table
                        } failed", ex)
                    if (!ifExistsSet) {
                      sys
                        .error(s"Dropping Index table ${ tableIdentifier.database }.${
                          tableIdentifier
                            .table
                        } failed: ${ ex.getMessage }")
                    }
                } finally {
                    if (isValidDeletion) {
                      val databaseLoc = CarbonEnv
                        .getDatabaseLocation(carbonTable.getDatabaseName, sparkSession)
                      val tablePath = databaseLoc + CarbonCommonConstants.FILE_SEPARATOR +
                                      tableName
                      // deleting any remaining files.
                      val metadataFilePath = carbonTable.getMetadataPath
                      val fileType = FileFactory.getFileType(metadataFilePath)
                      if (FileFactory.isFileExist(metadataFilePath)) {
                        val file = FileFactory.getCarbonFile(metadataFilePath)
                        CarbonUtil.deleteFoldersAndFiles(file.getParentFile)
                      }
                      import org.apache.commons.io.FileUtils
                      if (FileFactory.isFileExist(tablePath)) {
                        FileUtils.deleteDirectory(new File(tablePath))
                      }
                    }
                }
              }
            }
            }
        }
        catch {
          case e: Exception => e.printStackTrace()
        }
      case _ =>
    }
  }
}
