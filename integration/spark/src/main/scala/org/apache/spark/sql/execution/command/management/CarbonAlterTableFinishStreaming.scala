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

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.MetadataCommand

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.streaming.segment.StreamSegment

/**
 * This command will try to change the status of the segment from "streaming" to "streaming finish"
 */
case class CarbonAlterTableFinishStreaming(
    dbName: Option[String],
    tableName: String)
  extends MetadataCommand {
  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val carbonTable = CarbonEnv.getCarbonTable(dbName, tableName)(sparkSession)
    setAuditTable(carbonTable)
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val streamingLock = CarbonLockFactory.getCarbonLockObj(
      carbonTable.getTableInfo().getOrCreateAbsoluteTableIdentifier(),
      LockUsage.STREAMING_LOCK)
    try {
      if (streamingLock.lockWithRetries()) {
        StreamSegment.finishStreaming(carbonTable)
      } else {
        val msg = "Failed to finish streaming, because streaming is locked for table " +
                  carbonTable.getDatabaseName() + "." + carbonTable.getTableName()
        LOGGER.error(msg)
        throwMetadataException(carbonTable.getDatabaseName, carbonTable.getTableName, msg)
      }
    } finally {
      if (streamingLock.unlock()) {
        LOGGER.info("Table unlocked successfully after streaming finished" +
                    carbonTable.getDatabaseName() + "." + carbonTable.getTableName())
      } else {
        LOGGER.error("Unable to unlock Table lock for table " +
                     carbonTable.getDatabaseName() + "." + carbonTable.getTableName() +
                     " during streaming finished")
      }
    }
    Seq.empty
  }

  override protected def opName: String = "ALTER TABLE FINISH STREAMING"
}
