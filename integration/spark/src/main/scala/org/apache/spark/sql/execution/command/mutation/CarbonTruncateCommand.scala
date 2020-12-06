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

package org.apache.spark.sql.execution.command.mutation

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.command.{Checker, DataCommand, TruncateTableCommand}
import org.apache.spark.sql.optimizer.CarbonFilters

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.trash.DataTrashManager

case class CarbonTruncateCommand(child: TruncateTableCommand) extends DataCommand {
  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    Checker.validateTableExists(child.tableName.database, child.tableName.table, sparkSession)
    val carbonTable = CarbonEnv.getCarbonTable(
      child.tableName.database, child.tableName.table)(sparkSession)
    setAuditTable(carbonTable)
    if (!carbonTable.isTransactionalTable) {
      LOGGER.error(s"Unsupported truncate non-transactional table")
      throw new MalformedCarbonCommandException(
        "Unsupported truncate non-transactional table")
    }
    if (child.partitionSpec.isDefined) {
      throw new MalformedCarbonCommandException(
        "Unsupported truncate table with specified partition")
    }
    SegmentStatusManager.truncateTable(carbonTable)
    Seq.empty
  }

  override protected def opName = "TRUNCATE TABLE"
}
