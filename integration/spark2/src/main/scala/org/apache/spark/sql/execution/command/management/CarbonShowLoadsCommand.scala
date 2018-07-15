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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{Checker, DataCommand}
import org.apache.spark.sql.types.{StringType, TimestampType}

import org.apache.carbondata.api.CarbonStore
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException

case class CarbonShowLoadsCommand(
    databaseNameOp: Option[String],
    tableName: String,
    limit: Option[String],
    showHistory: Boolean = false)
  extends DataCommand {

  // add new columns of show segments at last
  override def output: Seq[Attribute] = {
    if (showHistory) {
      Seq(AttributeReference("SegmentSequenceId", StringType, nullable = false)(),
        AttributeReference("Status", StringType, nullable = false)(),
        AttributeReference("Load Start Time", TimestampType, nullable = false)(),
        AttributeReference("Load End Time", TimestampType, nullable = true)(),
        AttributeReference("Merged To", StringType, nullable = false)(),
        AttributeReference("File Format", StringType, nullable = false)(),
        AttributeReference("Visibility", StringType, nullable = false)())
    } else {
      Seq(AttributeReference("SegmentSequenceId", StringType, nullable = false)(),
        AttributeReference("Status", StringType, nullable = false)(),
        AttributeReference("Load Start Time", TimestampType, nullable = false)(),
        AttributeReference("Load End Time", TimestampType, nullable = true)(),
        AttributeReference("Merged To", StringType, nullable = false)(),
        AttributeReference("File Format", StringType, nullable = false)())
    }
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }
    CarbonStore.showSegments(
      limit,
      carbonTable.getAbsoluteTableIdentifier,
      showHistory
    )
  }
}
