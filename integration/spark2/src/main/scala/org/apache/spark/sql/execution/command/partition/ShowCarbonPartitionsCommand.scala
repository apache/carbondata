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

package org.apache.spark.sql.execution.command.partition

import org.apache.spark.sql.{AnalysisException, CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.command.{RunnableCommand, SchemaProcessCommand}
import org.apache.spark.sql.hive.CarbonRelation

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.spark.util.CommonUtil

/**
 * Command for show table partitions Command
 */
private[sql] case class ShowCarbonPartitionsCommand(
    tableIdentifier: TableIdentifier)
  extends RunnableCommand with SchemaProcessCommand {

  override val output: Seq[Attribute] = CommonUtil.partitionInfoOutput

  override def run(sparkSession: SparkSession): Seq[Row] = {
    processSchema(sparkSession)
  }

  override def processSchema(sparkSession: SparkSession): Seq[Row] = {
    val relation = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .lookupRelation(tableIdentifier)(sparkSession).asInstanceOf[CarbonRelation]
    val carbonTable = relation.carbonTable
    val tableName = carbonTable.getTableName
    val partitionInfo = carbonTable.getPartitionInfo(
      carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableName)
    if (partitionInfo == null) {
      throw new AnalysisException(
        s"SHOW PARTITIONS is not allowed on a table that is not partitioned: $tableName")
    }
    val partitionType = partitionInfo.getPartitionType
    val columnName = partitionInfo.getColumnSchemaList.get(0).getColumnName
    val LOGGER = LogServiceFactory.getLogService(ShowCarbonPartitionsCommand.getClass.getName)
    LOGGER.info("partition column name:" + columnName)
    CommonUtil.getPartitionInfo(columnName, partitionType, partitionInfo)
  }
}
