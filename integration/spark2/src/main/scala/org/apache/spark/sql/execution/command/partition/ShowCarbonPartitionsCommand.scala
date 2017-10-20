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

import scala.collection.JavaConverters._

import org.apache.spark.sql.{AnalysisException, CarbonEnv, Row, RowFactory, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{RunnableCommand, SchemaProcessCommand}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.types.{MetadataBuilder, StringType}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.PartitionInfo
import org.apache.carbondata.core.metadata.schema.partition.PartitionType

/**
 * Command for show table partitions Command
 */
private[sql] case class ShowCarbonPartitionsCommand(
    tableIdentifier: TableIdentifier)
  extends RunnableCommand with SchemaProcessCommand {

  override val output: Seq[Attribute] = partitionInfoOutput

  override def run(sparkSession: SparkSession): Seq[Row] = {
    processSchema(sparkSession)
  }

  override def processSchema(sparkSession: SparkSession): Seq[Row] = {
    val relation = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .lookupRelation(tableIdentifier)(sparkSession).
      asInstanceOf[CarbonRelation]
    val carbonTable = relation.tableMeta.carbonTable
    val tableName = carbonTable.getFactTableName
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
    getPartitionInfo(columnName, partitionType, partitionInfo)
  }

  private def partitionInfoOutput: Seq[Attribute] = Seq(
    AttributeReference("Partition(Id, DESC)", StringType, false,
      new MetadataBuilder().putString("comment", "partition").build())()
  )

  private def getPartitionInfo(columnName: String, partitionType: PartitionType,
      partitionInfo: PartitionInfo): Seq[Row] = {
    var result = Seq.newBuilder[Row]
    partitionType match {
      case PartitionType.RANGE =>
        result.+=(RowFactory.create("0" + ", " + columnName + " = DEFAULT"))
        val rangeInfo = partitionInfo.getRangeInfo
        val size = rangeInfo.size() - 1
        for (index <- 0 to size) {
          if (index == 0) {
            val id = partitionInfo.getPartitionId(index + 1).toString
            val desc = columnName + " < " + rangeInfo.get(index)
            result.+=(RowFactory.create(id + ", " + desc))
          } else {
            val id = partitionInfo.getPartitionId(index + 1).toString
            val desc = rangeInfo.get(index - 1) + " <= " + columnName + " < " + rangeInfo.get(index)
            result.+=(RowFactory.create(id + ", " + desc))
          }
        }
      case PartitionType.RANGE_INTERVAL =>
        result.+=(RowFactory.create(columnName + " = "))
      case PartitionType.LIST =>
        result.+=(RowFactory.create("0" + ", " + columnName + " = DEFAULT"))
        val listInfo = partitionInfo.getListInfo
        listInfo.asScala.foreach {
          f =>
            val id = partitionInfo.getPartitionId(listInfo.indexOf(f) + 1).toString
            val desc = columnName + " = " + f.toArray().mkString(", ")
            result.+=(RowFactory.create(id + ", " + desc))
        }
      case PartitionType.HASH =>
        val hashNumber = partitionInfo.getNumPartitions
        result.+=(RowFactory.create(columnName + " = HASH_NUMBER(" + hashNumber.toString() + ")"))
      case others =>
        result.+=(RowFactory.create(columnName + " = "))
    }
    val rows = result.result()
    rows
  }
}
