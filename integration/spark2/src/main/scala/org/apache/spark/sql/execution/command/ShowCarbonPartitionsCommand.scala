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

package org.apache.spark.sql.execution.command

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.types._

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.partition.PartitionType



private[sql] case class ShowCarbonPartitionsCommand(
    tableIdentifier: TableIdentifier) extends RunnableCommand {
  val LOGGER = LogServiceFactory.getLogService(ShowCarbonPartitionsCommand.getClass.getName)
  var columnName = ""
  override val output: Seq[Attribute] = Seq(
    // Column names are based on Hive.
    AttributeReference("ID", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "partition id").build())(),
    AttributeReference("Name", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "partition name").build())(),
    AttributeReference("Value", StringType, nullable = true,
      new MetadataBuilder().putString("comment", "partition value").build())()
  )
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val relation = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .lookupRelation(tableIdentifier)(sparkSession).
      asInstanceOf[CarbonRelation]
    val carbonTable = relation.tableMeta.carbonTable
    var partitionInfo = carbonTable.getPartitionInfo(
      carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableName)
    var partitionType = partitionInfo.getPartitionType
    var result = Seq.newBuilder[Row]
    columnName = partitionInfo.getColumnSchemaList.get(0).getColumnName
    LOGGER.info("partition column name:" + columnName)
    partitionType match {
      case PartitionType.RANGE =>
        result.+=(RowFactory.create("0", "", "default"))
        var id = 1
        var rangeInfo = partitionInfo.getRangeInfo
        var size = rangeInfo.size() - 1
        for (index <- 0 to size) {
          result.+=(RowFactory.create(id.toString(), "", "< " + rangeInfo.get(index)))
          id += 1
        }
      case PartitionType.RANGE_INTERVAL =>
        result.+=(RowFactory.create("", "", ""))
      case PartitionType.LIST =>
        result.+=(RowFactory.create("0", "", "default"))
        var id = 1
        var listInfo = partitionInfo.getListInfo
        var size = listInfo.size() - 1
        for (index <- 0 to size) {
          var listStr = ""
          listInfo.get(index).toArray().foreach { x =>
            if (listStr.isEmpty()) {
              listStr = x.toString()
            } else {
              listStr += ", " + x.toString()
            }
          }
          result.+=(RowFactory.create(id.toString(), "", listStr))
          id += 1
        }
      case PartitionType.HASH =>
        var hashNumber = partitionInfo.getNumPartitions
        result.+=(RowFactory.create("HASH PARTITION", "", hashNumber.toString()))
      case others =>
        result.+=(RowFactory.create("", "", ""))
    }
    result.result()
  }
}

