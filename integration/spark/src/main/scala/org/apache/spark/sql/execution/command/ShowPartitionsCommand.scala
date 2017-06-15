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
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.types._

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.partition.PartitionType


private[sql] case class ShowCarbonPartitionsCommand(
    tableIdentifier: TableIdentifier) extends RunnableCommand {
  val LOGGER = LogServiceFactory.getLogService(ShowCarbonPartitionsCommand.getClass.getName)
  override val output: Seq[Attribute] = Seq(
    // Column names are based on Hive.
    AttributeReference("ID", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "partition id").build())(),
    AttributeReference("Name", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "partition name").build())(),
    AttributeReference("Value", StringType, nullable = true,
      new MetadataBuilder().putString("comment", "partition value").build())()
  )
  override def run(sqlContext: SQLContext): Seq[Row] = {
 val relation = CarbonEnv.get.carbonMetastore
      .lookupRelation1(tableIdentifier)(sqlContext).
      asInstanceOf[CarbonRelation]
    val carbonTable = relation.tableMeta.carbonTable
    var partitionInfo = carbonTable.getPartitionInfo(
      carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableName)
    var partitionType = partitionInfo.getPartitionType
    var result = Seq.newBuilder[Row]
    // val result = new ArrayBuffer[Row]
    if (PartitionType.RANGE.equals(partitionType)) {
      result.+=(RowFactory.create("partition0", "0", "0"))
    } else if (PartitionType.RANGE_INTERVAL.equals(partitionType)) {
      result.+=(RowFactory.create("partition0", "0", "0"))
    } else if (PartitionType.LIST.equals(partitionType)) {
      result.+=(RowFactory.create("partition0", "0", "0"))
    } else if (PartitionType.HASH.equals(partitionType)) {
      result.+=(RowFactory.create("partition0", "0", "0"))
    }

    result.result()
  }
}

