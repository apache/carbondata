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

package org.apache.spark.sql.execution.command.schema

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.DataCommand
import org.apache.spark.sql.types.{LongType, StringType}

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonProperties

/**
 * collect dynamic detail information of the table, including table size, last modified time, etc.
 */
case class CarbonGetTableDetailCommand(
    databaseName: String,
    tableNames: Option[Seq[String]])
  extends DataCommand {

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    if (tableNames.isDefined) {
      tableNames.get.map { tablename =>
        val carbonTable = CarbonEnv.getCarbonTable(Option(databaseName),
          tablename)(sparkSession)

        Row(
          tablename,
          carbonTable.size,
          SegmentStatusManager
            .getTableStatusLastModifiedTime(carbonTable.getAbsoluteTableIdentifier))
      }
    } else {
      Seq.empty[Row]
    }
  }

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("table name", StringType, nullable = false)(),
      AttributeReference("table size", LongType, nullable = false)(),
      AttributeReference("last modified time", LongType, nullable = false)())
  }

  override protected def opName: String = "GET TABLE DETAIL"
}
