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

package org.apache.spark.sql.execution.command.index

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{Checker, DataCommand}
import org.apache.spark.sql.types.StringType

import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema

/**
 * Show the indexes on the table
 */
case class CarbonShowIndexCommand(table: Option[TableIdentifier])
  extends DataCommand {

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("Name", StringType, nullable = false)(),
      AttributeReference("ClassName", StringType, nullable = false)(),
      AttributeReference("Associated Table", StringType, nullable = false)(),
      AttributeReference("Properties", StringType, nullable = false)(),
      AttributeReference("Status", StringType, nullable = false)(),
      AttributeReference("Sync Info", StringType, nullable = false)())
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    convertToRow(getAllIndexes(sparkSession))
  }

  /**
   * get all indexes
   */
  def getAllIndexes(sparkSession: SparkSession): Seq[DataMapSchema] = {
    val indexSchemaList: util.List[DataMapSchema] = new util.ArrayList[DataMapSchema]()
    table match {
      case Some(table) =>
        val carbonTable = CarbonEnv.getCarbonTable(table)(sparkSession)
        setAuditTable(carbonTable)
        Checker.validateTableExists(table.database, table.table, sparkSession)
        val indexSchemas = DataMapStoreManager.getInstance().getDataMapSchemasOfTable(carbonTable)
        if (!indexSchemas.isEmpty) {
          indexSchemaList.addAll(indexSchemas)
        }
      case _ =>
        indexSchemaList.addAll(DataMapStoreManager.getInstance().getAllIndexSchemas)
    }
    indexSchemaList.asScala
  }

  private def convertToRow(schemaList: Seq[DataMapSchema]) = {
    if (schemaList != null && schemaList.nonEmpty) {
      schemaList
        .map { schema =>
          Row(
            schema.getDataMapName,
            schema.getProviderName,
            schema.getUniqueTableName,
            schema.getPropertiesAsString,
            schema.getStatus.name(),
            schema.getSyncStatus
          )
      }
    } else {
      Seq.empty
    }
  }

  override protected def opName: String = "SHOW INDEX"
}
