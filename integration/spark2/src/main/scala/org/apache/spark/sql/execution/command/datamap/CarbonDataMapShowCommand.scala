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

package org.apache.spark.sql.execution.command.datamap

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{Checker, DataCommand}
import org.apache.spark.sql.types.StringType

import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema

/**
 * Show the datamaps on the table
 *
 * @param tableIdentifier
 */
case class CarbonDataMapShowCommand(tableIdentifier: Option[TableIdentifier])
  extends DataCommand {

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("DataMapName", StringType, nullable = false)(),
      AttributeReference("ClassName", StringType, nullable = false)(),
      AttributeReference("Associated Table", StringType, nullable = false)(),
      AttributeReference("DMProperties", StringType, nullable = false)())
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val finalSchemaList: util.List[DataMapSchema] = new util.ArrayList[DataMapSchema]()
    tableIdentifier match {
      case Some(table) =>
        Checker.validateTableExists(table.database, table.table, sparkSession)
        val carbonTable = CarbonEnv.getCarbonTable(table)(sparkSession)
        if (carbonTable.hasDataMapSchema) {
          finalSchemaList.addAll(carbonTable.getTableInfo.getDataMapSchemaList)
        }
        val indexSchemas = DataMapStoreManager.getInstance().getAllDataMapSchemas(carbonTable)
        if (!indexSchemas.isEmpty) {
          finalSchemaList.addAll(indexSchemas)
        }
        convertToRow(finalSchemaList)
      case _ =>
        convertToRow(DataMapStoreManager.getInstance().getAllDataMapSchemas)
    }
  }

  private def convertToRow(schemaList: util.List[DataMapSchema]) = {
    if (schemaList != null && schemaList.size() > 0) {
      schemaList.asScala.map { s =>
        var table = "(NA)"
        val relationIdentifier = s.getRelationIdentifier
        var dmProperties = "(NA)"
        val isFGorCGdm =
          s.getProviderName.equalsIgnoreCase(DataMapClassProvider.LUCENEFG.toString) ||
          s.getProviderName.equalsIgnoreCase(DataMapClassProvider.LUCENECG.toString)
        if (relationIdentifier != null && !isFGorCGdm) {
          table = relationIdentifier.getDatabaseName + "." + relationIdentifier.getTableName
        }
        if (s.getProviderName.equalsIgnoreCase(DataMapClassProvider.PREAGGREGATE.toString)) {
          dmProperties = "(NA)"
        } else {
          dmProperties = s.getProperties.keySet().toString.concat("=>")
            .concat(s.getProperties.values().toString)
        }
        Row(s.getDataMapName, s.getProviderName, table, dmProperties)
      }
    } else {
      Seq.empty
    }
  }
}
