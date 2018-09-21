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
import org.apache.carbondata.core.metadata.schema.datamap.{DataMapClassProvider, DataMapProperty}
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
      AttributeReference("DataMap Properties", StringType, nullable = false)())
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    convertToRow(getAllDataMaps(sparkSession))
  }

  /**
   * get all datamaps for this table, including preagg, index datamaps and mv
   */
  def getAllDataMaps(sparkSession: SparkSession): util.List[DataMapSchema] = {
    val dataMapSchemaList: util.List[DataMapSchema] = new util.ArrayList[DataMapSchema]()
    tableIdentifier match {
      case Some(table) =>
        val carbonTable = CarbonEnv.getCarbonTable(table)(sparkSession)
        Checker.validateTableExists(table.database, table.table, sparkSession)
        if (carbonTable.hasDataMapSchema) {
          dataMapSchemaList.addAll(carbonTable.getTableInfo.getDataMapSchemaList)
        }
        val indexSchemas = DataMapStoreManager.getInstance().getDataMapSchemasOfTable(carbonTable)
        if (!indexSchemas.isEmpty) {
          dataMapSchemaList.addAll(indexSchemas)
        }
      case _ =>
        dataMapSchemaList.addAll(DataMapStoreManager.getInstance().getAllDataMapSchemas)
    }
    dataMapSchemaList
  }

  private def convertToRow(schemaList: util.List[DataMapSchema]) = {
    if (schemaList != null && schemaList.size() > 0) {
      schemaList.asScala
        .map { s =>
          val relationIdentifier = s.getRelationIdentifier
          val table = relationIdentifier.getDatabaseName + "." + relationIdentifier.getTableName
          // preaggregate datamap does not support user specified property, therefor we return empty
          val dmPropertieStr = if (s.getProviderName.equalsIgnoreCase(
              DataMapClassProvider.PREAGGREGATE.getShortName)) {
            ""
          } else {
            s.getProperties.asScala
              // ignore internal used property
              .filter(p => !p._1.equalsIgnoreCase(DataMapProperty.DEFERRED_REBUILD) &&
                           !p._1.equalsIgnoreCase(DataMapProperty.CHILD_SELECT_QUERY) &&
                           !p._1.equalsIgnoreCase(DataMapProperty.QUERY_TYPE))
              .map(p => s"'${ p._1 }'='${ p._2 }'").toSeq
              .sorted.mkString(", ")
          }
        Row(s.getDataMapName, s.getProviderName, table, dmPropertieStr)
      }
    } else {
      Seq.empty
    }
  }
}
