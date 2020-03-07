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

package org.apache.spark.sql.execution.command.view

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{Checker, DataCommand}
import org.apache.spark.sql.types.{BooleanType, StringType}

import org.apache.carbondata.core.view.{MVProperty, MVSchema}
import org.apache.carbondata.view.MVManagerInSpark

/**
 * Show Materialized View Command implementation
 */
case class CarbonShowMVCommand(
    databaseNameOption: Option[String],
    relatedTableIdentifier: Option[TableIdentifier]) extends DataCommand {

  override def output: Seq[Attribute] = {
    Seq(
      AttributeReference("Database", StringType, nullable = false)(),
      AttributeReference("Name", StringType, nullable = false)(),
      AttributeReference("Status", StringType, nullable = false)(),
      AttributeReference("Refresh Mode", StringType, nullable = false)(),
      AttributeReference("Refresh Trigger Mode", StringType, nullable = false)(),
      AttributeReference("Properties", StringType, nullable = false)())
  }

  override def processData(session: SparkSession): Seq[Row] = {
    // Get mv schemas.
    val schemaList = new util.ArrayList[MVSchema]()
    val viewManager = MVManagerInSpark.get(session)
    relatedTableIdentifier match {
      case Some(table) =>
        val relatedTable = CarbonEnv.getCarbonTable(table)(session)
        setAuditTable(relatedTable)
        Checker.validateTableExists(table.database, table.table, session)
        if (databaseNameOption.isDefined) {
          schemaList.addAll(viewManager.getSchemasOnTable(
            databaseNameOption.get,
            relatedTable))
        } else {
          schemaList.addAll(viewManager.getSchemasOnTable(relatedTable))
        }
      case _ =>
        if (databaseNameOption.isDefined) {
          schemaList.addAll(viewManager.getSchemas(databaseNameOption.get))
        } else {
          schemaList.addAll(viewManager.getSchemas())
        }
    }
    // Convert mv schema to row.
    schemaList.asScala.map {
      schema =>
        Row(
          schema.getIdentifier.getDatabaseName,
          schema.getIdentifier.getTableName,
          schema.getStatus.name(),
          schema.getProperties.get(MVProperty.REFRESH_MODE),
          schema.getProperties.get(MVProperty.REFRESH_TRIGGER_MODE),
          schema.getPropertiesAsString
        )
    }
  }

  override protected def opName: String = "SHOW MATERIALIZED VIEW"
}
