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

package org.apache.spark.sql.execution.command.table

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.MetadataCommand
import org.apache.spark.sql.types.{BooleanType, StringType}


private[sql] case class CarbonShowTablesCommand ( databaseName: Option[String],
    tableIdentifierPattern: Option[String])  extends MetadataCommand{

  // The result of SHOW TABLES has three columns: database, tableName and isTemporary.
  override val output: Seq[Attribute] = {
    AttributeReference("database", StringType, nullable = false)() ::
    AttributeReference("tableName", StringType, nullable = false)() ::
    AttributeReference("isTemporary", BooleanType, nullable = false)() :: Nil
  }

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    // Since we need to return a Seq of rows, we will call getTables directly
    // instead of calling tables in sparkSession.
    val catalog = sparkSession.sessionState.catalog
    val db = databaseName.getOrElse(catalog.getCurrentDatabase)
    var tables =
      tableIdentifierPattern.map(catalog.listTables(db, _)).getOrElse(catalog.listTables(db))
    val externalCatalog = sparkSession.sharedState.externalCatalog
    // tables will be filtered for all the dataMaps to show only main tables
    tables.collect {
      case tableIdent if externalCatalog.getTable(db, tableIdent.table).storage.properties
        .getOrElse("isVisible", true).toString.toBoolean =>
        val isTemp = catalog.isTemporaryTable(tableIdent)
        Row(tableIdent.database.getOrElse("default"), tableIdent.table, isTemp)
    }
  }

}
