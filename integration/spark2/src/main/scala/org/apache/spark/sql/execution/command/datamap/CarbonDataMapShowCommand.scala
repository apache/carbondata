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

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{Checker, DataCommand}
import org.apache.spark.sql.types.StringType

/**
 * Show the datamaps on the table
 * @param databaseNameOp
 * @param tableName
 */
case class CarbonDataMapShowCommand(
    databaseNameOp: Option[String],
    tableName: String)
  extends DataCommand {

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("DataMapName", StringType, nullable = false)(),
      AttributeReference("ClassName", StringType, nullable = false)(),
      AttributeReference("Associated Table", StringType, nullable = false)())
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    val schemaList = carbonTable.getTableInfo.getDataMapSchemaList
    if (schemaList != null && schemaList.size() > 0) {
      schemaList.asScala.map { s =>
        var table = "(NA)"
        val relationIdentifier = s.getRelationIdentifier
        if (relationIdentifier != null) {
          table = relationIdentifier.getDatabaseName + "." + relationIdentifier.getTableName
        }
        Row(s.getDataMapName, s.getProviderName, table)
      }
    } else {
      Seq.empty
    }
  }
}
