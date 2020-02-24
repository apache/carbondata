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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command._
import org.apache.spark.util.AlterTableUtil

private[sql] case class CarbonAlterTableSetCommand(
    tableIdentifier: TableIdentifier,
    properties: Map[String, String],
    isView: Boolean)
  extends MetadataCommand {

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    setAuditTable(tableIdentifier.database.getOrElse(sparkSession.catalog.currentDatabase),
      tableIdentifier.table)
    AlterTableUtil.modifyTableProperties(
      tableIdentifier,
      properties,
      Nil,
      set = true)(sparkSession,
      sparkSession.sessionState.catalog)
    setAuditInfo(properties)
    Seq.empty
  }

  override protected def opName: String = "ALTER TABLE SET"
}
