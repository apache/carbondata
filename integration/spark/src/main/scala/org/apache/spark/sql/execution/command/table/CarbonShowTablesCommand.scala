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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.command.{MetadataCommand, ShowTablesCommand}


private[sql] case class CarbonShowTablesCommand(showTablesCommand: ShowTablesCommand)
  extends MetadataCommand {

  override val output: Seq[Attribute] = showTablesCommand.output

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val rows = showTablesCommand.run(sparkSession)
    val externalCatalog = sparkSession.sharedState.externalCatalog
    // this method checks whether the table is mainTable or MV based on property "isVisible"
    def isMainTable(db: String, table: String) = {
      var isMainTable = true
      try {
        isMainTable = externalCatalog.getTable(db, table).storage.properties
          .getOrElse("isVisible", true).toString.toBoolean
      } catch {
        case ex: Throwable =>
        // ignore the exception for show tables
      }
      isMainTable
    }
    // tables will be filtered for all the MVs to show only main tables
    rows.filter(row => isMainTable(row.get(0).toString, row.get(1).toString))
  }

  override protected def opName: String = "SHOW TABLES"
}
