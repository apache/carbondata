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

package org.apache.spark.sql.hive.execution.command

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.{CarbonDropTableCommand, DropDatabaseCommand, RunnableCommand}

case class CarbonDropDatabaseCommand(command: DropDatabaseCommand)
  extends RunnableCommand {

  override val output = command.output

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val dbName = command.databaseName
    // DropHiveDB command will fail if cascade is false and one or more table exists in database
    val rows = command.run(sparkSession)
    if (command.cascade) {
      val tablesInDB = CarbonEnv.getInstance(sparkSession).carbonMetastore.getAllTables()
        .filterNot(_.database.exists(_.equalsIgnoreCase(dbName)))
      tablesInDB.foreach { tableName =>
        CarbonDropTableCommand(true, Some(dbName), tableName.table).run(sparkSession)
      }
    }
    CarbonEnv.getInstance(sparkSession).carbonMetastore.dropDatabaseDirectory(dbName)
    rows
  }
}

