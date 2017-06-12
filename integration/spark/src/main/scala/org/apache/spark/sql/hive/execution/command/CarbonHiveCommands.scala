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

import org.apache.spark.sql._
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.execution.command.DropTableCommand
import org.apache.spark.sql.hive.execution.HiveNativeCommand

private[hive] case class CreateDatabaseCommand(dbName: String,
    command: HiveNativeCommand) extends RunnableCommand {
  def run(sqlContext: SQLContext): Seq[Row] = {
    val rows = command.run(sqlContext)
    CarbonEnv.get.carbonMetastore.createDatabaseDirectory(dbName)
    rows
  }
}

private[hive] case class DropDatabaseCommand(dbName: String,
    command: HiveNativeCommand) extends RunnableCommand {
  def run(sqlContext: SQLContext): Seq[Row] = {
    val rows = command.run(sqlContext)
    CarbonEnv.get.carbonMetastore.dropDatabaseDirectory(dbName)
    rows
  }
}

private[hive] case class DropDatabaseCascadeCommand(dbName: String,
    command: HiveNativeCommand) extends RunnableCommand {
  def run(sqlContext: SQLContext): Seq[Row] = {
    val tablesInDB = CarbonEnv.get.carbonMetastore
        .getTables(Some(dbName))(sqlContext).map(x => x._1)
    val rows = command.run(sqlContext)
    tablesInDB.foreach{tableName =>
      DropTableCommand(true, Some(dbName), tableName).run(sqlContext)
    }
    CarbonEnv.get.carbonMetastore.dropDatabaseDirectory(dbName)
    rows
  }
}
