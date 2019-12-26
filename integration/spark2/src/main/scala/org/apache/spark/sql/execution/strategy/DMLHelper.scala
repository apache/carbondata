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

package org.apache.spark.sql.execution.strategy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.LoadDataCommand
import org.apache.spark.sql.execution.command.management.{CarbonInsertIntoHadoopFsRelationCommand, CarbonLoadDataCommand}
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.parser.CarbonSparkSqlParserUtil

object DMLHelper {
  def loadData(
      loadDataCommand: LoadDataCommand,
      sparkSession: SparkSession
  ): CarbonLoadDataCommand = {
    val loadOptions = Map.empty[String, String]
    CarbonLoadDataCommand(
      databaseNameOp = loadDataCommand.table.database,
      tableName = loadDataCommand.table.table.toLowerCase,
      factPathFromUser = loadDataCommand.path,
      dimFilesPath = Seq(),
      options = loadOptions,
      isOverwriteTable = loadDataCommand.isOverwrite,
      partition =
        loadDataCommand
          .partition
          .getOrElse(Map.empty)
          .map { case (col, value) =>
            (col, Some(value))
          })
  }

  def insertInto(
      insertIntoCommand: InsertIntoHadoopFsRelationCommand
  ): CarbonInsertIntoHadoopFsRelationCommand = {
    CarbonInsertIntoHadoopFsRelationCommand(
      insertIntoCommand.outputPath,
      CarbonSparkSqlParserUtil.copyTablePartition(insertIntoCommand.staticPartitions),
      insertIntoCommand.ifPartitionNotExists,
      insertIntoCommand.partitionColumns,
      insertIntoCommand.bucketSpec,
      insertIntoCommand.fileFormat,
      insertIntoCommand.options,
      insertIntoCommand.query,
      insertIntoCommand.mode,
      insertIntoCommand.catalogTable,
      insertIntoCommand.fileIndex,
      insertIntoCommand.outputColumnNames)
  }
}
