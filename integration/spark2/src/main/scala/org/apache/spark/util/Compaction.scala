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
package org.apache.spark.util

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.AlterTableModel
import org.apache.spark.sql.execution.command.management.CarbonAlterTableCompactionCommand
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.core.constants.CarbonCommonConstants

/**
 * table compaction api
 */
 // scalastyle:off
object Compaction {

  def compaction(spark: SparkSession, dbName: String, tableName: String,
      compactionType: String): Unit = {
    TableAPIUtil.validateTableExists(spark, dbName, tableName)
    if (compactionType.equalsIgnoreCase(CarbonCommonConstants.MAJOR) ||
        compactionType.equalsIgnoreCase(CarbonCommonConstants.MINOR)) {
      CarbonAlterTableCompactionCommand(AlterTableModel(Some(dbName),
        tableName,
        None,
        compactionType,
        Some(System.currentTimeMillis()),
        "")).run(spark)
    }
    else {
      CarbonException.analysisException("Compaction type is wrong. Please select minor or major.")
    }

  }

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: Compaction <store path> <table name> <major|minor>")
      System.exit(1)
    }

    val storePath = TableAPIUtil.escape(args(0))
    val (dbName, tableName) = TableAPIUtil.parseSchemaName(TableAPIUtil.escape(args(1)))
    val compactionType = TableAPIUtil.escape(args(2))
    val spark = TableAPIUtil.spark(storePath, s"Compaction: $dbName.$tableName")
    compaction(spark, dbName, tableName, compactionType)
  }
}
