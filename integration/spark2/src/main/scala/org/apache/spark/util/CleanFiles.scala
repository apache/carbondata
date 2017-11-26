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

import org.apache.carbondata.api.CarbonStore

/**
 * clean files api
 */
 // scalastyle:off
object CleanFiles {

  /**
   * Clean the stale segments from table
   * @param spark
   * @param dbName
   * @param tableName
   * @param storePath
   * @param forceTableClean if true, it deletes the table and its contents with force.It does not
   *                        drop table from hive metastore so should be very careful to use it.
   */
  def cleanFiles(spark: SparkSession, dbName: String, tableName: String,
      storePath: String, forceTableClean: Boolean = false): Unit = {
    TableAPIUtil.validateTableExists(spark, dbName, tableName)
    val carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(spark)
    CarbonStore.cleanFiles(dbName, tableName, storePath, carbonTable, forceTableClean)
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: CleanFiles <store path> <table name>")
      System.exit(1)
    }

    val storePath = TableAPIUtil.escape(args(0))
    val (dbName, tableName) = TableAPIUtil.parseSchemaName(TableAPIUtil.escape(args(1)))
    var forceTableClean = false
    if (args.length > 2) {
      forceTableClean = args(2).toBoolean
    }
    val spark = TableAPIUtil.spark(storePath, s"CleanFiles: $dbName.$tableName")
    CarbonEnv.getInstance(spark).carbonMetastore.
      checkSchemasModifiedTimeAndReloadTables()
    cleanFiles(spark, dbName, tableName, storePath, forceTableClean)
  }
}
