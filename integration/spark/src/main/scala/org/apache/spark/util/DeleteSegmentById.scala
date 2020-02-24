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

import org.apache.carbondata.api.CarbonStore

/**
 * delete segments by id list
 */
 // scalastyle:off
object DeleteSegmentById {

  def extractSegmentIds(segmentIds: String): Seq[String] = {
    segmentIds.split(",").toSeq
  }

  def deleteSegmentById(spark: SparkSession, dbName: String, tableName: String,
      segmentIds: Seq[String]): Unit = {
    TableAPIUtil.validateTableExists(spark, dbName, tableName)
    val carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(spark)
    CarbonStore.deleteLoadById(segmentIds, dbName, tableName, carbonTable)
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println(
        "Usage: DeleteSegmentByID <store path> <table name> <segment id list>")
      System.exit(1)
    }

    val storePath = TableAPIUtil.escape(args(0))
    val (dbName, tableName) = TableAPIUtil.parseSchemaName(TableAPIUtil.escape(args(1)))
    val segmentIds = extractSegmentIds(TableAPIUtil.escape(args(2)))
    val spark = TableAPIUtil.spark(storePath, s"DeleteSegmentById: $dbName.$tableName")
    deleteSegmentById(spark, dbName, tableName, segmentIds)
  }
}
