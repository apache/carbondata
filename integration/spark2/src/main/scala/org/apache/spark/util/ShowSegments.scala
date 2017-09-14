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

import java.text.SimpleDateFormat

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.hive.CarbonRelation

import org.apache.carbondata.api.CarbonStore

// scalastyle:off
object ShowSegments {

  def showSegments(spark: SparkSession, dbName: String, tableName: String,
      limit: Option[String]): Seq[Row] = {
    TableAPIUtil.validateTableExists(spark, dbName, tableName)
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetastore.
      lookupRelation(Some(dbName), tableName)(spark).asInstanceOf[CarbonRelation].
      tableMeta.carbonTable
    CarbonStore.showSegments(dbName, tableName, limit, carbonTable.getMetaDataFilepath)
  }

  def showString(rows: Seq[Row]): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.s")
    val sb = new StringBuilder
    sb.append("+------------+------------------+----------------------+----------------------+----------------------+\n")
      .append("|SegmentId   |Status            |Load Start Time       |Load End Time         |Merged To             |\n")
      .append("+------------+------------------+----------------------+----------------------+----------------------+\n")
      rows.foreach{row =>
        sb.append("|")
          .append(StringUtils.rightPad(row.getString(0), 12))
          .append("|")
          .append(StringUtils.rightPad(row.getString(1), 18))
          .append("|")
          .append(sdf.format(row.getAs[java.sql.Timestamp](2)))
          .append("|")
          .append(sdf.format(row.getAs[java.sql.Timestamp](3)))
          .append("|")
          .append(StringUtils.rightPad(row.getString(4), 18))
          .append("|\n")
      }
    sb.append("+------------+------------------+----------------------+----------------------+----------------------+\n")
    sb.toString
  }

  def parseLimit(limit: String): Int = {
    Integer.parseInt(limit)
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: ShowSegments <store path> <table name> [limit]")
      System.exit(1)
    }

    val storePath = TableAPIUtil.escape(args(0))
    val (dbName, tableName) = TableAPIUtil.parseSchemaName(TableAPIUtil.escape(args(1)))

    val limit = if (args.length >= 3 ) {
      Some(TableAPIUtil.escape(args(2)))
    } else {
      None
    }
    val spark = TableAPIUtil.spark(storePath, s"ShowSegments: $dbName.$tableName")
    CarbonEnv.getInstance(spark).carbonMetastore.
      checkSchemasModifiedTimeAndReloadTables(CarbonEnv.getInstance(spark).storePath)
    val rows = showSegments(spark, dbName, tableName, limit)
    System.out.println(showString(rows))
  }
}
