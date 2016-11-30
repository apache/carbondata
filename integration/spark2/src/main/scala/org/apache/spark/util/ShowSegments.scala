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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.command.ShowLoads
import org.apache.spark.sql.types.{StringType, TimestampType}

// scalastyle:off
object ShowSegments {

  def showSegments(spark: SparkSession, dbName: Option[String], tableName: String,
      limit: Option[String]): Seq[Row] = {
    val output =  Seq(AttributeReference("SegmentSequenceId", StringType, nullable = false)(),
      AttributeReference("Status", StringType, nullable = false)(),
      AttributeReference("Load Start Time", TimestampType, nullable = false)(),
      AttributeReference("Load End Time", TimestampType, nullable = false)())
    ShowLoads(dbName, tableName, limit: Option[String], output).run(spark)
  }

  def showString(rows: Seq[Row]): String = {
    val sb = new StringBuilder
    sb.append("+-----------------+---------------+---------------------+---------------------+\n")
      .append("|SegmentSequenceId|Status         |Load Start Time      |Load End Time        |\n")
      .append("+-----------------+---------------+---------------------+---------------------+\n")
      rows.foreach{row =>
        sb.append("|")
          .append(StringUtils.rightPad(row.getString(0), 17))
          .append("|")
          .append(StringUtils.rightPad(row.getString(1).substring(0, 15), 15))
          .append("|")
          .append(row.getAs[java.sql.Timestamp](2).formatted("yyyy-MM-dd HH:mm:ss.s"))
          .append("|")
          .append(row.getAs[java.sql.Timestamp](3).formatted("yyyy-MM-dd HH:mm:ss.s"))
          .append("|\n")
      }
    sb.append("+-----------------+---------------+---------------------+---------------------+\n")
    sb.toString
  }

  def parseLimit(limit: String): Int = {
    Integer.parseInt(limit)
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: ShowSegments <store path> <table name> [limit]");
      System.exit(1)
    }

    val storePath = TableAPIUtil.escape(args(0))
    val (dbName, tableName) = TableAPIUtil.parseSchemaName(TableAPIUtil.escape(args(1)))

    val limit = if (args.length >= 3 ) {
      Some(TableAPIUtil.escape(args(2)))
    } else {
      None
    }
    val spark = TableAPIUtil.spark(storePath, s"TableCleanFiles: $dbName.$tableName")
    CarbonEnv.init(spark.sqlContext)
    val rows = showSegments(spark, Option(dbName), tableName, limit)
    System.out.println(showString(rows))
  }
}
