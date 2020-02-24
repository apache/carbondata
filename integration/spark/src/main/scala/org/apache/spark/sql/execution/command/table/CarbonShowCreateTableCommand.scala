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

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{MetadataCommand, ShowCreateTableCommand}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.types.StringType

import org.apache.carbondata.common.logging.LogServiceFactory

case class CarbonShowCreateTableCommand(
    child: ShowCreateTableCommand
) extends MetadataCommand {
  override val output: Seq[Attribute] = Seq(
    AttributeReference("createtab_stmt", StringType, nullable = false)()
  )

  override protected def opName: String = "SHOW CREATE TABLE"

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val dbName = CarbonEnv.getDatabaseName(child.table.database)(sparkSession)
    val tableName = child.table.table
    setAuditTable(dbName, tableName)
    val relation = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      .lookupRelation(Option(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation]
    if (relation == null) {
      throw new NoSuchTableException(dbName, tableName)
    }
    if (null == relation.carbonTable) {
      LOGGER.error(s"Show create table failed. table not found: $dbName.$child.tableName.table")
      throw new NoSuchTableException(dbName, child.table.table)
    }

    val originSql = child
      .run(sparkSession)
      .head
      .getString(0)
    val rows = originSql
      .split("\n", -1)

    var inOptions = false
    val indexes = rows
      .zipWithIndex
      .map { case (row, index) =>
        if (inOptions) {
          if (row.equals(")")) {
            inOptions = false
            index
          } else {
            -1
          }
        } else {
          if (row.equals("OPTIONS (")) {
            inOptions = true
            index
          } else {
            -1
          }
        }
      }
      .filter(_ != -1)
    if (indexes.length == 2) {
      val part1 = rows.take(indexes(0))
      val part3 = if (indexes(1) < rows.length - 1) {
        rows.takeRight(rows.length - 1 - indexes(1))
      } else {
        Array.empty[String]
      }
      val part2 = rows
        .slice(indexes(0), indexes(1) + 1)
        .filterNot(isInvisible(_))
      val allParts = if (part2.length == 2) {
        part1 ++ part3
      } else {
        val lastOption = part2(part2.length -2)
        if (lastOption.endsWith(",")) {
          part2(part2.length -2) = lastOption.substring(0, lastOption.length - 1 )
        }
        part1 ++ part2 ++ part3
      }
      Seq(Row(allParts.mkString("\n")))
    } else {
      Seq(Row(originSql))
    }
  }

  private val startKeyWords =
    Seq("`tablepath`", "`carbonschema", "`dbname`", "`tablename`", "`serialization.format`",
      "`isexternal`", "`isvisible`", "`istransactional`")

  private def isInvisible(row: String): Boolean = {
    if (row != null) {
      val tmpRow = row.trim.toLowerCase
      startKeyWords.exists(tmpRow.startsWith(_))
    } else {
      false
    }
  }

}
