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

package org.apache.spark.sql.execution.command

import scala.language.implicitConversions

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

import org.apache.carbondata.common.logging.LogServiceFactory

object Checker {
  def validateTableExists(
      dbName: Option[String],
      tableName: String,
      session: SparkSession): Unit = {
    val database = dbName.getOrElse(session.catalog.currentDatabase)
    val identifier = TableIdentifier(tableName, dbName)
    if (!CarbonEnv.getInstance(session).carbonMetastore.tableExists(identifier)(session)) {
      val err = s"table $dbName.$tableName not found"
      LogServiceFactory.getLogService(this.getClass.getName).error(err)
      throw new NoSuchTableException(database, tableName)
    }
  }
}

/**
 * Interface for command that modifies schema
 */
trait SchemaProcessCommand {
  def processSchema(sparkSession: SparkSession): Seq[Row]
}

/**
 * Interface for command that need to process data in file system
 */
trait DataProcessCommand {
  def processData(sparkSession: SparkSession): Seq[Row]
}
