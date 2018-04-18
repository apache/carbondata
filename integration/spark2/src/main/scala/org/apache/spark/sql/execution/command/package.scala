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
import org.apache.carbondata.spark.exception.ProcessMetaDataException

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
 * Operation that modifies metadata(schema, table_status, etc)
 */
trait MetadataProcessOpeation {
  def processMetadata(sparkSession: SparkSession): Seq[Row]

  // call this to throw exception when processMetadata failed
  def throwMetadataException(dbName: String, tableName: String, msg: String): Unit = {
    throw new ProcessMetaDataException(dbName, tableName, msg)
  }
}

/**
 * Operation that process data
 */
trait DataProcessOperation {
  def processData(sparkSession: SparkSession): Seq[Row]
}

/**
 * Command that modifies metadata(schema, table_status, etc) only without processing data
 */
abstract class MetadataCommand extends RunnableCommand with MetadataProcessOpeation {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    processMetadata(sparkSession)
  }
}

/**
 * Command that process data only without modifying metadata
 */
abstract class DataCommand extends RunnableCommand with DataProcessOperation {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    processData(sparkSession)
  }
}

/**
 * Subclass of this command is executed in an atomic fashion, either all or nothing.
 * Subclass need to process both metadata and data, processMetadata should be undoable
 * if process data failed.
 */
abstract class AtomicRunnableCommand
  extends RunnableCommand with MetadataProcessOpeation with DataProcessOperation {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    processMetadata(sparkSession)
    try {
      processData(sparkSession)
    } catch {
      case e: Exception =>
        undoMetadata(sparkSession, e)
        throw e
    }
  }

  /**
   * Developer should override this function to undo the changes in processMetadata.
   * @param sparkSession spark session
   * @param exception exception raised when processing data
   * @return rows to return to spark
   */
  def undoMetadata(sparkSession: SparkSession, exception: Exception): Seq[Row] = {
    val msg = s"Got exception $exception when processing data. " +
              s"But this command does not support undo yet, skipping the undo part."
    LogServiceFactory.getLogService(this.getClass.getCanonicalName).error(msg)
    Seq.empty
  }
}
