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

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.processing.util.Auditor
import org.apache.carbondata.spark.exception.ProcessMetaDataException

object Checker {
  def validateTableExists(
      dbName: Option[String],
      tableName: String,
      session: SparkSession): Unit = {
    val database = dbName.getOrElse(session.catalog.currentDatabase)
    val identifier = TableIdentifier(tableName, dbName)
    if (!CarbonEnv.getInstance(session).carbonMetaStore.tableExists(identifier)(session)) {
      val err = s"table $dbName.$tableName not found"
      LogServiceFactory.getLogService(this.getClass.getName).error(err)
      throw new NoSuchTableException(database, tableName)
    }
  }
}

/**
 * Operation that modifies metadata(schema, table_status, etc)
 */
trait MetadataProcessOperation {
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
 * An utility that run the command with audit log
 */
trait Auditable {
  // operation id that will be written in audit log
  private val operationId: String = String.valueOf(System.nanoTime())

  // extra info to be written in audit log, set by subclass of AtomicRunnableCommand
  var auditInfo: Map[String, String] = _

  // holds the dbName and tableName for which this command is executed for
  // used for audit log, set by subclass of AtomicRunnableCommand
  private var table: String = _

  // implement by subclass, return the operation name that record in audit log
  protected def opName: String

  protected def opTime(startTime: Long) = s"${System.currentTimeMillis() - startTime} ms"

  protected def setAuditTable(dbName: String, tableName: String): Unit =
    table = s"$dbName.$tableName"

  protected def setAuditTable(carbonTable: CarbonTable): Unit =
    table = s"${carbonTable.getDatabaseName}.${carbonTable.getTableName}"

  protected def setAuditInfo(map: Map[String, String]): Unit = auditInfo = map

  /**
   * Run the passed command and record the audit log.
   * Two audit log will be output, one for operation start another for operation success/failure
   * @param runCmd command to run
   * @param spark session
   * @return command result
   */
  protected def runWithAudit(runCmd: (SparkSession => Seq[Row]), spark: SparkSession): Seq[Row] = {
    val start = System.currentTimeMillis()
    Auditor.logOperationStart(opName, operationId)
    val rows = try {
      runCmd(spark)
    } catch {
      case e: Throwable =>
        val map = Map("Exception" -> e.getClass.getName, "Message" -> e.getMessage)
        Auditor.logOperationEnd(opName, operationId, false, table, opTime(start), map.asJava)
        throw e
    }
    Auditor.logOperationEnd(opName, operationId, true, table, opTime(start),
      if (auditInfo != null) auditInfo.asJava else new java.util.HashMap[String, String]())
    rows
  }
}

/**
 * Command that modifies metadata(schema, table_status, etc) only without processing data
 */
abstract class MetadataCommand
  extends RunnableCommand with MetadataProcessOperation with Auditable {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    runWithAudit(processMetadata, sparkSession)
  }
}

/**
 * Command that process data only without modifying metadata
 */
abstract class DataCommand extends RunnableCommand with DataProcessOperation with Auditable {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    runWithAudit(processData, sparkSession)
  }
}

/**
 * Subclass of this command is executed in an atomic fashion, either all or nothing.
 * Subclass need to process both metadata and data, processMetadata should be undoable
 * if process data failed.
 */
abstract class AtomicRunnableCommand
  extends RunnableCommand with MetadataProcessOperation with DataProcessOperation with Auditable {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    runWithAudit(spark => {
      processMetadata(spark)
      try {
        processData(spark)
      } catch {
        case e: Exception =>
          undoMetadata(spark, e)
          throw e
      }
    }, sparkSession)
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
