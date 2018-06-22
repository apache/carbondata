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

package org.apache.spark.sql.execution.command.management

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.execution.command.{AlterTableAddPartitionCommand, MetadataCommand}
import org.apache.spark.sql.execution.command.table.CarbonCreateTableCommand

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, SegmentFileStore}
import org.apache.carbondata.core.metadata.schema.SchemaReader
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.metadata.schema.table.{DataMapSchema, TableInfo}
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.statusmanager.{SegmentManager, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{OperationContext, OperationListenerBus, RefreshTablePostExecutionEvent, RefreshTablePreExecutionEvent}

/**
 * Command to register carbon table from existing carbon table data
 */
case class RefreshCarbonTableCommand(
    databaseNameOp: Option[String],
    tableName: String)
  extends MetadataCommand {
  val LOGGER: LogService =
    LogServiceFactory.getLogService(this.getClass.getName)

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val databaseName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    // Steps
    // 1. get table path
    // 2. perform the below steps
    // 2.1 check if the table already register with hive then ignore and continue with the next
    // schema
    // 2.2 register the table with the hive check if the table being registered has aggregate table
    // then do the below steps
    // 2.2.1 validate that all the aggregate tables are copied at the store location.
    // 2.2.2 Register the aggregate tables
    val tablePath = CarbonEnv.getTablePath(databaseNameOp, tableName)(sparkSession)
    val identifier = AbsoluteTableIdentifier.from(tablePath, databaseName, tableName)
    // 2.1 check if the table already register with hive then ignore and continue with the next
    // schema
    if (!sparkSession.sessionState.catalog.listTables(databaseName)
      .exists(_.table.equalsIgnoreCase(tableName))) {
      // check the existence of the schema file to know its a carbon table
      val schemaFilePath = CarbonTablePath.getSchemaFilePath(identifier.getTablePath)
      // if schema file does not exist then the table will either non carbon table or stale
      // carbon table
      if (FileFactory.isFileExist(schemaFilePath, FileFactory.getFileType(schemaFilePath))) {
        // read TableInfo
        val tableInfo = SchemaReader.getTableInfo(identifier)
        // 2.2 register the table with the hive check if the table being registered has
        // aggregate table then do the below steps
        // 2.2.1 validate that all the aggregate tables are copied at the store location.
        val dataMapSchemaList = tableInfo.getDataMapSchemaList
        if (null != dataMapSchemaList && dataMapSchemaList.size() != 0) {
          // validate all the aggregate tables are copied at the storeLocation
          val allExists = validateAllAggregateTablePresent(databaseName,
            dataMapSchemaList, sparkSession)
          if (!allExists) {
            // fail the register operation
            val msg = s"Table registration with Database name [$databaseName] and Table name " +
                      s"[$tableName] failed. All the aggregate Tables for table [$tableName] is" +
                      s" not copied under database [$databaseName]"
            LOGGER.audit(msg)
            throwMetadataException(databaseName, tableName, msg)
          }
          // 2.2.1 Register the aggregate tables to hive
          registerAggregates(databaseName, dataMapSchemaList)(sparkSession)
        }
        registerTableWithHive(databaseName, tableName, tableInfo, tablePath)(sparkSession)
        // Register partitions to hive metastore in case of hive partitioning carbon table
        if (tableInfo.getFactTable.getPartitionInfo != null &&
            tableInfo.getFactTable.getPartitionInfo.getPartitionType == PartitionType.NATIVE_HIVE) {
          registerAllPartitionsToHive(identifier, sparkSession)
        }
      } else {
        LOGGER.audit(
          s"Table registration with Database name [$databaseName] and Table name [$tableName] " +
          s"failed." +
          s"Table [$tableName] either non carbon table or stale carbon table under database " +
          s"[$databaseName]")
      }
    } else {
      LOGGER.audit(
        s"Table registration with Database name [$databaseName] and Table name [$tableName] " +
        s"failed." +
        s"Table [$tableName] either already exists or registered under database [$databaseName]")
    }
    // update the schema modified time
    metaStore.updateAndTouchSchemasUpdatedTime()
    Seq.empty
  }

  /**
   * the method prepare the data type for raw column
   *
   * @param column
   * @return
   */
  def prepareDataType(column: ColumnSchema): String = {
    column.getDataType.getName.toLowerCase() match {
      case "decimal" =>
        "decimal(" + column.getPrecision + "," + column.getScale + ")"
      case others =>
        others
    }
  }

  /**
   * The method register the carbon table with hive
   *
   * @param dbName
   * @param tableName
   * @param tableInfo
   * @param sparkSession
   * @return
   */
  def registerTableWithHive(dbName: String,
      tableName: String,
      tableInfo: TableInfo,
      tablePath: String)(sparkSession: SparkSession): Any = {
    val operationContext = new OperationContext
    try {
      val refreshTablePreExecutionEvent: RefreshTablePreExecutionEvent =
        new RefreshTablePreExecutionEvent(sparkSession,
          tableInfo.getOrCreateAbsoluteTableIdentifier())
      OperationListenerBus.getInstance.fireEvent(refreshTablePreExecutionEvent, operationContext)
      CarbonCreateTableCommand(tableInfo, ifNotExistsSet = false, tableLocation = Some(tablePath))
        .run(sparkSession)
      LOGGER.audit(s"Table registration with Database name [$dbName] and Table name " +
                   s"[$tableName] is successful.")
    } catch {
      case e: AnalysisException => throw e
      case e: Exception =>
        throw e
    }
    val refreshTablePostExecutionEvent: RefreshTablePostExecutionEvent =
      new RefreshTablePostExecutionEvent(sparkSession,
        tableInfo.getOrCreateAbsoluteTableIdentifier())
    OperationListenerBus.getInstance.fireEvent(refreshTablePostExecutionEvent, operationContext)
  }

  /**
   * The method validate that all the aggregate table are physically present
   *
   * @param dataMapSchemaList
   * @param sparkSession
   */
  def validateAllAggregateTablePresent(dbName: String, dataMapSchemaList: util.List[DataMapSchema],
      sparkSession: SparkSession): Boolean = {
    var fileExist = false
    dataMapSchemaList.asScala.foreach(dataMap => {
      val tableName = dataMap.getChildSchema.getTableName
      val tablePath = CarbonEnv.getTablePath(Some(dbName), tableName)(sparkSession)
      val schemaFilePath = CarbonTablePath.getSchemaFilePath(tablePath)
      try {
        fileExist = FileFactory.isFileExist(schemaFilePath, FileFactory.getFileType(schemaFilePath))
      } catch {
        case e: Exception =>
          fileExist = false
      }
      if (!fileExist) {
        return fileExist;
      }
    })
    true
  }

  /**
   * The method iterates over all the aggregate tables and register them to hive
   *
   * @param dataMapSchemaList
   * @return
   */
  def registerAggregates(dbName: String,
      dataMapSchemaList: util.List[DataMapSchema])(sparkSession: SparkSession): Any = {
    dataMapSchemaList.asScala.foreach(dataMap => {
      val tableName = dataMap.getChildSchema.getTableName
      if (!sparkSession.sessionState.catalog.listTables(dbName)
        .exists(_.table.equalsIgnoreCase(tableName))) {
        val tablePath = CarbonEnv.getTablePath(Some(dbName), tableName)(sparkSession)
        val absoluteTableIdentifier = AbsoluteTableIdentifier
          .from(tablePath, dbName, tableName)
        val tableInfo = SchemaReader.getTableInfo(absoluteTableIdentifier)
        registerTableWithHive(dbName, tableName, tableInfo, tablePath)(sparkSession)
      }
    })
  }

  /**
   * Read all the partition information which is stored in each segment and add to
   * the hive metastore
   */
  private def registerAllPartitionsToHive(
      absIdentifier: AbsoluteTableIdentifier,
      sparkSession: SparkSession): Unit = {
    val metadataDetails =
      new SegmentManager().getValidSegments(absIdentifier).getValidSegments.asScala
    // First read all partition information from each segment.
    val allpartitions = metadataDetails.map{ seg =>
      val mapper = new SegmentFileStore(absIdentifier.getTablePath, seg.getSegmentFileName)
      val specs = mapper.getLocationMap.asScala.map { case(location, fd) =>
        var updatedLoc =
          if (fd.isRelative) {
            absIdentifier.getTablePath + CarbonCommonConstants.FILE_SEPARATOR + location
          } else {
            location
          }
        new PartitionSpec(fd.getPartitions, updatedLoc)
      }
      Some(specs)
    }.filter(_.isDefined).map(_.get)
    val identifier =
      TableIdentifier(absIdentifier.getTableName, Some(absIdentifier.getDatabaseName))
    // Register the partition information to the hive metastore
    allpartitions.foreach { segPartitions =>
      val specs: Seq[(TablePartitionSpec, Option[String])] = segPartitions.map { indexPartitions =>
        (indexPartitions.getPartitions.asScala.map{ p =>
          val spec = p.split("=")
          (spec(0), spec(1))
        }.toMap, Some(indexPartitions.getLocation.toString))
      }.toSeq
      // Add partition information
      AlterTableAddPartitionCommand(identifier, specs, true).run(sparkSession)
    }
  }
}
