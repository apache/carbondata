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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.CarbonParserUtil.initializeSpatialIndexInstance
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.execution.command.{AlterTableAddPartitionCommand, MetadataCommand}
import org.apache.spark.sql.execution.command.table.CarbonCreateTableCommand
import org.apache.spark.util.{AlterTableUtil, SparkUtil}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, SegmentFileStore}
import org.apache.carbondata.core.metadata.schema.SchemaReader
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.metadata.schema.table.TableInfo
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{withEvents, RefreshTablePostExecutionEvent, RefreshTablePreExecutionEvent}

/**
 * Command to register carbon table from existing carbon table data
 */
case class RefreshCarbonTableCommand(
    databaseNameOp: Option[String],
    tableName: String)
  extends MetadataCommand {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val databaseName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    setAuditTable(databaseName, tableName)
    // Steps
    // 1. Get Table Metadata from spark.
    // 2 Perform below steps:
    // 2.1 If table exists then check if provider if carbon. If yes then go for carbon
    // refresh otherwise no need to do anything.
    // 2.1.1 If table does not exists then consider the table as carbon and check for schema file
    // existence.
    // 2.2 register the table with the hive check if the table being registered has aggregate table
    // then do the below steps
    // 2.2.1 validate that all the aggregate tables are copied at the store location.
    // 2.2.2 Register the aggregate tables
    // 2.1 check if the table already register with hive then ignore and continue with the next
    // schema
    val isCarbonDataSource = try {
      CarbonSource.isCarbonDataSource(sparkSession.sessionState.catalog
        .getTableMetadata(TableIdentifier(tableName, databaseNameOp)))
    } catch {
      case _: NoSuchTableException =>
        true
    }
    if (isCarbonDataSource) {
      val tablePath = CarbonEnv.getTablePath(databaseNameOp, tableName.toLowerCase)(sparkSession)
      val identifier = AbsoluteTableIdentifier.from(tablePath, databaseName, tableName.toLowerCase)
      // check the existence of the schema file to know its a carbon table
      val schemaFilePath = CarbonTablePath.getSchemaFilePath(identifier.getTablePath)
      // if schema file does not exist then the table will either non carbon table or stale
      // carbon table
      if (FileFactory.isFileExist(schemaFilePath)) {
        // read TableInfo
        val tableInfo = SchemaReader.getTableInfo(identifier)
        val tableProperties = tableInfo.getFactTable.getTableProperties
        val spatialIndex = CarbonCommonConstants.SPATIAL_INDEX
        val indexName = tableProperties.get(spatialIndex)
        if (indexName != null) {
          val SPATIAL_INDEX_CLASS = s"$spatialIndex.$indexName.class"
          val SPATIAL_INDEX_INSTANCE = s"$spatialIndex.$indexName.instance"
          // For spatial table, To make the instance compatible with previous versions,
          // initialise and update the index instance in table properties.
          tableProperties.remove(SPATIAL_INDEX_INSTANCE)
          initializeSpatialIndexInstance(tableProperties.get(SPATIAL_INDEX_CLASS),
            indexName, tableProperties.asScala)
          val tableIdentifier = new TableIdentifier(tableName, Some(tableInfo.getDatabaseName))
          if (sparkSession.sessionState.catalog.tableExists(tableIdentifier)) {
            // In direct upgrade scenario, if spatial table already exists then on refresh command,
            // update the property in metadata and fail table creation.
            LOGGER.info(s"Updating $SPATIAL_INDEX_INSTANCE table property on $tableName")
            AlterTableUtil.modifyTableProperties(tableIdentifier,
              Map(SPATIAL_INDEX_INSTANCE -> tableProperties.get(SPATIAL_INDEX_INSTANCE)),
              Seq.empty, true)(sparkSession, sparkSession.sessionState.catalog)
          }
        }
        // remove mv related info from source table properties
        tableInfo.getFactTable
          .getTableProperties.remove(CarbonCommonConstants.RELATED_MV_TABLES_MAP)
        // refresh the column schema in case of store before V3
        refreshColumnSchema(tableInfo)

        // 2.2 register the table with the hive
        registerTableWithHive(databaseName, tableName, tableInfo, tablePath)(sparkSession)
        // Register partitions to hive metastore in case of hive partitioning carbon table
        if (tableInfo.getFactTable.getPartitionInfo != null &&
            tableInfo.getFactTable.getPartitionInfo.getPartitionType == PartitionType.NATIVE_HIVE) {
          registerAllPartitionsToHive(identifier, sparkSession)
        }
      }
    }
    CarbonToSparkAdapter.createRefreshTableCommand(TableIdentifier(tableName, Option(databaseName)))
      .run(sparkSession)
  }

  /**
   * Refresh the sort_column flag in column schema in case of old store. Before V3, sort_column
   * option is not set but by default all dimension columns should be treated
   * as sort columns if SORT_COLUMNS property is not defined in table properties
   *
   * @param tableInfo
   */
  def refreshColumnSchema(tableInfo: TableInfo): Unit = {
    val tableProps: mutable.Map[String, String] = tableInfo.getFactTable.getTableProperties.asScala
    val sortColumns = tableProps.get(CarbonCommonConstants.SORT_COLUMNS)
    sortColumns match {
      case Some(sortColumn) =>
      // don't do anything
      case None =>
        // iterate over all the columns and make all the dimensions as sort columns true
        // check for the complex data types parent and child columns to
        // avoid adding them in SORT_COLUMNS
        tableInfo.getFactTable.getListOfColumns.asScala collect
        ({
          case columnSchema if columnSchema.isDimensionColumn &&
                               !columnSchema.getDataType.isComplexType &&
                               columnSchema.getSchemaOrdinal != -1 =>
            columnSchema.setSortColumn(true)
        })
    }
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
    var allowCreateTableNonEmptyLocation: String = null
    val allowCreateTableNonEmptyLocationConf =
      "spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation"
    try {
      if (sparkSession.sessionState.conf.contains(allowCreateTableNonEmptyLocationConf) &&
          SparkUtil.isSparkVersionXAndAbove("2.4")) {
        // During refresh table, when this option is set to true, creating managed tables with
        // nonempty location is allowed. Otherwise, an analysis exception is thrown.
        // https://kb.databricks.com/jobs/spark-overwrite-cancel.html
        allowCreateTableNonEmptyLocation = sparkSession.sessionState
          .conf.getConfString(allowCreateTableNonEmptyLocationConf)
        sparkSession.sessionState.conf.setConfString(allowCreateTableNonEmptyLocationConf, "true")
      }
      val tableIdentifier = tableInfo.getOrCreateAbsoluteTableIdentifier()
      withEvents(RefreshTablePreExecutionEvent(sparkSession, tableIdentifier),
        RefreshTablePostExecutionEvent(sparkSession, tableIdentifier)) {
        CarbonCreateTableCommand(tableInfo, ifNotExistsSet = false, tableLocation = Some(tablePath))
          .run(sparkSession)
      }
    } catch {
      case e: AnalysisException => throw e
      case e: Exception => throw e
    } finally {
      if (SparkUtil.isSparkVersionXAndAbove("2.4") && null != allowCreateTableNonEmptyLocation) {
        // Set it back to default
        sparkSession.sessionState.conf
          .setConfString(allowCreateTableNonEmptyLocationConf, allowCreateTableNonEmptyLocation)
      }
    }
  }

  /**
   * Read all the partition information which is stored in each segment and add to
   * the hive metastore
   */
  private def registerAllPartitionsToHive(
      absIdentifier: AbsoluteTableIdentifier,
      sparkSession: SparkSession): Unit = {
    val metadataDetails =
      SegmentStatusManager.readLoadMetadata(
        CarbonTablePath.getMetadataPath(absIdentifier.getTablePath))
    // First read all partition information from each segment.
    val allpartitions = metadataDetails.map{ metadata =>
      if (metadata.getSegmentStatus == SegmentStatus.SUCCESS ||
          metadata.getSegmentStatus == SegmentStatus.LOAD_PARTIAL_SUCCESS) {
        val mapper = new SegmentFileStore(absIdentifier.getTablePath, metadata.getSegmentFile)
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
      } else {
        None
      }
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

  override protected def opName: String = "REFRESH TABLE"
}
