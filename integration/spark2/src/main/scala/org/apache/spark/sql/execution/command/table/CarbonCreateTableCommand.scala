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

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession, _}
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.MetadataCommand
import org.apache.spark.util.SparkUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.InvalidConfigurationException
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.util.{CarbonUtil, ThreadLocalSessionInfo}
import org.apache.carbondata.events.{CreateTablePostExecutionEvent, CreateTablePreExecutionEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.spark.util.CarbonSparkUtil

case class CarbonCreateTableCommand(
    tableInfo: TableInfo,
    ifNotExistsSet: Boolean = false,
    tableLocation: Option[String] = None,
    isExternal : Boolean = false,
    createDSTable: Boolean = true,
    isVisible: Boolean = true)
  extends MetadataCommand {

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val tableName = tableInfo.getFactTable.getTableName
    var databaseOpt : Option[String] = None
    ThreadLocalSessionInfo
      .setConfigurationToCurrentThread(sparkSession.sessionState.newHadoopConf())
    if (tableInfo.getDatabaseName != null) {
      databaseOpt = Some(tableInfo.getDatabaseName)
    }
    val dbName = CarbonEnv.getDatabaseName(databaseOpt)(sparkSession)
    setAuditTable(dbName, tableName)
    setAuditInfo(tableInfo.getFactTable.getTableProperties.asScala.toMap
                 ++ Map("external" -> isExternal.toString))
    // set dbName and tableUnique Name in the table info
    tableInfo.setDatabaseName(dbName)
    tableInfo.setTableUniqueName(CarbonTable.buildUniqueName(dbName, tableName))
    val isTransactionalTable = tableInfo.isTransactionalTable
    if (sparkSession.sessionState.catalog.listTables(dbName)
      .exists(_.table.equalsIgnoreCase(tableName))) {
      if (!ifNotExistsSet) {
        throw new TableAlreadyExistsException(dbName, tableName)
      }
    } else {
      val path = tableLocation.getOrElse(
        CarbonEnv.getTablePath(Some(dbName), tableName)(sparkSession))
      val tablePath = if (FileFactory.getCarbonFile(path).exists() && !isExternal &&
                          isTransactionalTable && tableLocation.isEmpty) {
        path + "_" + tableInfo.getFactTable.getTableId
      } else {
        path
      }
      val streaming = tableInfo.getFactTable.getTableProperties.get("streaming")
      if (streaming != null && streaming.equalsIgnoreCase("true") && path.startsWith("s3")) {
        throw new UnsupportedOperationException("streaming is not supported with s3 store")
      }
      tableInfo.setTablePath(tablePath)
      val tableIdentifier = AbsoluteTableIdentifier
        .from(tablePath, dbName, tableName, tableInfo.getFactTable.getTableId)

      // Add validation for sort scope when create table
      val sortScope = tableInfo.getFactTable.getTableProperties.asScala
        .getOrElse("sort_scope", CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
      if (!CarbonUtil.isValidSortOption(sortScope)) {
        throw new InvalidConfigurationException(
          s"Passing invalid SORT_SCOPE '$sortScope', valid SORT_SCOPE are 'NO_SORT'," +
          s" 'BATCH_SORT', 'LOCAL_SORT' and 'GLOBAL_SORT' ")
      }

      if (tableInfo.getFactTable.getListOfColumns.size <= 0) {
        throwMetadataException(dbName, tableName, "Table should have at least one column.")
      }

      // Add validatation for column compressor when create table
      val columnCompressor = tableInfo.getFactTable.getTableProperties.get(
        CarbonCommonConstants.COMPRESSOR)
      try {
        if (null != columnCompressor) {
          CompressorFactory.getInstance().getCompressor(columnCompressor)
        }
      } catch {
        case ex : UnsupportedOperationException =>
          throw new InvalidConfigurationException(ex.getMessage)
      }

      val operationContext = new OperationContext
      val createTablePreExecutionEvent: CreateTablePreExecutionEvent =
        CreateTablePreExecutionEvent(sparkSession, tableIdentifier, Some(tableInfo))
      OperationListenerBus.getInstance.fireEvent(createTablePreExecutionEvent, operationContext)
      val catalog = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      val carbonSchemaString = catalog.generateTableSchemaString(tableInfo, tableIdentifier)
      if (createDSTable) {
        try {
          val tablePath = tableIdentifier.getTablePath
          val carbonRelation = CarbonSparkUtil.createCarbonRelation(tableInfo, tablePath)
          val rawSchema = CarbonSparkUtil.getRawSchema(carbonRelation)
          SparkUtil.setNullExecutionId(sparkSession)
          val partitionInfo = tableInfo.getFactTable.getPartitionInfo
          val partitionString =
            if (partitionInfo != null &&
                partitionInfo.getPartitionType == PartitionType.NATIVE_HIVE) {
              // Restrict dictionary encoding on partition columns.
              // TODO Need to decide whether it is required
              val dictionaryOnPartitionColumn =
              partitionInfo.getColumnSchemaList.asScala.exists{p =>
                p.hasEncoding(Encoding.DICTIONARY) && !p.hasEncoding(Encoding.DIRECT_DICTIONARY)
              }
              if (dictionaryOnPartitionColumn) {
                throwMetadataException(
                  dbName,
                  tableName,
                  s"Dictionary include cannot be applied on partition columns")
              }
              s" PARTITIONED BY (${partitionInfo.getColumnSchemaList.asScala.map(
                _.getColumnName).mkString(",")})"
            } else {
              ""
            }

          // synchronized to prevent concurrently creation of table with same name
          CarbonCreateTableCommand.synchronized {
            // isVisible property is added to hive table properties to differentiate between main
            // table and datamaps(like preaggregate). It is false only for datamaps. This is added
            // to improve the show tables performance when filtering the datamaps from main tables
            sparkSession.sql(
              s"""CREATE TABLE $dbName.$tableName
                 |(${ rawSchema })
                 |USING org.apache.spark.sql.CarbonSource
                 |OPTIONS (
                 |  tableName "$tableName",
                 |  dbName "$dbName",
                 |  tablePath "$tablePath",
                 |  path "${FileFactory.addSchemeIfNotExists(tablePath)}",
                 |  isExternal "$isExternal",
                 |  isTransactional "$isTransactionalTable",
                 |  isVisible "$isVisible"
                 |  $carbonSchemaString)
                 |  $partitionString
             """.stripMargin)
          }
        } catch {
          case e: AnalysisException =>
            // AnalysisException thrown with table already exists msg incase of conurrent drivers
            if (e.getMessage().contains("already exists")) {

              // Clear the cache first
              CarbonEnv.getInstance(sparkSession).carbonMetaStore
                .removeTableFromMetadata(dbName, tableName)

              // Delete the folders created by this call if the actual path is different
              val actualPath = CarbonEnv
                .getCarbonTable(TableIdentifier(tableName, Option(dbName)))(sparkSession)
                .getTablePath

              if (!actualPath.equalsIgnoreCase(tablePath)) {
                LOGGER
                  .error(
                    "TableAlreadyExists with path : " + actualPath + " So, deleting " + tablePath)
                FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(tablePath))
              }

              // No need to throw for create if not exists
              if (ifNotExistsSet) {
                LOGGER.error(e, e)
              } else {
                LOGGER.error(e)
                throw e
              }
            } else {
              LOGGER.error(e)
              throw e
            }

          case e: Exception =>
            // call the drop table to delete the created table.
            try {
              CarbonEnv.getInstance(sparkSession).carbonMetaStore
                .dropTable(tableIdentifier)(sparkSession)
            } catch {
              case _: Exception => // No operation
            }
            val msg = s"Create table'$tableName' in database '$dbName' failed"
            throwMetadataException(dbName, tableName, msg.concat(", ").concat(e.getMessage))
        }
      }
      val createTablePostExecutionEvent: CreateTablePostExecutionEvent =
        CreateTablePostExecutionEvent(sparkSession, tableIdentifier)
      OperationListenerBus.getInstance.fireEvent(createTablePostExecutionEvent, operationContext)
    }
    Seq.empty
  }

  override protected def opName: String = "CREATE TABLE"
}
