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
import org.apache.spark.sql.parser.CarbonSparkSqlParserUtil
import org.apache.spark.util.SparkUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.util.ThreadLocalSessionInfo
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
      val tablePath = CarbonEnv.createTablePath(
        Some(dbName),
        tableName,
        tableInfo.getFactTable.getTableId,
        tableLocation,
        isExternal,
        isTransactionalTable
      )(sparkSession)
      tableInfo.setTablePath(tablePath)
      CarbonSparkSqlParserUtil.validateTableProperties(tableInfo)
      val tableIdentifier = AbsoluteTableIdentifier
        .from(tablePath, dbName, tableName, tableInfo.getFactTable.getTableId)
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
              s" PARTITIONED BY (${partitionInfo.getColumnSchemaList.asScala.map(
                _.getColumnName.toLowerCase).mkString(",")})"
            } else {
              ""
            }

          // add carbon properties into option list in addition to carbon default properties
          val repeatedPropKeys =
            Seq("tablename",
              "dbname",
              "tablePath",
              "isExternal",
              "path",
              "isTransactional",
              "isVisible",
              "carbonSchemaPartsNo")
          val tableProperties =
            tableInfo
              .getFactTable
              .getTableProperties
              .asScala
              .filter(prop => !repeatedPropKeys.exists(_.equalsIgnoreCase(prop._1)))
              .map { property =>
                s"""  ${ property._1 }  "${ property._2 }","""
              }
              .mkString("\n", "\n", "")

          // synchronized to prevent concurrently creation of table with same name
          CarbonCreateTableCommand.synchronized {
            // isVisible property is added to hive table properties to differentiate between main
            // table and datamaps(like preaggregate). It is false only for datamaps. This is added
            // to improve the show tables performance when filtering the datamaps from main tables
            sparkSession.sql(
              s"""CREATE TABLE $dbName.$tableName
                 |(${ rawSchema })
                 |USING carbondata
                 |OPTIONS (${tableProperties}
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
            throw e
            val msg = s"Create table'$tableName' in database '$dbName' failed"
            throwMetadataException(dbName, tableName, s"$msg, ${e.getMessage}")
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
