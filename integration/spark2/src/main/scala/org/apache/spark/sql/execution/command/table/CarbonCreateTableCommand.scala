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
import org.apache.spark.sql.execution.SQLExecution.EXECUTION_ID_KEY
import org.apache.spark.sql.execution.command.{Field, MetadataCommand, TableModel, TableNewProcessor}
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.exception.InvalidConfigurationException
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.events.{CreateTablePostExecutionEvent, CreateTablePreExecutionEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.spark.util.CarbonSparkUtil

case class CarbonCreateTableCommand(
    tableInfo: TableInfo,
    ifNotExistsSet: Boolean = false,
    tableLocation: Option[String] = None,
    createDSTable: Boolean = true)
  extends MetadataCommand {

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val tableName = tableInfo.getFactTable.getTableName
    var databaseOpt : Option[String] = None
    if(tableInfo.getDatabaseName != null) {
      databaseOpt = Some(tableInfo.getDatabaseName)
    }
    val dbName = CarbonEnv.getDatabaseName(databaseOpt)(sparkSession)
    // set dbName and tableUnique Name in the table info
    tableInfo.setDatabaseName(dbName)
    tableInfo.setTableUniqueName(CarbonTable.buildUniqueName(dbName, tableName))
    LOGGER.audit(s"Creating Table with Database name [$dbName] and Table name [$tableName]")

    if (sparkSession.sessionState.catalog.listTables(dbName)
      .exists(_.table.equalsIgnoreCase(tableName))) {
      if (!ifNotExistsSet) {
        LOGGER.audit(
          s"Table creation with Database name [$dbName] and Table name [$tableName] failed. " +
          s"Table [$tableName] already exists under database [$dbName]")
        throw new TableAlreadyExistsException(dbName, tableName)
      }
    } else {
      val tablePath = tableLocation.getOrElse(
        CarbonEnv.getTablePath(Some(dbName), tableName)(sparkSession))
      tableInfo.setTablePath(tablePath)
      val tableIdentifier = AbsoluteTableIdentifier.from(tablePath, dbName, tableName)

      // Add validation for sort scope when create table
      val sortScope = tableInfo.getFactTable.getTableProperties.asScala
        .getOrElse("sort_scope", CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
      if (!CarbonUtil.isValidSortOption(sortScope)) {
        throw new InvalidConfigurationException(
          s"Passing invalid SORT_SCOPE '$sortScope', valid SORT_SCOPE are 'NO_SORT'," +
          s" 'BATCH_SORT', 'LOCAL_SORT' and 'GLOBAL_SORT' ")
      }

      if (tableInfo.getFactTable.getListOfColumns.size <= 0) {
        CarbonException.analysisException("Table should have at least one column.")
      }

      val operationContext = new OperationContext
      val createTablePreExecutionEvent: CreateTablePreExecutionEvent =
        CreateTablePreExecutionEvent(sparkSession, tableIdentifier, Some(tableInfo))
      OperationListenerBus.getInstance.fireEvent(createTablePreExecutionEvent, operationContext)
      val catalog = CarbonEnv.getInstance(sparkSession).carbonMetastore
      val carbonSchemaString = catalog.generateTableSchemaString(tableInfo, tableIdentifier)
      if (createDSTable) {
        try {
          val tablePath = tableIdentifier.getTablePath
          val carbonRelation = CarbonSparkUtil.createCarbonRelation(tableInfo, tablePath)
          val rawSchema = CarbonSparkUtil.getRawSchema(carbonRelation)
          sparkSession.sparkContext.setLocalProperty(EXECUTION_ID_KEY, null)
          val partitionInfo = tableInfo.getFactTable.getPartitionInfo
          val partitionString =
            if (partitionInfo != null &&
                partitionInfo.getPartitionType == PartitionType.NATIVE_HIVE) {
              s" PARTITIONED BY (${partitionInfo.getColumnSchemaList.asScala.map(
                _.getColumnName).mkString(",")})"
            } else {
              ""
            }
          sparkSession.sql(
            s"""CREATE TABLE $dbName.$tableName
               |(${ rawSchema })
               |USING org.apache.spark.sql.CarbonSource
               |OPTIONS (
               |  tableName "$tableName",
               |  dbName "$dbName",
               |  tablePath "$tablePath",
               |  path "$tablePath"
               |  $carbonSchemaString)
               |  $partitionString
             """.stripMargin)
        } catch {
          case e: AnalysisException => throw e
          case e: Exception =>
            // call the drop table to delete the created table.
            CarbonEnv.getInstance(sparkSession).carbonMetastore
              .dropTable(tableIdentifier)(sparkSession)

            val msg = s"Create table'$tableName' in database '$dbName' failed."
            LOGGER.audit(msg)
            LOGGER.error(e, msg)
            CarbonException.analysisException(msg)
        }
      }
      val createTablePostExecutionEvent: CreateTablePostExecutionEvent =
        CreateTablePostExecutionEvent(sparkSession, tableIdentifier)
      OperationListenerBus.getInstance.fireEvent(createTablePostExecutionEvent, operationContext)
      LOGGER.audit(s"Table created with Database name [$dbName] and Table name [$tableName]")
    }
    Seq.empty
  }
}
