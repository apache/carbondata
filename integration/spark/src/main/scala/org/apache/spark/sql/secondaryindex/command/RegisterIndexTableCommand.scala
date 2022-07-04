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

package org.apache.spark.sql.secondaryindex.command

import java.util

import scala.collection.JavaConverters._

import org.apache.log4j.Logger
import org.apache.spark.sql.{CarbonDataCommands, CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.SchemaReader
import org.apache.carbondata.core.metadata.schema.table.TableInfo
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema

/**
 * Register index table with main table
 * 1. check if the main and index table exist
 * 2. call the create index command with isCreateSIndex = false
 * (do not create the si table in store path & avoid data load for si)
 */
case class RegisterIndexTableCommand(dbName: Option[String], indexTableName: String,
    parentTable: String)
  extends CarbonDataCommands {

  val LOGGER: Logger =
    LogServiceFactory.getLogService(this.getClass.getName)

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val databaseName = CarbonEnv.getDatabaseName(dbName)(sparkSession)
    setAuditTable(databaseName, indexTableName)
    setAuditInfo(Map("Parent TableName" -> parentTable))
    // 1. check if the main and index table exist
    if (!sparkSession.sessionState.catalog.tableExists(
      TableIdentifier(parentTable, Some(databaseName)))) {
      val message: String = s"Secondary Index Table registration for table [$indexTableName] with" +
                            s" table [$databaseName.$parentTable] failed." +
                            s"Table [$parentTable] does not exists under database [$databaseName]"
      CarbonException.analysisException(message)
    }
    if (!sparkSession.sessionState.catalog.tableExists(
      TableIdentifier(indexTableName, Some(databaseName)))) {
      val message: String = s"Secondary Index Table registration for table [$indexTableName] with" +
                            s" table [$databaseName.$parentTable] failed." +
                            s"Secondary Index Table [$indexTableName] does not exists under" +
                            s" database [$databaseName]"
      CarbonException.analysisException(message)
    }
    val indexTable = CarbonEnv.getCarbonTable(dbName, indexTableName)(sparkSession)
    // get table path from carbon table, instead of creating table path, since the SI table can
    // be renamed before register index
    val tablePath = indexTable.getTablePath
    val absoluteTableIdentifier = AbsoluteTableIdentifier.from(tablePath, databaseName,
      indexTableName)
    // 2. Read TableInfo
    val tableInfo = SchemaReader.getTableInfo(absoluteTableIdentifier)
    val columns: List[String] = getIndexColumn(tableInfo)
    val secondaryIndex = IndexModel(dbName, parentTable.toLowerCase, columns,
      indexTableName.toLowerCase)
    val properties = tableInfo.getFactTable.getTableProperties
    properties.put("tablePath", tablePath)
    // 3. Call the create index command with isCreateSIndex = false
    // (do not create the si table in store path)
    CarbonCreateSecondaryIndexCommand(
      indexModel = secondaryIndex,
      tableProperties = properties.asScala,
      ifNotExists = false,
      isDeferredRefresh = false,
      isCreateSIndex = false).run(sparkSession)
    LOGGER.info(s"Table [$indexTableName] registered as Secondary Index table with" +
                s" table [$databaseName.$parentTable] successfully.")
    Seq.empty
  }

  /**
   * The method return's the List of dimension columns excluding the positionReference dimension
   *
   * @param tableInfo TableInfo object
   * @return List[String] List of dimension column names
   */
  def getIndexColumn(tableInfo: TableInfo) : List[String] = {
    val columns: util.List[ColumnSchema] = tableInfo.getFactTable.getListOfColumns
    columns.asScala.filter(f => f.isDimensionColumn &&
                                !f.getColumnName.equalsIgnoreCase(
                                  CarbonCommonConstants.POSITION_REFERENCE)
    ).map(column => column.getColumnName.toLowerCase()).toList
  }

  override protected def opName: String = "Register Index Table"

}
