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
package org.apache.spark.sql.execution.command.preaaggregate

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command._

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.exception.InvalidConfigurationException
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.TableInfo
import org.apache.carbondata.core.util.CarbonUtil

/**
 * Below command class will be used to create pre-aggregate table
 * and updating the parent table about the child table information
 * Failure case:
 * 1. failed to create pre aggregate table.
 * 2. failed to update main table
 *
 * @param cm
 * @param dataFrame
 * @param createDSTable
 * @param queryString
 */
case class CreatePreAggregateTableCommand(
    cm: TableModel,
    dataFrame: DataFrame,
    createDSTable: Boolean = true,
    queryString: String,
    fieldRelationMap: scala.collection.mutable.LinkedHashMap[Field, DataMapField])
  extends RunnableCommand with SchemaProcessCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    processSchema(sparkSession)
  }

  override def processSchema(sparkSession: SparkSession): Seq[Row] = {
    val storePath = CarbonEnv.getInstance(sparkSession).storePath
    CarbonEnv.getInstance(sparkSession).carbonMetastore.
      checkSchemasModifiedTimeAndReloadTables(storePath)
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    cm.databaseName = GetDB.getDatabaseName(cm.databaseNameOp, sparkSession)
    val tbName = cm.tableName
    val dbName = cm.databaseName
    LOGGER.audit(s"Creating Table with Database name [$dbName] and Table name [$tbName]")
    // getting the parent table
    val parentTable = PreAggregateUtil.getParentCarbonTable(dataFrame.logicalPlan)
    // getting the table name
    val parentTableName = parentTable.getFactTableName
    // getting the db name of parent table
    val parentDbName = parentTable.getDatabaseName
    // updating the relation identifier, this will be stored in child table
    // which can be used during dropping of pre-aggreate table as parent table will
    // also get updated
    cm.parentTable = Some(parentTable)
    cm.dataMapRelation = Some(fieldRelationMap)
    val tableInfo: TableInfo = TableNewProcessor(cm)
    // Add validation for sort scope when create table
    val sortScope = tableInfo.getFactTable.getTableProperties
      .getOrDefault("sort_scope", CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
    if (!CarbonUtil.isValidSortOption(sortScope)) {
      throw new InvalidConfigurationException(
        s"Passing invalid SORT_SCOPE '$sortScope', valid SORT_SCOPE are 'NO_SORT', 'BATCH_SORT'," +
        s" 'LOCAL_SORT' and 'GLOBAL_SORT' ")
    }

    if (tableInfo.getFactTable.getListOfColumns.size <= 0) {
      sys.error("No Dimensions found. Table should have at least one dimesnion !")
    }

    if (sparkSession.sessionState.catalog.listTables(dbName)
      .exists(_.table.equalsIgnoreCase(tbName))) {
      if (!cm.ifNotExistsSet) {
        LOGGER.audit(
          s"Table creation with Database name [$dbName] and Table name [$tbName] failed. " +
          s"Table [$tbName] already exists under database [$dbName]")
        sys.error(s"Table [$tbName] already exists under database [$dbName]")
      }
    } else {
      val tableIdentifier = AbsoluteTableIdentifier.from(storePath, dbName, tbName)
      // Add Database to catalog and persist
      val catalog = CarbonEnv.getInstance(sparkSession).carbonMetastore
      val tablePath = tableIdentifier.getTablePath
      val carbonSchemaString = catalog.generateTableSchemaString(tableInfo, tablePath)
      if (createDSTable) {
        try {
          val fields = new Array[Field](cm.dimCols.size + cm.msrCols.size)
          cm.dimCols.foreach(f => fields(f.schemaOrdinal) = f)
          cm.msrCols.foreach(f => fields(f.schemaOrdinal) = f)
          sparkSession.sql(
            s"""CREATE TABLE $dbName.$tbName
               |(${ fields.map(f => f.rawSchema).mkString(",") })
               |USING org.apache.spark.sql.CarbonSource""".stripMargin +
            s""" OPTIONS (tableName "$tbName", dbName "$dbName", tablePath """.stripMargin +
            s""""$tablePath"$carbonSchemaString) """)
          // child schema object which will be updated on parent table about the
          val childSchema = tableInfo.getFactTable
            .buildChildSchema("", tableInfo.getDatabaseName, queryString, "AGGREGATION")
          // upadting the parent table about child table
          PreAggregateUtil.updateMainTable(parentDbName, parentTableName, childSchema, sparkSession)
        } catch {
          case e: Exception =>
            val identifier: TableIdentifier = TableIdentifier(tbName, Some(dbName))
            // call the drop table to delete the created table.
            CarbonEnv.getInstance(sparkSession).carbonMetastore
              .dropTable(tablePath, identifier)(sparkSession)
            LOGGER.audit(s"Table creation with Database name [$dbName] " +
                         s"and Table name [$tbName] failed")
            throw e
        }
      }

      LOGGER.audit(s"Table created with Database name [$dbName] and Table name [$tbName]")
    }
    Seq.empty
  }
}
