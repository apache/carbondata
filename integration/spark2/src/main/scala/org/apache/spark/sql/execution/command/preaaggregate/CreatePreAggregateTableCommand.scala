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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.management.CarbonLoadDataCommand
import org.apache.spark.sql.execution.command.table.{CarbonCreateTableCommand, CarbonDropTableCommand}
import org.apache.spark.sql.parser.CarbonSpark2SqlParser

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.datatype.{DataTypes => CarbonDataTypes}
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.path.CarbonTablePath

/**
 * Below command class will be used to create pre-aggregate table
 * and updating the parent table about the child table information
 * It will be either success or nothing happen in case of failure:
 * 1. failed to create pre aggregate table.
 * 2. failed to update main table
 *
 */
case class CreatePreAggregateTableCommand(
    dataMapName: String,
    parentTableIdentifier: TableIdentifier,
    dmClassName: String,
    dmProperties: Map[String, String],
    queryString: String)
  extends AtomicRunnableCommand {

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val updatedQuery = new CarbonSpark2SqlParser().addPreAggFunction(queryString)
    val df = sparkSession.sql(updatedQuery)
    val dataMapFields = PreAggregateUtil.validateActualSelectPlanAndGetAttributes(
      df.logicalPlan, queryString)
    val dataMapSchema = dataMapFields.map(entry => (entry._1.getFieldName, entry._2)).toMap
    val fields = dataMapSchema.keySet.map { fieldName =>
      dataMapFields.filter(_._1.getFieldName.equals(fieldName)).head._1
    }
    val tableProperties = mutable.Map[String, String]()
    dmProperties.foreach(t => tableProperties.put(t._1, t._2))
    // Create the aggregation table name with parent table name prefix
    val tableIdentifier = TableIdentifier(
        parentTableIdentifier.table +"_" + dataMapName, parentTableIdentifier.database)

    val databaseName = CarbonEnv.getDatabaseName(tableIdentifier.database)(sparkSession)
    val tableName = tableIdentifier.table

    // getting the parent table
    val parentTable = PreAggregateUtil.getParentCarbonTable(df.logicalPlan)
    // getting the table name
    val parentTableName = parentTable.getTableName
    // getting the db name of parent table
    val parentDbName = parentTable.getDatabaseName

    assert(parentTableName.equalsIgnoreCase(parentTableIdentifier.table))

    val tablePath = CarbonEnv.getTablePath(Some(databaseName), tableName)(sparkSession)

    CarbonCreateTableCommand(
      databaseNameOp = tableIdentifier.database,
      tableName = tableIdentifier.table,
      tableLocation = Some(tablePath),
      tableProperties = tableProperties,
      tableSchema = CarbonDataTypes.createStructType(fields.toSeq.asJava),
      parentTable = Some(parentTable),
      dataMapFields = Some(dataMapSchema)
    ).run(sparkSession)

    try {
      val table = CarbonEnv.getCarbonTable(tableIdentifier)(sparkSession)
      val tableInfo = table.getTableInfo

      // updating the relation identifier, this will be stored in child table
      // which can be used during dropping of pre-aggreate table as parent table will
      // also get updated
      val childSchema = tableInfo.getFactTable.buildChildSchema(
        dataMapName,
        CarbonCommonConstants.AGGREGATIONDATAMAPSCHEMA,
        tableInfo.getDatabaseName,
        queryString, "AGGREGATION")
      dmProperties.foreach(f => childSchema.getProperties.put(f._1, f._2))

      // updating the parent table about child table
      PreAggregateUtil.updateMainTable(
        parentDbName,
        parentTableName,
        childSchema,
        sparkSession)
    } catch {
      case e: Exception =>
        CarbonDropTableCommand(
          ifExistsSet = true,
          Some(databaseName),
          tableName
        ).run(sparkSession)
        throw e

    }

    Seq.empty
  }

  override def undoMetadata(sparkSession: SparkSession, exception: Exception): Seq[Row] = {
    // drop child table and undo the change in table info of main table
    CarbonDropTableCommand(
      ifExistsSet = true,
      tableIdentifier.database,
      tableIdentifier.table
    ).run(sparkSession)

    // TODO: undo the change in table info of main table

    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    // load child table if parent table has existing segments
    val dbName = CarbonEnv.getDatabaseName(parentTableIdentifier.database)(sparkSession)
    val parentCarbonTable = CarbonEnv.getCarbonTable(Some(dbName),
      parentTableIdentifier.table)(sparkSession)
    // This will be used to check if the parent table has any segments or not. If not then no
    // need to fire load for pre-aggregate table. Therefore reading the load details for PARENT
    // table.
    val loadAvailable = SegmentStatusManager.readLoadMetadata(parentCarbonTable.getMetaDataFilepath)
      .nonEmpty
    if (loadAvailable) {
      val headers = parentCarbonTable.getTableInfo.getFactTable.getListOfColumns.
        asScala.map(_.getColumnName).mkString(",")
      val childDataFrame = sparkSession.sql(
        new CarbonSpark2SqlParser().addPreAggLoadFunction(queryString))
      CarbonLoadDataCommand(tableIdentifier.database,
        tableIdentifier.table,
        null,
        Nil,
        Map("fileheader" -> headers),
        isOverwriteTable = false,
        dataFrame = Some(childDataFrame),
        internalOptions = Map(CarbonCommonConstants.IS_INTERNAL_LOAD_CALL -> "true")).
        run(sparkSession)
    }
    Seq.empty
  }

  // Create the aggregation table name with parent table name prefix
  private lazy val tableIdentifier =
    TableIdentifier(parentTableIdentifier.table + "_" + dataMapName, parentTableIdentifier.database)

}


