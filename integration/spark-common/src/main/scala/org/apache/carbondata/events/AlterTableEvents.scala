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
package org.apache.carbondata.events

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsModel, AlterTableDataTypeChangeModel, AlterTableDropColumnModel, AlterTableRenameModel}

import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.processing.loading.model.CarbonLoadModel

/**
 *
 * @param carbonTable
 * @param alterTableDropColumnModel
 * @param sparkSession
 */
case class AlterTableDropColumnPreEvent(carbonTable: CarbonTable,
    alterTableDropColumnModel: AlterTableDropColumnModel,
    sparkSession: SparkSession) extends Event with AlterTableDropColumnEventInfo


/**
 * Class for handling clean up in case of any failure and abort the operation
 *
 * @param carbonTable
 * @param alterTableDataTypeChangeModel
 */
case class AlterTableDataTypeChangePreEvent(carbonTable: CarbonTable,
        alterTableDataTypeChangeModel: AlterTableDataTypeChangeModel)
  extends Event with AlterTableDataTypeChangeEventInfo

/**
 *
 * @param carbonTable
 * @param alterTableDropColumnModel
 * @param sparkSession
 */
case class AlterTableDropColumnPostEvent(carbonTable: CarbonTable,
    alterTableDropColumnModel: AlterTableDropColumnModel,
    sparkSession: SparkSession) extends Event with AlterTableDropColumnEventInfo


/**
 *
 * @param carbonTable
 * @param alterTableDropColumnModel
 * @param sparkSession
 */
case class AlterTableDropColumnAbortEvent(carbonTable: CarbonTable,
    alterTableDropColumnModel: AlterTableDropColumnModel,
    sparkSession: SparkSession) extends Event with AlterTableDropColumnEventInfo


/**
 *
 * @param carbonTable
 * @param alterTableRenameModel
 * @param newTablePath
 * @param sparkSession
 */
case class AlterTableRenamePreEvent(carbonTable: CarbonTable,
    alterTableRenameModel: AlterTableRenameModel, newTablePath: String,
    sparkSession: SparkSession) extends Event with AlterTableRenameEventInfo

/**
 *
 * @param carbonTable
 * @param alterTableAddColumnsModel
 */
case class AlterTableAddColumnPreEvent(carbonTable: CarbonTable,
    alterTableAddColumnsModel: AlterTableAddColumnsModel)
  extends Event with AlterTableAddColumnEventInfo


/**
 *
 * @param carbonTable
 * @param alterTableRenameModel
 * @param newTablePath
 * @param sparkSession
 */
case class AlterTableRenamePostEvent(carbonTable: CarbonTable,
    alterTableRenameModel: AlterTableRenameModel, newTablePath: String,
    sparkSession: SparkSession) extends Event with AlterTableRenameEventInfo


/**
 *
 * @param carbonTable
 * @param alterTableRenameModel
 * @param newTablePath
 * @param sparkSession
 */
case class AlterTableRenameAbortEvent(carbonTable: CarbonTable,
    alterTableRenameModel: AlterTableRenameModel, newTablePath: String,
    sparkSession: SparkSession) extends Event with AlterTableRenameEventInfo


/**
 *
 * @param carbonTable
 * @param carbonLoadModel
 * @param mergedLoadName
 * @param sQLContext
 */
case class AlterTableCompactionPreEvent(carbonTable: CarbonTable,
    carbonLoadModel: CarbonLoadModel,
    mergedLoadName: String,
    sQLContext: SQLContext) extends Event with AlterTableCompactionEventInfo


/**
 *
 * @param carbonTable
 * @param carbonLoadModel
 * @param mergedLoadName
 * @param sQLContext
 */
case class AlterTableCompactionPostEvent(carbonTable: CarbonTable,
    carbonLoadModel: CarbonLoadModel,
    mergedLoadName: String,
    sQLContext: SQLContext) extends Event with AlterTableCompactionEventInfo


/**
 * Class for handling clean up in case of any failure and abort the operation
 *
 * @param carbonTable
 * @param carbonLoadModel
 * @param mergedLoadName
 * @param sQLContext
 */
case class AlterTableCompactionAbortEvent(carbonTable: CarbonTable,
    carbonLoadModel: CarbonLoadModel,
    mergedLoadName: String,
    sQLContext: SQLContext) extends Event with AlterTableCompactionEventInfo
