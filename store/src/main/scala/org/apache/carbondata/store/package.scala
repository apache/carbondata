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
package org.apache.carbondata.store

import scala.collection.mutable.Map

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.TableIdentifier

import org.apache.carbondata.core.metadata.{CarbonTableIdentifier, ColumnIdentifier}
import org.apache.carbondata.core.metadata.schema.PartitionInfo
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonDimension, ColumnSchema}
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentUpdateStatusManager}
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.merger.CompactionType

 /**
  * Column validator
  */
trait ColumnValidator {
  def validateColumns(columns: Seq[ColumnSchema])
}
/**
 * Dictionary related helper service
 */
trait DictionaryDetailService {
  def getDictionaryDetail(dictFolderPath: String, primDimensions: Array[CarbonDimension],
      table: CarbonTableIdentifier, storePath: String): DictionaryDetail
}

/**
 * Dictionary related detail
 */
case class DictionaryDetail(columnIdentifiers: Array[ColumnIdentifier],
    dictFilePaths: Array[String], dictFileExists: Array[Boolean])

/**
 * Factory class
 */
object CarbonSparkFactory {
  /**
   * @return column validator
   */
  def getCarbonColumnValidator: ColumnValidator = {
    new CarbonColumnValidator
  }

  /**
   * @return dictionary helper
   */
  def getDictionaryDetailService: DictionaryDetailService = {
    new DictionaryDetailHelper
  }
}

case class TableModel(
    ifNotExistsSet: Boolean,
    var databaseName: String,
    databaseNameOp: Option[String],
    tableName: String,
    tableProperties: Map[String, String],
    dimCols: Seq[Field],
    msrCols: Seq[Field],
    sortKeyDims: Option[Seq[String]],
    highcardinalitydims: Option[Seq[String]],
    noInvertedIdxCols: Option[Seq[String]],
    columnGroups: Seq[String],
    colProps: Option[java.util.Map[String,
      java.util.List[ColumnProperty]]] = None,
    bucketFields: Option[BucketFields],
    partitionInfo: Option[PartitionInfo])

case class Field(column: String, var dataType: Option[String], name: Option[String],
    children: Option[List[Field]], parent: String = null,
    storeType: Option[String] = Some("columnar"),
    var schemaOrdinal: Int = -1,
    var precision: Int = 0, var scale: Int = 0, var rawSchema: String = "")

case class ColumnProperty(key: String, value: String)

case class ComplexField(complexType: String, primitiveField: Option[Field],
    complexField: Option[ComplexField])

case class Partitioner(partitionClass: String, partitionColumn: Array[String], partitionCount: Int,
    nodeList: Array[String])

case class PartitionerField(partitionColumn: String, dataType: Option[String],
    columnComment: String)

case class BucketFields(bucketColumns: Seq[String], numberOfBuckets: Int)

case class DataLoadTableFileMapping(table: String, loadPath: String)

case class ExecutionErrors(var failureCauses: FailureCauses, var errorMsg: String )

case class CarbonMergerMapping(storeLocation: String,
    hdfsStoreLocation: String,
    metadataFilePath: String,
    var mergedLoadName: String,
    databaseName: String,
    factTableName: String,
    validSegments: Array[String],
    tableId: String,
    campactionType: CompactionType,
    // maxSegmentColCardinality is Cardinality of last segment of compaction
    var maxSegmentColCardinality: Array[Int],
    // maxSegmentColumnSchemaList is list of column schema of last segment of compaction
    var maxSegmentColumnSchemaList: List[ColumnSchema])

case class NodeInfo(TaskId: String, noOfBlocks: Int)

case class AlterTableModel(dbName: Option[String],
    tableName: String,
    segmentUpdateStatusManager: Option[SegmentUpdateStatusManager],
    compactionType: String,
    factTimeStamp: Option[Long],
    var alterSql: String)

case class UpdateTableModel(isUpdate: Boolean,
    updatedTimeStamp: Long,
    var executorErrors: ExecutionErrors)

case class CompactionModel(compactionSize: Long,
    compactionType: CompactionType,
    carbonTable: CarbonTable,
    isDDLTrigger: Boolean)

case class CompactionCallableModel(carbonLoadModel: CarbonLoadModel,
    storeLocation: String,
    carbonTable: CarbonTable,
    loadsToMerge: java.util.List[LoadMetadataDetails],
    sqlContext: SQLContext,
    compactionType: CompactionType)

case class AlterPartitionModel(carbonLoadModel: CarbonLoadModel,
    segmentId: String,
    oldPartitionIds: List[Int],
    sqlContext: SQLContext
)

case class SplitPartitionCallableModel(carbonLoadModel: CarbonLoadModel,
    segmentId: String,
    partitionId: String,
    oldPartitionIds: List[Int],
    sqlContext: SQLContext)

case class DropPartitionCallableModel(carbonLoadModel: CarbonLoadModel,
    segmentId: String,
    partitionId: String,
    oldPartitionIds: List[Int],
    dropWithData: Boolean,
    carbonTable: CarbonTable,
    sqlContext: SQLContext)

case class DataTypeInfo(dataType: String, precision: Int = 0, scale: Int = 0)

case class AlterTableDataTypeChangeModel(dataTypeInfo: DataTypeInfo,
    databaseName: Option[String],
    tableName: String,
    columnName: String,
    newColumnName: String)

case class AlterTableRenameModel(
    oldTableIdentifier: TableIdentifier,
    newTableIdentifier: TableIdentifier
)

case class AlterTableAddColumnsModel(
    databaseName: Option[String],
    tableName: String,
    tableProperties: Map[String, String],
    dimCols: Seq[Field],
    msrCols: Seq[Field],
    highCardinalityDims: Seq[String])

case class AlterTableDropColumnModel(databaseName: Option[String],
    tableName: String,
    columns: List[String])

case class AlterTableDropPartitionModel(databaseName: Option[String],
    tableName: String,
    partitionId: String,
    dropWithData: Boolean)

case class AlterTableSplitPartitionModel(databaseName: Option[String],
    tableName: String,
    partitionId: String,
    splitInfo: List[String])


