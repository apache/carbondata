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

package org.apache.carbondata.dictionary

import java.util.UUID

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import org.apache.spark.sql.DataFrame

import org.apache.carbondata.cardinality.CardinalityMatrix
import org.apache.carbondata.common.CarbonToolConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.SchemaEvolution
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo, TableSchema}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata, CarbonTableIdentifier}
import org.apache.carbondata.core.util.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.core.writer.ThriftWriter

trait CarbonTableUtil {

  val dictionaryGenerator: DictionaryGenerator

  /**
   * This method creates Dictionary from Input Data
   *
   * @param cardinalityMatrix
   * @param dataFrame
   * @return
   */
  def createDictionary(cardinalityMatrix: List[CardinalityMatrix],
      dataFrame: DataFrame): String = {
    val (carbonTable, absoluteTableIdentifier) = createCarbonTableMeta(cardinalityMatrix, dataFrame)
    dictionaryGenerator.writeDictionary(carbonTable, cardinalityMatrix, absoluteTableIdentifier)
    absoluteTableIdentifier.getStorePath
  }

  /**
   * Creates Metastore for Dictionary Creation
   *
   * @param cardinalityMatrix
   * @param dataFrame
   * @return
   */
  private def createCarbonTableMeta(cardinalityMatrix: List[CardinalityMatrix],
      dataFrame: DataFrame): (CarbonTable, AbsoluteTableIdentifier) = {
    val tableInfo: TableInfo = new TableInfo()
    val tableSchema: TableSchema = new TableSchema()
    val absoluteTableIdentifier: AbsoluteTableIdentifier = new AbsoluteTableIdentifier(
      dictionaryGenerator.getStorePath(),
      new CarbonTableIdentifier(CarbonToolConstants.DefaultDatabase,
        CarbonToolConstants.DefaultTable,
        UUID.randomUUID().toString))
    val columnSchemas = getColumnSchemas(cardinalityMatrix)
    tableSchema.setListOfColumns(columnSchemas)
    val schemaEvol: SchemaEvolution = new SchemaEvolution()
    schemaEvol.setSchemaEvolutionEntryList(List())
    val (schemaMetadataPath, schemaFilePath) = setTableSchemaDetails(tableSchema,
      schemaEvol,
      tableInfo,
      absoluteTableIdentifier)
    val schemaConverter = new ThriftWrapperSchemaConverterImpl()
    val thriftTableInfo = schemaConverter
      .fromWrapperToExternalTableInfo(tableInfo,
        tableInfo.getDatabaseName,
        tableInfo.getFactTable.getTableName)
    val fileType = FileFactory.getFileType(schemaMetadataPath)
    if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
      FileFactory.mkdirs(schemaMetadataPath, fileType)
    }
    val thriftWriter = new ThriftWriter(schemaFilePath, false)
    thriftWriter.open()
    thriftWriter.write(thriftTableInfo)
    thriftWriter.close()
    (CarbonMetadata.getInstance()
      .getCarbonTable(tableInfo.getTableUniqueName), absoluteTableIdentifier)
  }

  /**
   * Configures Table Schema details
   *
   * @param tableSchema
   * @param schemaEvol
   * @param tableInfo
   * @param absoluteTableIdentifier
   * @return
   */
  private def setTableSchemaDetails(tableSchema: TableSchema,
      schemaEvol: SchemaEvolution,
      tableInfo: TableInfo,
      absoluteTableIdentifier: AbsoluteTableIdentifier): (String, String) = {
    tableInfo.setStorePath(dictionaryGenerator.getStorePath())
    tableInfo.setDatabaseName(CarbonToolConstants.DefaultDatabase)
    tableSchema.setTableName(CarbonToolConstants.DefaultTable)
    tableSchema.setSchemaEvalution(schemaEvol)
    tableSchema.setTableId(UUID.randomUUID().toString)
    tableInfo.setTableUniqueName(
      absoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName +
      CarbonToolConstants.DefaultSeparator +
      absoluteTableIdentifier.getCarbonTableIdentifier.getTableName)
    tableInfo.setLastUpdatedTime(System.currentTimeMillis())
    tableInfo.setFactTable(tableSchema)
    tableInfo.setAggregateTableList(List.empty[TableSchema].asJava)
    val carbonTablePath = CarbonStorePath
      .getCarbonTablePath(absoluteTableIdentifier.getStorePath,
        absoluteTableIdentifier.getCarbonTableIdentifier)
    val schemaFilePath = carbonTablePath.getSchemaFilePath
    val schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath)
    tableInfo.setMetaDataFilepath(schemaMetadataPath)
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo)
    (schemaMetadataPath, schemaFilePath)
  }

  /**
   * This method returns list of ColumnSchema
   *
   * @param cardinalityMatrix
   * @return
   */
  private def getColumnSchemas(cardinalityMatrix: List[CardinalityMatrix]): List[ColumnSchema] = {

    val encoding = List(Encoding.DICTIONARY).asJava
    cardinalityMatrix.zipWithIndex.map { case (element, columnGroupId) =>
      val columnSchema = new ColumnSchema()
      columnSchema.setColumnName(element.columnName)
      columnSchema.setColumnar(true)
      columnSchema.setDataType(dictionaryGenerator.parseDataType(element.dataType))
      columnSchema.setEncodingList(encoding)
      columnSchema.setColumnUniqueId(element.columnName)
      columnSchema
        .setDimensionColumn(dictionaryGenerator
          .isDictionaryColumn(element.cardinality, element.dataType))
      // TODO: assign column group id to all columns
      columnSchema.setColumnGroup(columnGroupId)
      columnSchema
    }
  }
}

object CarbonTableUtil extends CarbonTableUtil {
  val dictionaryGenerator = DictionaryGenerator
}
