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

package org.apache.spark.sql.parser

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.{CarbonParserUtil, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.parser.ParserUtils.operationNotAllowed
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsModel, AlterTableDataTypeChangeModel, Field, PartitionerField, TableModel, TableNewProcessor}
import org.apache.spark.sql.execution.command.management.CarbonLoadDataCommand
import org.apache.spark.sql.execution.command.schema.{CarbonAlterTableAddColumnCommand, CarbonAlterTableColRenameDataTypeChangeCommand}
import org.apache.spark.sql.types.StructField

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.InvalidConfigurationException
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.SchemaReader
import org.apache.carbondata.core.metadata.schema.table.TableInfo
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.CarbonOption
import org.apache.carbondata.spark.exception.ProcessMetaDataException
import org.apache.carbondata.spark.util.CarbonScalaUtil

/**
 * Utility class to validate the create table and CTAS command,
 * and to prepare the logical plan of create table and CTAS command.
 */
object CarbonSparkSqlParserUtil {
  /**
   * The method validate that the property configured for the streaming attribute is valid.
   *
   * @param carbonOption Instance of CarbonOption having all the required option for datasource.
   */
  def validateStreamingProperty(carbonOption: CarbonOption): Unit = {
    try {
      carbonOption.isStreaming
    } catch {
      case _: IllegalArgumentException =>
        throw new MalformedCarbonCommandException(
          "Table property 'streaming' should be either 'true' or 'false'")
    }
  }

  def validateTableProperties(tableInfo: TableInfo): Unit = {
    val tablePath = tableInfo.getTablePath
    val streaming = tableInfo.getFactTable.getTableProperties.get("streaming")
    if (streaming != null && streaming.equalsIgnoreCase("true") && tablePath.startsWith("s3")) {
      throw new UnsupportedOperationException("streaming is not supported with s3 store")
    }
    // bucket table should not set sort scope because the data can only sort inside bucket itself
    val bucketColumnsProp = tableInfo.getFactTable.getTableProperties.get("bucket_columns")
    val sortScopeProp = tableInfo.getFactTable.getTableProperties.get("sort_scope")
    if (bucketColumnsProp != null) {
      if (sortScopeProp != null) {
        // bucket table can only sort inside bucket, can not set sort_scope for table.
        throw new ProcessMetaDataException(tableInfo.getDatabaseName,
          tableInfo.getFactTable.getTableName, "Bucket table only sort inside buckets," +
            " can not set sort scope but can set sort columns.");
      } else {
        tableInfo.getFactTable.getListOfColumns.asScala.foreach(column =>
          if (column.getDataType == DataTypes.BINARY) {
            throw new ProcessMetaDataException(tableInfo.getDatabaseName,
              tableInfo.getFactTable.getTableName, "bucket table do not support binary.");
          }
        )
      }
    }
    // Add validation for sort scope when create table
    val sortScope = tableInfo.getFactTable.getTableProperties.asScala
      .getOrElse("sort_scope", CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
    if (!CarbonUtil.isValidSortOption(sortScope)) {
      throw new InvalidConfigurationException(
        s"Passing invalid SORT_SCOPE '$sortScope', valid SORT_SCOPE are 'NO_SORT'," +
        s" 'LOCAL_SORT' and 'GLOBAL_SORT' ")
    }

    if (tableInfo.getFactTable.getListOfColumns.size <= 0) {
      throw new ProcessMetaDataException(tableInfo.getDatabaseName,
        tableInfo.getFactTable.getTableName, "Table should have at least one column.")
    }

    // Add validation for column compressor when create table
    val columnCompressor = tableInfo.getFactTable.getTableProperties.get(
      CarbonCommonConstants.COMPRESSOR)
    validateColumnCompressorProperty(columnCompressor)
  }

  def validateColumnCompressorProperty(columnCompressor: String): Unit = {
    // Add validation for column compressor when creating index table
    try {
      if (null != columnCompressor) {
        CompressorFactory.getInstance().getCompressor(columnCompressor)
      }
    } catch {
      case ex: UnsupportedOperationException =>
        throw new InvalidConfigurationException(ex.getMessage)
    }
  }


  def getProperties(table: CatalogTable): Map[String, String] = {
    if (table.storage.properties.nonEmpty) {
      // for carbon session, properties get from storage.
      table.storage.properties
    } else {
      // for leo session, properties can not get from storageFormat, should use tableDesc.
      table.properties
    }
  }

  def normalizeProperties(properties: Map[String, String]): Map[String, String] = {
    properties.map { entry =>
      if (needToConvertToLowerCase(entry._1)) {
        (entry._1.toLowerCase, entry._2.toLowerCase)
      } else {
        (entry._1.toLowerCase, entry._2)
      }
    }
  }

  def buildTableInfoFromCatalogTable(
      table: CatalogTable,
      ifNotExists: Boolean,
      sparkSession: SparkSession,
      selectQuery: Option[LogicalPlan] = None): TableInfo = {
    val tableProperties = normalizeProperties(getProperties(table))
    val options = new CarbonOption(Map(tableProperties.toSeq: _*))
    // validate streaming property
    validateStreamingProperty(options)
    val parser = new CarbonSpark2SqlParser()
    val isExternal = table.tableType == CatalogTableType.EXTERNAL
    var fields = parser.getFields(table.schema.fields, isExternal)
    val provider = table.provider.get
    val partitionColumnNames = table.partitionColumnNames.map(_.toLowerCase)

    // validate for create table as select
    selectQuery match {
      case Some(q) =>
        // create table as select does not allow creation of partitioned table
        if (partitionColumnNames.nonEmpty) {
          throw new MalformedCarbonCommandException(
            "A Create Table As Select (CTAS) statement is not allowed to " +
            "create a partitioned table using Carbondata file formats.")
        }
        // create table as select does not allow to explicitly specify schema
        if (fields.nonEmpty) {
          throw new MalformedCarbonCommandException(
            "Schema can not be specified in a Create Table As Select (CTAS) statement")
        }
        // external table is not allow
        if (isExternal) {
          throw new MalformedCarbonCommandException(
            "Create external table as select is not allowed")
        }
        fields = parser
          .getFields(CarbonEnv.getInstance(sparkSession).carbonMetaStore
            .getSchemaFromUnresolvedRelation(sparkSession, Some(q).get))
      case _ =>
      // ignore this case
    }

    if (partitionColumnNames.nonEmpty && options.isStreaming) {
      throw new MalformedCarbonCommandException(
        "Streaming is not allowed on partitioned table")
    }
    if (!isExternal && fields.isEmpty) {
      throw new MalformedCarbonCommandException(
        "Creating table without column(s) is not supported")
    }
    // filter out internally added external keyword property
    val newTableProperties = tableProperties.filterNot(_._1.equalsIgnoreCase("hasexternalkeyword"))
    if (isExternal && fields.isEmpty && newTableProperties.nonEmpty) {
      // as fields are always zero for external table, cannot validate table properties.
      throw new MalformedCarbonCommandException(
        "Table properties are not supported for external table")
    }

    // validate tblProperties
    val tblProperties = mutable.Map(tableProperties.toSeq: _*)
    val bucketFields =
      parser.getBucketFields(tblProperties, fields, options)
    var isTransactionalTable: Boolean = true
    // table must convert database name and table name to lower case
    val identifier = AbsoluteTableIdentifier.from(
      CarbonUtil.checkAndAppendFileSystemURIScheme(table.storage
        .locationUri
        .map(CatalogUtils.URIToString)
        .getOrElse("")),
      CarbonEnv.getDatabaseName(table.identifier.database)(sparkSession).toLowerCase(),
      table.identifier.table.toLowerCase()
    )
    val tableInfo = if (isExternal) {
      // read table info from schema file in the provided table path
      val tableInfo = {
        try {
          val schemaPath = CarbonTablePath.getSchemaFilePath(identifier.getTablePath)
          if (!FileFactory.isFileExist(schemaPath)) {
            if (provider.equalsIgnoreCase("'carbonfile'")) {
              SchemaReader.inferSchema(identifier, true)
            } else {
              isTransactionalTable = false
              SchemaReader.inferSchema(identifier, false)
            }
          } else {
            SchemaReader.getTableInfo(identifier)
          }
        } catch {
          case e: Throwable =>
            if (fields.nonEmpty) {
              val partitionerFields = fields
                .filter(field => partitionColumnNames.contains(field.column))
                .map(field => PartitionerField(field.column, field.dataType, null))
              val tableModel: TableModel = CarbonParserUtil.prepareTableModel(
                ifNotExists,
                Some(identifier.getDatabaseName),
                identifier.getTableName,
                fields,
                partitionerFields,
                tblProperties,
                bucketFields,
                isAlterFlow = false,
                table.comment
              )
              if(table.properties.contains("hasexternalkeyword")) {
                isTransactionalTable = true
              }
              TableNewProcessor(tableModel)
            } else {
              throw new MalformedCarbonCommandException(
                s"Invalid table path provided: ${ identifier.getTablePath } ")
            }
        }
      }
      // set "_external" property, so that DROP TABLE will not delete the data
      if (provider.equalsIgnoreCase("'carbonfile'")) {
        tableInfo.getFactTable.getTableProperties.put("_filelevelformat", "true")
        tableInfo.getFactTable.getTableProperties.put("_external", "false")
      } else {
        tableInfo.getFactTable.getTableProperties.put("_external", "true")
        tableInfo.getFactTable.getTableProperties.put("_filelevelformat", "false")
      }
      var isLocalDic_enabled = tableInfo.getFactTable.getTableProperties
        .get(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE)
      if (null == isLocalDic_enabled) {
        tableInfo.getFactTable.getTableProperties
          .put(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE,
            CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.LOCAL_DICTIONARY_SYSTEM_ENABLE,
                CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE_DEFAULT))
      }
      isLocalDic_enabled = tableInfo.getFactTable.getTableProperties
        .get(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE)
      if (CarbonScalaUtil.validateLocalDictionaryEnable(isLocalDic_enabled) &&
          isLocalDic_enabled.toBoolean) {
        val allColumns = tableInfo.getFactTable.getListOfColumns
        for (i <- 0 until allColumns.size()) {
          val cols = allColumns.get(i)
          if (cols.getDataType == DataTypes.STRING || cols.getDataType == DataTypes.VARCHAR) {
            cols.setLocalDictColumn(true)
          }
        }
        tableInfo.getFactTable.setListOfColumns(allColumns)
      }
      tableInfo
    } else {
      val partitionerFields = fields
        .filter(field => partitionColumnNames.contains(field.column))
        .map(field => PartitionerField(field.column, field.dataType, null))
      // prepare table model of the collected tokens
      val tableModel: TableModel = CarbonParserUtil.prepareTableModel(
        ifNotExists,
        Option(identifier.getDatabaseName),
        identifier.getTableName,
        fields,
        partitionerFields,
        tblProperties,
        bucketFields,
        isAlterFlow = false,
        table.comment
      )
      TableNewProcessor(tableModel)
    }
    tableInfo.setTablePath(identifier.getTablePath)
    tableInfo.setTransactionalTable(isTransactionalTable)
    tableInfo
  }

  /**
   * This method will convert the database name to lower case
   *
   * @param dbName database name.
   * @return Option of String
   */
  def convertDbNameToLowerCase(dbName: Option[String]): Option[String] = {
    dbName match {
      case Some(databaseName) => Some(databaseName.toLowerCase)
      case None => dbName
    }
  }

  /**
   * Parse a key-value map from a [[TablePropertyListContext]], assuming all values are specified.
   *
   * @param ctx   Instance of TablePropertyListContext defining parser rule for the table
   *              properties.
   * @param props <Map[String, String]> Map of table property list
   * @return <Map[String, String]> Map of transformed table property.
   */
  def visitPropertyKeyValues(ctx: TablePropertyListContext,
      props: Map[String, String]): Map[String, String] = {
    val badKeys = props.filter { case (_, v) => v == null }.keys
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values must be specified for key(s): ${ badKeys.mkString("[", ",", "]") }", ctx)
    }
    props.map { case (key, value) =>
      (key.toLowerCase, value)
    }
  }

  /**
   * check's whether need to convert to lower case
   *
   * @param key <String> property key
   * @return returns <true> if lower case conversion is needed else <false>
   */
  def needToConvertToLowerCase(key: String): Boolean = {
    var noConvertList = Array(CarbonCommonConstants.COMPRESSOR, "PATH", "bad_record_path",
      "timestampformat", "dateformat")
    if (key.toLowerCase.startsWith(CarbonCommonConstants.SPATIAL_INDEX) && key.endsWith(".class")) {
      noConvertList = noConvertList ++ Array(key)
    }
    !noConvertList.exists(x => x.equalsIgnoreCase(key))
  }

  /**
   * The method validate the create table command and returns the table's columns.
   *
   * @param tableHeader       An instance of CreateTableHeaderContext having parser rules for
   *                          create table.
   * @param skewSpecContext   An instance of SkewSpecContext having parser rules for create table.
   * @param bucketSpecContext An instance of BucketSpecContext having parser rules for create table.
   * @param columns           An instance of ColTypeListContext having parser rules for columns
   *                          of the table.
   * @param cols              Table;s columns.
   * @param tableIdentifier   Instance of table identifier.
   * @param isTempTable       Flag to identify temp table.
   * @return Table's column names <Seq[String]>.
   */
  def validateCreateTableReqAndGetColumns(tableHeader: CreateTableHeaderContext,
      skewSpecContext: SkewSpecContext,
      bucketSpecContext: BucketSpecContext,
      columns: ColTypeListContext,
      cols: Seq[StructField],
      tableIdentifier: TableIdentifier,
      isTempTable: Boolean): Seq[String] = {
    // TODO: implement temporary tables
    if (isTempTable) {
      throw new ParseException(
        "CREATE TEMPORARY TABLE is not supported yet. " +
        "Please use CREATE TEMPORARY VIEW as an alternative.", tableHeader)
    }
    if (skewSpecContext != null) {
      operationNotAllowed("CREATE TABLE ... SKEWED BY", skewSpecContext)
    }
    if (bucketSpecContext != null) {
      operationNotAllowed("CREATE TABLE ... CLUSTERED BY", bucketSpecContext)
    }

    // Ensuring whether no duplicate name is used in table definition
    val colNames: Seq[String] = cols.map(_.name)
    checkIfDuplicateColumnExists(columns, tableIdentifier, colNames)
    colNames
  }

  def checkIfDuplicateColumnExists(columns: ColTypeListContext,
      tableIdentifier: TableIdentifier,
      colNames: Seq[String]): Unit = {
    if (colNames.length != colNames.distinct.length) {
      val duplicateColumns = colNames.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => "\"" + x + "\""
      }
      val errorMessage = s"Duplicated column names found in table definition of " +
                         s"$tableIdentifier: ${ duplicateColumns.mkString("[", ",", "]") }"
      // In case of create table as select, ColTypeListContext will be null. Check if
      // duplicateColumns present in column names list, If yes, throw exception
      if (null != columns) {
        operationNotAllowed(errorMessage, columns)
      } else {
        throw new UnsupportedOperationException(errorMessage)
      }
    }
  }

  /**
   * The method return's the storage type
   *
   * @param createFileFormat
   * @return
   */
  def getFileStorage(createFileFormat: CreateFileFormatContext): String = {
    Option(createFileFormat) match {
      case Some(value) =>
        val result = value.children.get(1).getText
        if (result.equalsIgnoreCase("by")) {
          value.storageHandler().STRING().getSymbol.getText
        } else if (result.equalsIgnoreCase("as") && value.children.size() > 1) {
          value.children.get(2).getText
        } else {
          // The case of "STORED AS PARQUET/ORC"
          ""
        }
      case _ => ""
    }
  }

  def copyTablePartition(source: TablePartitionSpec): TablePartitionSpec = {
    val target: TablePartitionSpec = source.map(entry => (entry._1.toLowerCase, entry._2))
    target
  }

  def alterTableColumnRenameAndModifyDataType(
      dbName: Option[String],
      table: String,
      columnName: String,
      columnNameCopy: String,
      dataType: Option[String],
      values: Option[List[(Int, Int)]],
      comment: Option[String],
      complexChild: Option[Field]
  ): CarbonAlterTableColRenameDataTypeChangeCommand = {
    val isColumnRename = !columnName.equalsIgnoreCase(columnNameCopy)
    val dataTypeInfo = if (!dataType.equals(None)) {
      CarbonParserUtil.parseDataType(columnName, dataType.get.toLowerCase, values)
    } else {
      CarbonParserUtil.parseDataType(columnNameCopy, complexChild.get, values)
    }
    CarbonAlterTableColRenameDataTypeChangeCommand(AlterTableDataTypeChangeModel(
      dataTypeInfo,
      CarbonParserUtil.convertDbNameToLowerCase(dbName),
      table.toLowerCase,
      columnName.toLowerCase,
      columnNameCopy.toLowerCase,
      isColumnRename,
      comment))
  }

  def alterTableAddColumns(
      dbName: Option[String],
      table: String,
      fields: List[Field],
      tblProp: Option[List[(String, String)]]
  ): CarbonAlterTableAddColumnCommand = {
    val tableProps = if (tblProp.isDefined) {
      tblProp.get.groupBy(_._1.toLowerCase).foreach(f =>
        if (f._2.size > 1) {
          val name = f._1.toLowerCase
          val colName = name.substring(14)
          if (name.startsWith("default.value.") &&
              fields.count(p => p.column.equalsIgnoreCase(colName)) == 1) {
            sys.error(s"Duplicate default value exist for new column: ${ colName }")
          }
        }
      )
      // default value should not be converted to lower case
      val tblProps = tblProp.get
        .map(f => if (CarbonCommonConstants.TABLE_BLOCKSIZE.equalsIgnoreCase(f._1) ||
                      CarbonCommonConstants.SORT_COLUMNS.equalsIgnoreCase(f._1) ||
                      CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE.equalsIgnoreCase(f._1) ||
                      CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD.equalsIgnoreCase(f._1)) {
          throw new MalformedCarbonCommandException(
            s"Unsupported Table property in add column: ${ f._1 }")
        } else if (f._1.toLowerCase.startsWith("default.value.")) {
          if (fields.count(field => CarbonParserUtil.checkFieldDefaultValue(field.column,
            f._1.toLowerCase)) == 1) {
            f._1 -> f._2
          } else {
            throw new MalformedCarbonCommandException(
              s"Default.value property does not matches with the columns in ALTER command. " +
              s"Column name in property is: ${ f._1 }")
          }
        } else {
          f._1 -> f._2.toLowerCase
        })
      scala.collection.mutable.Map(tblProps: _*)
    } else {
      scala.collection.mutable.Map.empty[String, String]
    }

    val tableModel = CarbonParserUtil.prepareTableModel(false,
      CarbonParserUtil.convertDbNameToLowerCase(dbName),
      table.toLowerCase,
      fields.map(CarbonParserUtil.convertFieldNamesToLowercase),
      Seq.empty,
      tableProps,
      None,
      true)

    val alterTableAddColumnsModel = AlterTableAddColumnsModel(
      CarbonParserUtil.convertDbNameToLowerCase(dbName),
      table,
      tableProps.toMap,
      tableModel.dimCols,
      tableModel.msrCols,
      tableModel.highCardinalityDims.getOrElse(Seq.empty))
    CarbonAlterTableAddColumnCommand(alterTableAddColumnsModel)
  }

  def loadDataNew(
      databaseNameOp: Option[String],
      tableName: String,
      optionsList: Option[List[(String, String)]],
      partitions: Option[List[(String, Option[String])]],
      filePath: String,
      isOverwrite: Option[String]): CarbonLoadDataCommand = {
    if (optionsList.isDefined) {
      CarbonParserUtil.validateOptions(optionsList)
    }
    val optionsMap = optionsList.getOrElse(List.empty[(String, String)]).toMap
    val partitionSpec = partitions.getOrElse(List.empty[(String, Option[String])]).toMap
    CarbonLoadDataCommand(
      databaseNameOp = CarbonParserUtil.convertDbNameToLowerCase(databaseNameOp),
      tableName = tableName,
      factPathFromUser = filePath,
      dimFilesPath = Seq(),
      options = optionsMap,
      isOverwriteTable = isOverwrite.isDefined,
      partition = partitionSpec)
  }

  def convertPropertiesToLowercase(properties: Map[String, String]): mutable.Map[String, String] = {
    val tableProperties = mutable.Map[String, String]()
    properties.foreach { property =>
      if (needToConvertToLowerCase(property._1)) {
        tableProperties.put(property._1.toLowerCase, property._2.toLowerCase)
      } else {
        tableProperties.put(property._1.toLowerCase, property._2)
      }
    }
    tableProperties
  }
}
