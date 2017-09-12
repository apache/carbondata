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

package org.apache.spark.sql

import java.io.{BufferedWriter, FileWriter, IOException}
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.schema.InvalidSchemaException
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.CarbonLateDecodeStrategy
import org.apache.spark.sql.execution.command.{CreateTable, TableModel, TableNewProcessor}
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory}
import org.apache.spark.sql.hive.{CarbonMetaStore, CarbonRelation}
import org.apache.spark.sql.execution.command.{BucketFields, CreateTable}
import org.apache.spark.sql.optimizer.CarbonLateDecodeRule
import org.apache.spark.sql.parser.CarbonSpark2SqlParser
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.CarbonStreamingOutputWriterFactory
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry
import org.apache.carbondata.core.metadata.schema.table.TableInfo
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.format.{DataType, TableInfo, TableSchema}
import org.apache.carbondata.spark.CarbonOption
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException


/**
 * Carbon relation provider compliant to data source api.
 * Creates carbon relations
 */
class CarbonSource extends CreatableRelationProvider with RelationProvider
  with SchemaRelationProvider with DataSourceRegister with FileFormat  {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def shortName(): String = {
    "carbondata"
  }

  // will be called if hive supported create table command is provided
  override def createRelation(sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    CarbonEnv.getInstance(sqlContext.sparkSession)
    // if path is provided we can directly create Hadoop relation.
    // Otherwise create datasource relation
    parameters.get("tablePath") match {
      case Some(path) => CarbonDatasourceHadoopRelation(sqlContext.sparkSession,
        Array(path),
        parameters,
        None)
      case _ =>
        val options = new CarbonOption(parameters)
        val storePath = CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.STORE_LOCATION)
        val tablePath = storePath + "/" + options.dbName + "/" + options.tableName
        CarbonDatasourceHadoopRelation(sqlContext.sparkSession, Array(tablePath), parameters, None)
    }
  }

  // called by any write operation like INSERT INTO DDL or DataFrame.write API
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    CarbonEnv.getInstance(sqlContext.sparkSession)
    // User should not specify path since only one store is supported in carbon currently,
    // after we support multi-store, we can remove this limitation
    require(!parameters.contains("path"), "'path' should not be specified, " +
                                          "the path to store carbon file is the 'storePath' " +
                                          "specified when creating CarbonContext")

    val options = new CarbonOption(parameters)
    val storePath = CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION)
    val tablePath = new Path(storePath + "/" + options.dbName + "/" + options.tableName)
    val isExists = tablePath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
      .exists(tablePath)
    val (doSave, doAppend) = (mode, isExists) match {
      case (SaveMode.ErrorIfExists, true) =>
        sys.error(s"ErrorIfExists mode, path $storePath already exists.")
      case (SaveMode.Overwrite, true) =>
        sqlContext.sparkSession
          .sql(s"DROP TABLE IF EXISTS ${ options.dbName }.${ options.tableName }")
        (true, false)
      case (SaveMode.Overwrite, false) | (SaveMode.ErrorIfExists, false) =>
        (true, false)
      case (SaveMode.Append, _) =>
        (false, true)
      case (SaveMode.Ignore, exists) =>
        (!exists, false)
    }

    if (doSave) {
      // save data when the save mode is Overwrite.
      new CarbonDataFrameWriter(sqlContext, data).saveAsCarbonFile(parameters)
    } else if (doAppend) {
      new CarbonDataFrameWriter(sqlContext, data).appendToCarbonFile(parameters)
    }

    createRelation(sqlContext, parameters, data.schema)
  }

  // called by DDL operation with a USING clause
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      dataSchema: StructType): BaseRelation = {
    CarbonEnv.getInstance(sqlContext.sparkSession)
    addLateDecodeOptimization(sqlContext.sparkSession)
    val dbName: String = parameters.getOrElse("dbName",
      CarbonCommonConstants.DATABASE_DEFAULT_NAME).toLowerCase
    val tableOption: Option[String] = parameters.get("tableName")
    if (tableOption.isEmpty) {
      sys.error("Table creation failed. Table name is not specified")
    }
    val tableName = tableOption.get.toLowerCase()
    if (tableName.contains(" ")) {
      sys.error("Table creation failed. Table name cannot contain blank space")
    }
    val (path, updatedParams) = if (sqlContext.sparkSession.sessionState.catalog.listTables(dbName)
      .exists(_.table.equalsIgnoreCase(tableName))) {
        getPathForTable(sqlContext.sparkSession, dbName, tableName, parameters)
    } else {
        createTableIfNotExists(sqlContext.sparkSession, parameters, dataSchema)
    }

    CarbonDatasourceHadoopRelation(sqlContext.sparkSession, Array(path), updatedParams,
      Option(dataSchema))
  }

  private def addLateDecodeOptimization(ss: SparkSession): Unit = {
    if (ss.sessionState.experimentalMethods.extraStrategies.isEmpty) {
      ss.sessionState.experimentalMethods.extraStrategies = Seq(new CarbonLateDecodeStrategy)
      ss.sessionState.experimentalMethods.extraOptimizations = Seq(new CarbonLateDecodeRule)
    }
  }


  private def createTableIfNotExists(sparkSession: SparkSession, parameters: Map[String, String],
      dataSchema: StructType) = {

    val dbName: String = parameters.getOrElse("dbName",
      CarbonCommonConstants.DATABASE_DEFAULT_NAME).toLowerCase
    val tableName: String = parameters.getOrElse("tableName", "").toLowerCase

    try {
      if (parameters.contains("carbonSchemaPartsNo")) {
        getPathForTable(sparkSession, dbName, tableName, parameters)
      } else {
        CarbonEnv.getInstance(sparkSession).carbonMetastore
          .lookupRelation(Option(dbName), tableName)(sparkSession)
        (CarbonEnv.getInstance(sparkSession).storePath + s"/$dbName/$tableName", parameters)
      }
    } catch {
      case ex: NoSuchTableException =>
        val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetastore
        val updatedParams =
          CarbonSource.updateAndCreateTable(dataSchema, sparkSession, metaStore, parameters)
        getPathForTable(sparkSession, dbName, tableName, updatedParams)
      case ex: Exception =>
        throw new Exception("do not have dbname and tablename for carbon table", ex)
    }
  }

/**
 * Returns the path of the table
 * @param sparkSession
 * @param dbName
 * @param tableName
 * @return
 */
  private def getPathForTable(sparkSession: SparkSession, dbName: String,
      tableName : String, parameters: Map[String, String]): (String, Map[String, String]) = {

    if (StringUtils.isBlank(tableName)) {
      throw new MalformedCarbonCommandException("The Specified Table Name is Blank")
    }
    if (tableName.contains(" ")) {
      throw new MalformedCarbonCommandException("Table Name Should not have spaces ")
    }
    try {
      if (parameters.contains("tablePath")) {
        (parameters("tablePath"), parameters)
      } else if (!sparkSession.isInstanceOf[CarbonSession]) {
        (CarbonEnv.getInstance(sparkSession).storePath + "/" + dbName + "/" + tableName, parameters)
      } else {
        val relation = CarbonEnv.getInstance(sparkSession).carbonMetastore
          .lookupRelation(Option(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation]
        (relation.tableMeta.tablePath, parameters)
      }
    } catch {
        case ex: Exception =>
          throw new Exception(s"Do not have $dbName and $tableName", ex)
    }
  }

/**
 * Prepares a write job and returns an [[OutputWriterFactory]].  Client side job preparation can
 * be put here.  For example, user defined output committer can be configured here
 * by setting the output committer class in the conf of spark.sql.sources.outputCommitterClass.
 */
  def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {

    // Check if table with given path exists
    validateTable(options.get("path").get)

    // Check id streaming data schema matches with carbon table schema
    // Data from socket source does not have schema attached to it,
    // Following check is to ignore schema validation for socket source.
    if (!(dataSchema.size.equals(1) &&
      dataSchema.fields(0).dataType.equals(StringType))) {
      val tablePath = options.get("path")
      val path: String = tablePath match {
        case Some(value) => value
        case None => ""
      }
      val meta: CarbonMetastore = new CarbonMetastore(sparkSession.conf, path)
      val schemaPath = path + "/Metadata/schema"
      val schema: TableInfo = meta.readSchemaFile(schemaPath)
      val isSchemaValid = validateSchema(schema, dataSchema)

      if(!isSchemaValid) {
        LOGGER.error("Schema Validation Failed: streaming data schema"
          + "does not match with carbon table schema")
        throw new InvalidSchemaException("Schema Validation Failed : " +
          "streaming data schema does not match with carbon table schema")
      }
    }
    new CarbonStreamingOutputWriterFactory()
  }

  /**
   * Read schema from existing carbon table
   * @param sparkSession
   * @param tablePath carbon table path
   * @return true if schema validation is successful else false
   */
  private def getTableSchema(sparkSession: SparkSession, tablePath: String): TableInfo = {
    val meta: CarbonMetastore = new CarbonMetastore(sparkSession.conf, tablePath)
    val schemaPath = tablePath + "/Metadata/schema"
    val schema: TableInfo = meta.readSchemaFile(schemaPath)
    schema
  }

  /**
   * Validates streamed schema against existing table schema
   * @param schema existing carbon table schema
   * @param dataSchema streamed data schema
   * @return true if schema validation is successful else false
   */
  private def validateSchema(schema: TableInfo, dataSchema: StructType): Boolean = {
    val factTable: TableSchema = schema.getFact_table

    import scala.collection.mutable.ListBuffer
    import scala.collection.JavaConverters._
    var columnnSchemaValues = factTable.getTable_columns.asScala.sortBy(_.schemaOrdinal)

    var columnDataTypes = new ListBuffer[String]()
    for(columnDataType <- columnnSchemaValues) {
      columnDataTypes.append(columnDataType.data_type.toString)
    }
    val tableColumnDataTypeList = columnDataTypes.toList

    var streamSchemaDataTypes = new ListBuffer[String]()
    for(i <- 0 until dataSchema.size) {
      streamSchemaDataTypes
        .append(
          mapStreamingDataTypeToString(dataSchema.fields(i).dataType.toString))
    }
    val streamedDataTypeList = streamSchemaDataTypes.toList

    val isValid = tableColumnDataTypeList == streamedDataTypeList
    isValid
  }

  /**
   * Parses streamed datatype according to carbon datatype
   * @param dataType
   * @return String
   */
  def mapStreamingDataTypeToString(dataType: String): String = {
    dataType match {
      case "IntegerType" => DataType.INT.toString
      case "StringType" => DataType.STRING.toString
      case "DateType" => DataType.DATE.toString
      case "DoubleType" => DataType.DOUBLE.toString
      case "FloatType" => DataType.DOUBLE.toString
      case "LongType" => DataType.LONG.toString
      case "ShortType" => DataType.SHORT.toString
      case "TimestampType" => DataType.TIMESTAMP.toString
    }
  }

  /**
   * Validates if given table exists or throws exception
   * @param String existing carbon table path
   * @return None
   */
  private def validateTable(tablePath: String): Unit = {

    val formattedTablePath = tablePath.replace('\\', '/')
    val names = formattedTablePath.split("/")
    if (names.length < 3) {
      throw new IllegalArgumentException("invalid table path: " + tablePath)
    }
    val tableName : String = names(names.length - 1)
    val dbName : String = names(names.length - 2)
    val storePath = formattedTablePath.substring(0,
      formattedTablePath.lastIndexOf
      (((dbName.concat(CarbonCommonConstants.FILE_SEPARATOR).toString)
        .concat(tableName)).toString) - 1)
    val absoluteTableIdentifier: AbsoluteTableIdentifier =
      new AbsoluteTableIdentifier(storePath,
        new CarbonTableIdentifier(dbName, tableName,
          UUID.randomUUID().toString))

    if (!checkIfTableExists(absoluteTableIdentifier)) {
      throw new NoSuchTableException(dbName, tableName)
    }
  }

  /**
   * Checks if table exists by checking its schema file
   * @param absoluteTableIdentifier
   * @return Boolean
   */
  private def checkIfTableExists(absoluteTableIdentifier: AbsoluteTableIdentifier): Boolean = {
    val carbonTablePath: CarbonTablePath = CarbonStorePath
      .getCarbonTablePath(absoluteTableIdentifier)
    val schemaFilePath: String = carbonTablePath.getSchemaFilePath
    FileFactory.isFileExist(schemaFilePath, FileFactory.FileType.LOCAL) ||
      FileFactory.isFileExist(schemaFilePath, FileFactory.FileType.HDFS) ||
      FileFactory.isFileExist(schemaFilePath, FileFactory.FileType.VIEWFS)
  }

  /**
   * If user wants to stream data from carbondata table source
   * and if following conditions are true:
   *    1. No schema provided by the user in readStream()
   *    2. spark.sql.streaming.schemaInference is set to true
   * carbondata can infer a table schema from a valid table path
   * The schema inference is not mandatory, but good have.
   * When possible, this method should return the schema of the given `files`.  When the format
   * does not support inference, or no valid files are given should return None.  In these cases
   * Spark will require that user specify the schema manually.
   */
  def inferSchema(
                   sparkSession: SparkSession,
                   options: Map[String, String],
                   files: Seq[FileStatus]): Option[StructType] = {
    Some(new StructType().add("value", StringType))
    val path = options.get("path")
    val tablePath: String = path match {
      case Some(value) => value
      case None => ""
    }
    // Check if table with given path exists
    validateTable(tablePath)
    val schema: TableInfo = getTableSchema(sparkSession: SparkSession, tablePath: String)
    val factTable: TableSchema = schema.getFact_table
    import scala.collection.JavaConverters._
    var columnnSchemaValues = factTable.getTable_columns.asScala.sortBy(_.schemaOrdinal)

    val tableColumnNames = new ListBuffer[String]()
    for (columnName <- columnnSchemaValues) {
      tableColumnNames.append(columnName.column_name)
    }
    val tableColumnNamesList = tableColumnNames.toList

    var columnDataTypes = new ListBuffer[String]()
    for(columnDataType <- columnnSchemaValues) {
      columnDataTypes.append(columnDataType.data_type.toString)
    }
    val tableColumnDataTypeList = columnDataTypes.toList

    val inferredSchema: Option[StructType] = new Some(new StructType())
    for (i <- tableColumnNamesList.indices) {
      inferredSchema.get.add(tableColumnNamesList(i), tableColumnDataTypeList(i))
    }

    inferredSchema
  }
}

object CarbonSource {

  def createTableInfoFromParams(parameters: Map[String, String],
      dataSchema: StructType,
      dbName: String,
      tableName: String): TableModel = {
    val sqlParser = new CarbonSpark2SqlParser
    val fields = sqlParser.getFields(dataSchema)
    val map = scala.collection.mutable.Map[String, String]()
    parameters.foreach { case (key, value) => map.put(key, value.toLowerCase()) }
    val options = new CarbonOption(parameters)
    val bucketFields = sqlParser.getBucketFields(map, fields, options)
    sqlParser.prepareTableModel(ifNotExistPresent = false, Option(dbName),
      tableName, fields, Nil, map, bucketFields)
  }

  /**
   * Update spark catalog table with schema information in case of schema storage is hive metastore
   * @param tableDesc
   * @param sparkSession
   * @return
   */
  def updateCatalogTableWithCarbonSchema(tableDesc: CatalogTable,
      sparkSession: SparkSession): CatalogTable = {
    val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val storageFormat = tableDesc.storage
    val properties = storageFormat.properties
    if (!properties.contains("carbonSchemaPartsNo")) {
      val map = updateAndCreateTable(tableDesc.schema, sparkSession, metaStore, properties)
      // updating params
      val updatedFormat = storageFormat.copy(properties = map)
      tableDesc.copy(storage = updatedFormat)
    } else {
      val tableInfo = CarbonUtil.convertGsonToTableInfo(properties.asJava)
      if (!metaStore.isReadFromHiveMetaStore) {
        // save to disk
        metaStore.saveToDisk(tableInfo, properties.get("tablePath").get)
        // remove schema string from map as we don't store carbon schema to hive metastore
        val map = CarbonUtil.removeSchemaFromMap(properties.asJava)
        val updatedFormat = storageFormat.copy(properties = map.asScala.toMap)
        tableDesc.copy(storage = updatedFormat)
      } else {
        tableDesc
      }
    }
  }

  def updateAndCreateTable(dataSchema: StructType,
      sparkSession: SparkSession,
      metaStore: CarbonMetaStore,
      properties: Map[String, String]): Map[String, String] = {
    val dbName: String = properties.getOrElse("dbName",
      CarbonCommonConstants.DATABASE_DEFAULT_NAME).toLowerCase
    val tableName: String = properties.getOrElse("tableName", "").toLowerCase
    val model = createTableInfoFromParams(properties, dataSchema, dbName, tableName)
    val tableInfo: TableInfo = TableNewProcessor(model)
    val tablePath = CarbonEnv.getInstance(sparkSession).storePath + "/" + dbName + "/" + tableName
    val schemaEvolutionEntry = new SchemaEvolutionEntry
    schemaEvolutionEntry.setTimeStamp(tableInfo.getLastUpdatedTime)
    tableInfo.getFactTable.getSchemaEvalution.
      getSchemaEvolutionEntryList.add(schemaEvolutionEntry)
    val map = if (metaStore.isReadFromHiveMetaStore) {
      val tableIdentifier = AbsoluteTableIdentifier.fromTablePath(tablePath)
      val carbonTablePath = CarbonStorePath.getCarbonTablePath(tableIdentifier)
      val schemaMetadataPath =
        CarbonTablePath.getFolderContainingFile(carbonTablePath.getSchemaFilePath)
      tableInfo.setMetaDataFilepath(schemaMetadataPath)
      tableInfo.setStorePath(tableIdentifier.getStorePath)
      CarbonUtil.convertToMultiStringMap(tableInfo)
    } else {
      metaStore.saveToDisk(tableInfo, tablePath)
      new java.util.HashMap[String, String]()
    }
    properties.foreach(e => map.put(e._1, e._2))
    map.put("tablePath", tablePath)
    map.asScala.toMap
  }
}
