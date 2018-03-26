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

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.command.{TableModel, TableNewProcessor}
import org.apache.spark.sql.execution.strategy.CarbonLateDecodeStrategy
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.hive.CarbonMetaStore
import org.apache.spark.sql.optimizer.CarbonLateDecodeRule
import org.apache.spark.sql.parser.CarbonSpark2SqlParser
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry
import org.apache.carbondata.core.metadata.schema.table.TableInfo
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.spark.CarbonOption
import org.apache.carbondata.spark.util.CarbonScalaUtil
import org.apache.carbondata.streaming.{CarbonStreamException, StreamSinkFactory}

/**
 * Carbon relation provider compliant to data source api.
 * Creates carbon relations
 */
class CarbonSource extends CreatableRelationProvider with RelationProvider
  with SchemaRelationProvider with StreamSinkProvider with DataSourceRegister {

  override def shortName(): String = "carbondata"

  // will be called if hive supported create table command is provided
  override def createRelation(sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    CarbonEnv.getInstance(sqlContext.sparkSession)
    // if path is provided we can directly create Hadoop relation. \
    // Otherwise create datasource relation
    val newParameters = CarbonScalaUtil.getDeserializedParameters(parameters)
    newParameters.get("tablePath") match {
      case Some(path) => CarbonDatasourceHadoopRelation(sqlContext.sparkSession,
        Array(path),
        newParameters,
        None)
      case _ =>
        val options = new CarbonOption(newParameters)
        val tablePath =
          CarbonEnv.getTablePath(options.dbName, options.tableName)(sqlContext.sparkSession)
        CarbonDatasourceHadoopRelation(sqlContext.sparkSession,
          Array(tablePath),
          newParameters,
          None)
    }
  }

  // called by any write operation like INSERT INTO DDL or DataFrame.write API
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    CarbonEnv.getInstance(sqlContext.sparkSession)
    val newParameters = CarbonScalaUtil.getDeserializedParameters(parameters)
    // User should not specify path since only one store is supported in carbon currently,
    // after we support multi-store, we can remove this limitation
    require(!newParameters.contains("path"), "'path' should not be specified, " +
                                          "the path to store carbon file is the 'storePath' " +
                                          "specified when creating CarbonContext")

    val options = new CarbonOption(newParameters)
    val tablePath = new Path(
      CarbonEnv.getTablePath(options.dbName, options.tableName)(sqlContext.sparkSession))
    val isExists = tablePath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
      .exists(tablePath)
    val (doSave, doAppend) = (mode, isExists) match {
      case (SaveMode.ErrorIfExists, true) =>
        CarbonException.analysisException(s"table path already exists.")
      case (SaveMode.Overwrite, true) =>
        val dbName = CarbonEnv.getDatabaseName(options.dbName)(sqlContext.sparkSession)
        // In order to overwrite, delete all segments in the table
        sqlContext.sparkSession.sql(
          s"""
             | DELETE FROM TABLE $dbName.${options.tableName}
             | WHERE SEGMENT.STARTTIME BEFORE '2099-06-01 01:00:00'
           """.stripMargin)
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
      new CarbonDataFrameWriter(sqlContext, data).saveAsCarbonFile(newParameters)
    } else if (doAppend) {
      new CarbonDataFrameWriter(sqlContext, data).appendToCarbonFile(newParameters)
    }

    createRelation(sqlContext, newParameters, data.schema)
  }

  // called by DDL operation with a USING clause
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      dataSchema: StructType): BaseRelation = {
    CarbonEnv.getInstance(sqlContext.sparkSession)
    addLateDecodeOptimization(sqlContext.sparkSession)
    val newParameters = CarbonScalaUtil.getDeserializedParameters(parameters)
    val dbName: String =
      CarbonEnv.getDatabaseName(newParameters.get("dbName"))(sqlContext.sparkSession)
    val tableOption: Option[String] = newParameters.get("tableName")
    if (tableOption.isEmpty) {
      CarbonException.analysisException("Table creation failed. Table name is not specified")
    }
    val tableName = tableOption.get.toLowerCase()
    if (tableName.contains(" ")) {
      CarbonException.analysisException(
        "Table creation failed. Table name cannot contain blank space")
    }
    val (path, updatedParams) = if (sqlContext.sparkSession.sessionState.catalog.listTables(dbName)
      .exists(_.table.equalsIgnoreCase(tableName))) {
        getPathForTable(sqlContext.sparkSession, dbName, tableName, newParameters)
    } else {
        createTableIfNotExists(sqlContext.sparkSession, newParameters, dataSchema)
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


  private def createTableIfNotExists(
      sparkSession: SparkSession,
      parameters: Map[String, String],
      dataSchema: StructType): (String, Map[String, String]) = {

    val dbName: String = CarbonEnv.getDatabaseName(parameters.get("dbName"))(sparkSession)
    val tableName: String = parameters.getOrElse("tableName", "").toLowerCase

    try {
      val carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
      (carbonTable.getTablePath, parameters)
    } catch {
      case _: NoSuchTableException =>
        val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetastore
        val identifier = AbsoluteTableIdentifier.from(
          CarbonEnv.getTablePath(Some(dbName), tableName)(sparkSession),
          dbName,
          tableName)
        val updatedParams = CarbonSource.updateAndCreateTable(
          identifier, dataSchema, sparkSession, metaStore, parameters)
        (CarbonEnv.getTablePath(Some(dbName), tableName)(sparkSession), updatedParams)
      case ex: Exception =>
        throw new Exception("do not have dbname and tablename for carbon table", ex)
    }
  }

  /**
   * Returns the path of the table
   *
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
        (CarbonProperties.getStorePath + "/" + dbName + "/" + tableName, parameters)
      } else {
        (CarbonEnv.getTablePath(Some(dbName), tableName)(sparkSession), parameters)
      }
    } catch {
      case ex: Exception =>
        throw new Exception(s"Do not have $dbName and $tableName", ex)
    }
  }

  /**
   * produce a streaming `Sink` for a specific format
   * now it will create a default sink(CarbonAppendableStreamSink) for row format
   */
  override def createSink(sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {

    // check "tablePath" option
    val options = new CarbonOption(parameters)
    val dbName = CarbonEnv.getDatabaseName(options.dbName)(sqlContext.sparkSession)
    val tableName = options.tableName
    if (tableName.contains(" ")) {
      throw new CarbonStreamException("Table creation failed. Table name cannot contain blank " +
                                      "space")
    }
    val sparkSession = sqlContext.sparkSession
    val carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
    if (!carbonTable.isStreamingTable) {
      throw new CarbonStreamException(s"Table ${carbonTable.getDatabaseName}." +
                                      s"${carbonTable.getTableName} is not a streaming table")
    }

    // create sink
    StreamSinkFactory.createStreamTableSink(
      sqlContext.sparkSession,
      sqlContext.sparkSession.sessionState.newHadoopConf(),
      carbonTable,
      parameters)
  }

}

object CarbonSource {

  def createTableInfoFromParams(
      parameters: Map[String, String],
      dataSchema: StructType,
      identifier: AbsoluteTableIdentifier): TableModel = {
    val sqlParser = new CarbonSpark2SqlParser
    val fields = sqlParser.getFields(dataSchema)
    val map = scala.collection.mutable.Map[String, String]()
    parameters.foreach { case (key, value) => map.put(key, value.toLowerCase()) }
    val options = new CarbonOption(parameters)
    val bucketFields = sqlParser.getBucketFields(map, fields, options)
    sqlParser.prepareTableModel(ifNotExistPresent = false, Option(identifier.getDatabaseName),
      identifier.getTableName, fields, Nil, map, bucketFields)
  }

  /**
   * Update spark catalog table with schema information in case of schema storage is hive metastore
   * @param tableDesc
   * @param sparkSession
   * @return
   */
  def updateCatalogTableWithCarbonSchema(
      tableDesc: CatalogTable,
      sparkSession: SparkSession): CatalogTable = {
    val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val storageFormat = tableDesc.storage
    val properties = storageFormat.properties
    if (!properties.contains("carbonSchemaPartsNo")) {
      val tablePath = CarbonEnv.getTablePath(
        tableDesc.identifier.database, tableDesc.identifier.table)(sparkSession)
      val dbName = CarbonEnv.getDatabaseName(tableDesc.identifier.database)(sparkSession)
      val identifier = AbsoluteTableIdentifier.from(tablePath, dbName, tableDesc.identifier.table)
      val map = updateAndCreateTable(
        identifier,
        tableDesc.schema,
        sparkSession,
        metaStore,
        properties)
      // updating params
      val updatedFormat = storageFormat.copy(properties = map)
      tableDesc.copy(storage = updatedFormat)
    } else {
      val tableInfo = CarbonUtil.convertGsonToTableInfo(properties.asJava)
      if (!metaStore.isReadFromHiveMetaStore) {
        // save to disk
        metaStore.saveToDisk(tableInfo, properties("tablePath"))
        // remove schema string from map as we don't store carbon schema to hive metastore
        val map = CarbonUtil.removeSchemaFromMap(properties.asJava)
        val updatedFormat = storageFormat.copy(properties = map.asScala.toMap)
        tableDesc.copy(storage = updatedFormat)
      } else {
        tableDesc
      }
    }
  }

  def updateAndCreateTable(
      identifier: AbsoluteTableIdentifier,
      dataSchema: StructType,
      sparkSession: SparkSession,
      metaStore: CarbonMetaStore,
      properties: Map[String, String]): Map[String, String] = {
    val model = createTableInfoFromParams(properties, dataSchema, identifier)
    val tableInfo: TableInfo = TableNewProcessor(model)
    tableInfo.setTablePath(identifier.getTablePath)
    tableInfo.setDatabaseName(identifier.getDatabaseName)
    val schemaEvolutionEntry = new SchemaEvolutionEntry
    schemaEvolutionEntry.setTimeStamp(tableInfo.getLastUpdatedTime)
    tableInfo.getFactTable.getSchemaEvalution.getSchemaEvolutionEntryList.add(schemaEvolutionEntry)
    val map = if (metaStore.isReadFromHiveMetaStore) {
      CarbonUtil.convertToMultiStringMap(tableInfo)
    } else {
      metaStore.saveToDisk(tableInfo, identifier.getTablePath)
      new java.util.HashMap[String, String]()
    }
    properties.foreach(e => map.put(e._1, e._2))
    map.put("tablepath", identifier.getTablePath)
    map.put("dbname", identifier.getDatabaseName)
    if (map.containsKey("tableName")) {
      val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
      LOGGER.warn("tableName is not required in options, ignoring it")
    }
    map.put("tableName", identifier.getTableName)
    map.asScala.toMap
  }
}
