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
import org.apache.spark.sql.execution.strategy.CarbonLateDecodeStrategy
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.optimizer.CarbonLateDecodeRule
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.datatype.{StructType => CarbonStructType}
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry
import org.apache.carbondata.core.metadata.schema.table.{BucketFields, MalformedCarbonCommandException, TableInfo}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
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
    parameters.get("tablePath") match {
      case Some(path) => CarbonDatasourceHadoopRelation(sqlContext.sparkSession,
        Array(path),
        parameters,
        None)
      case _ =>
        val options = new CarbonOption(sqlContext.sparkSession, parameters)
        val tablePath =
          CarbonEnv.getTablePath(Some(options.dbName), options.tableName)(sqlContext.sparkSession)
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
    val options = new CarbonOption(sqlContext.sparkSession, parameters)
    val tablePath = if (options.tablePath.isEmpty) {
      CarbonEnv.getTablePath(Some(options.dbName), options.tableName)(sqlContext.sparkSession)
    } else {
      options.tablePath.get
    }
    val path = new Path(tablePath)
    val isExists = path.getFileSystem(sqlContext.sparkContext.hadoopConfiguration).exists(path)
    val (doSave, doAppend) = (mode, isExists) match {
      case (SaveMode.ErrorIfExists, true) =>
        CarbonException.analysisException(s"path ${CarbonProperties.getStorePath} already exists.")
      case (SaveMode.Overwrite, true) =>
        sqlContext.sparkSession.sql(s"DROP TABLE IF EXISTS ${options.dbName}.${options.tableName}")
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
    val options = new CarbonOption(sqlContext.sparkSession, parameters)
    val dbName = options.dbName
    val table = options.tableName
    if (table == null) {
      CarbonException.analysisException("Table name is not specified")
    }
    val tableName = table.toLowerCase()
    if (tableName.contains(" ")) {
      CarbonException.analysisException("Table name cannot contain blank space")
    }
    val (path, updatedParams) =
      if (tableExists(sqlContext, dbName, tableName)) {
        getPathForTable(sqlContext.sparkSession, dbName, tableName, parameters)
      } else {
        createTableIfNotExists(sqlContext.sparkSession, options, dataSchema)
      }

    CarbonDatasourceHadoopRelation(sqlContext.sparkSession, Array(path), updatedParams,
      Option(dataSchema))
  }

  private def tableExists(sqlContext: SQLContext, dbName: String, tableName: String): Boolean = {
    sqlContext.sparkSession.sessionState.catalog.listTables(dbName)
      .exists(_.table.equalsIgnoreCase(tableName))
  }

  private def addLateDecodeOptimization(ss: SparkSession): Unit = {
    if (ss.sessionState.experimentalMethods.extraStrategies.isEmpty) {
      ss.sessionState.experimentalMethods.extraStrategies = Seq(new CarbonLateDecodeStrategy)
      ss.sessionState.experimentalMethods.extraOptimizations = Seq(new CarbonLateDecodeRule)
    }
  }

  private def createTableIfNotExists(
      sparkSession: SparkSession,
      options: CarbonOption,
      dataSchema: StructType): (String, Map[String, String]) = {

    val dbName: String = options.dbName
    val tableName: String = options.tableName

    try {
      if (options.toMap.contains("carbonSchemaPartsNo")) {
        getPathForTable(sparkSession, dbName, tableName, options.toMap)
      } else {
        CarbonEnv.getInstance(sparkSession).carbonMetastore
          .lookupRelation(Option(dbName), tableName)(sparkSession)
        (CarbonProperties.getStorePath + s"/$dbName/$tableName", options.toMap)
      }
    } catch {
      case _: NoSuchTableException =>
        val updatedParams = CarbonSource.updateAndCreateTable(sparkSession, options, dataSchema)
        getPathForTable(sparkSession, dbName, tableName, updatedParams)
      case ex: Exception =>
        throw ex
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
  private def getPathForTable(
      sparkSession: SparkSession,
      dbName: String,
      tableName : String,
      parameters: Map[String, String]): (String, Map[String, String]) = {

    if (StringUtils.isBlank(tableName)) {
      throw new MalformedCarbonCommandException("The Specified Table Name is Blank")
    }
    if (tableName.contains(" ")) {
      throw new MalformedCarbonCommandException("Table Name Should not have spaces ")
    }
    if (parameters.contains("tablePath")) {
      (parameters("tablePath"), parameters)
    } else if (!sparkSession.isInstanceOf[CarbonSession]) {
      (CarbonProperties.getStorePath + "/" + dbName + "/" + tableName, parameters)
    } else {
      val carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
      (carbonTable.getTablePath, parameters)
    }
  }

  /**
   * produce a streaming `Sink` for a specific format
   * now it will create a default sink(CarbonAppendableStreamSink) for row format
   */
  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    val options = new CarbonOption(sqlContext.sparkSession, parameters)
    val dbName = options.dbName
    val tableName = options.tableName
    if (tableName.contains(" ")) {
      throw new CarbonStreamException(
        "Table creation failed. Table name cannot contain blank space")
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
      val map = updateAndCreateTable(
        sparkSession,
        new CarbonOption(sparkSession, properties), tableDesc.schema)
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
      sparkSession: SparkSession,
      options: CarbonOption,
      dataSchema: StructType): Map[String, String] = {
    val dbName = options.dbName
    val tableName = options.tableName
    val tablePath = CarbonEnv.getTablePath(Some(dbName), tableName)(sparkSession)
    val tableInfo = TableInfo.builder
      .databaseName(dbName)
      .tableName(tableName)
      .tableProperties(options.toMap.asJava)
      .tablePath(tablePath)
      .schema(
        CarbonScalaUtil.convertSparkToCarbonDataType(dataSchema).asInstanceOf[CarbonStructType])
      .bucketFields(
        new BucketFields(
          options.bucketColumns.split(",").toSeq.asJava,
          options.bucketColumns))
      .create()

    val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val schemaEvolutionEntry = new SchemaEvolutionEntry
    schemaEvolutionEntry.setTimeStamp(tableInfo.getLastUpdatedTime)
    tableInfo.getFactTable.getSchemaEvalution.getSchemaEvolutionEntryList.add(schemaEvolutionEntry)
    val map = if (metaStore.isReadFromHiveMetaStore) {
      CarbonUtil.convertToMultiStringMap(tableInfo)
    } else {
      metaStore.saveToDisk(tableInfo, tablePath)
      new java.util.HashMap[String, String]()
    }
    options.toMap.foreach(e => map.put(e._1, e._2))
    map.put("tablePath", tablePath)
    map.put("dbname", dbName)
    map.asScala.toMap
  }
}
