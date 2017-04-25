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

import scala.language.implicitConversions

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.execution.CarbonLateDecodeStrategy
import org.apache.spark.sql.execution.command.{BucketFields, CreateTable, Field}
import org.apache.spark.sql.optimizer.CarbonLateDecodeRule
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DecimalType, StructType}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.CarbonOption
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException

/**
 * Carbon relation provider compliant to data source api.
 * Creates carbon relations
 */
class CarbonSource extends CreatableRelationProvider with RelationProvider
  with SchemaRelationProvider with DataSourceRegister {

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
    val tableName: String = parameters.getOrElse("tableName", "default_table").toLowerCase

    val path = if (sqlContext.sparkSession.sessionState.catalog.listTables(dbName)
      .exists(_.table.equalsIgnoreCase(tableName))) {
        getPathForTable(sqlContext.sparkSession, dbName, tableName)
    } else {
        createTableIfNotExists(sqlContext.sparkSession, parameters, dataSchema)
    }

    CarbonDatasourceHadoopRelation(sqlContext.sparkSession, Array(path), parameters,
      Option(dataSchema))
  }

  private def addLateDecodeOptimization(ss: SparkSession): Unit = {
    if (ss.sessionState.experimentalMethods.extraStrategies.isEmpty) {
      ss.sessionState.experimentalMethods.extraStrategies = Seq(new CarbonLateDecodeStrategy)
      ss.sessionState.experimentalMethods.extraOptimizations = Seq(new CarbonLateDecodeRule)
    }
  }


  private def createTableIfNotExists(sparkSession: SparkSession, parameters: Map[String, String],
      dataSchema: StructType): String = {

    val dbName: String = parameters.getOrElse("dbName",
      CarbonCommonConstants.DATABASE_DEFAULT_NAME).toLowerCase
    val tableName: String = parameters.getOrElse("tableName", "default_table").toLowerCase
    if (StringUtils.isBlank(tableName)) {
      throw new MalformedCarbonCommandException("The Specified Table Name is Blank")
    }
    if (tableName.contains(" ")) {
      throw new MalformedCarbonCommandException("Table Name Should not have spaces ")
    }
    val options = new CarbonOption(parameters)
    try {
      CarbonEnv.getInstance(sparkSession).carbonMetastore
        .lookupRelation(Option(dbName), tableName)(sparkSession)
      CarbonEnv.getInstance(sparkSession).carbonMetastore.storePath + s"/$dbName/$tableName"
    } catch {
      case ex: NoSuchTableException =>
        val fields = dataSchema.map { col =>
          val dataType = Option(col.dataType.toString)
          // This is to parse complex data types
          val colName = col.name.toLowerCase
          val f: Field = Field(colName, dataType, Option(colName), None, null)
          // the data type of the decimal type will be like decimal(10,0)
          // so checking the start of the string and taking the precision and scale.
          // resetting the data type with decimal
          Option(col.dataType).foreach {
            case d: DecimalType =>
              f.precision = d.precision
              f.scale = d.scale
              f.dataType = Some("decimal")
            case _ => // do nothing
          }
          f
        }
        val map = scala.collection.mutable.Map[String, String]()
        parameters.foreach { parameter => map.put(parameter._1, parameter._2.toLowerCase) }
        val bucketFields = if (options.isBucketingEnabled) {
          if (options.bucketNumber.toString.contains("-") ||
              options.bucketNumber.toString.contains("+")) {
            throw new MalformedCarbonCommandException("INVALID NUMBER OF BUCKETS SPECIFIED" +
                                                      options.bucketNumber.toString)
          }
          else {
            Some(BucketFields(options.bucketColumns.toLowerCase.split(",").map(_.trim),
              options.bucketNumber))
          }
        } else {
          None
        }

        val cm = TableCreator.prepareTableModel(ifNotExistPresent = false, Option(dbName),
          tableName, fields, Nil, bucketFields, map)
        CreateTable(cm, false).run(sparkSession)
        CarbonEnv.getInstance(sparkSession).carbonMetastore.storePath + s"/$dbName/$tableName"
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
      tableName : String): String = {

    if (StringUtils.isBlank(tableName)) {
      throw new MalformedCarbonCommandException("The Specified Table Name is Blank")
    }
    if (tableName.contains(" ")) {
      throw new MalformedCarbonCommandException("Table Name Should not have spaces ")
    }
    try {
      CarbonEnv.getInstance(sparkSession).carbonMetastore
        .lookupRelation(Option(dbName), tableName)(sparkSession)
      CarbonEnv.getInstance(sparkSession).carbonMetastore.storePath + s"/$dbName/$tableName"
    } catch {
      case ex: Exception =>
        throw new Exception(s"Do not have $dbName and $tableName", ex)
    }
  }

}
