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

import java.sql.{DriverManager, ResultSet}

import scala.language.implicitConversions

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{JdbcRDDExt, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{Analyzer, OverrideCatalog, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.cubemodel.{LoadCubeAPI, MergeCube, PartitionData}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.hive._
import org.apache.spark.sql.jdbc.JdbcResultSetRDD
import org.apache.spark.sql.types.StructType

import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.util.CarbonProperties
import org.carbondata.integration.spark.agg.FlattenExpr
import org.carbondata.integration.spark.rdd.CarbonDataFrameRDD
import org.carbondata.integration.spark.util.CarbonSparkInterFaceLogEvent
import org.carbondata.query.aggregator.MeasureAggregator

class CarbonContext(val sc: SparkContext, metadataPath: String) extends HiveContext(sc) {
  self =>

  var lastSchemaUpdatedTime = System.currentTimeMillis()

  override lazy val catalog = new
      CarbonMetastoreCatalog(this, metadataPath, metadataHive) with OverrideCatalog

  @transient
  override protected[sql] lazy val analyzer =
    new Analyzer(catalog, functionRegistry, conf)

  override protected[sql] def dialectClassName = classOf[CarbonSQLDialect].getCanonicalName

  experimental.extraStrategies = CarbonStrategy.getStrategy(self) :: Nil

  def updateSchema(schemaPath: String, encrypted: Boolean = true, aggTablesGen: Boolean = false) {
    CarbonEnv.getInstance(this).carbonCatalog.updateCube(schemaPath, encrypted, aggTablesGen)(this)
  }

  def cubeExists(schemaName: String, cubeName: String): Boolean = {
    CarbonEnv.getInstance(this).carbonCatalog.cubeExists(Seq(schemaName, cubeName))(this)
  }

  def loadData(schemaName: String = null, cubeName: String, dataPath: String,
               dimFilesPath: String = null) {
    var schemaNameLocal = schemaName
    if (schemaNameLocal == null) {
      schemaNameLocal = "default"
    }
    var dimFilesPathLocal = dimFilesPath
    if (dimFilesPath == null) {
      dimFilesPathLocal = dataPath
    }
    CarbonContext.updateCarbonPorpertiesPath(this)
    LoadCubeAPI(schemaNameLocal, cubeName, dataPath, dimFilesPathLocal, null).run(this)
  }

  def mergeData(schemaName: String = null, cubeName: String, tableName: String) {
    var schemaNameLocal = schemaName
    if (schemaNameLocal == null) {
      schemaNameLocal = cubeName
    }
    MergeCube(schemaNameLocal, cubeName, tableName).run(this)
  }

  @DeveloperApi
  implicit def toAggregates(aggregate: MeasureAggregator): Double = aggregate.getDoubleValue()


  override def sql(sql: String): SchemaRDD = {
    // queryId will be unique for each query, creting query detail holder
    val queryId: String = System.nanoTime() + ""
    this.setConf("queryId", queryId)

    CarbonContext.updateCarbonPorpertiesPath(this)
    val sqlString = sql.toUpperCase
    val LOGGER = LogServiceFactory.getLogService(CarbonContext.getClass().getName())
    LOGGER
      .info(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Query [$sqlString]")
    val logicPlan: LogicalPlan = parseSql(sql)
    val result = new CarbonDataFrameRDD(sql: String, this, logicPlan)

    // We force query optimization to happen right away instead of letting it happen lazily like
    // when using the query DSL.  This is so DDL commands behave as expected.  This is only
    // generates the RDD lineage for DML queries, but do not perform any execution.
    result
  }

  /**
   * All the measure objects inside SchemaRDD will be flattened
   */

  def flattenRDD(rdd: SchemaRDD): SchemaRDD = {
    val fields = rdd.schema.fields.map { f =>
      new Column(FlattenExpr(UnresolvedAttribute(f.name)))
    }
    rdd.as(Symbol("carbon_flatten")).select(fields: _*)
  }

  /** Caches the specified table in-memory. */
  override def cacheTable(tableName: String): Unit = {
    // todo:
  }

  /**
   * Loads from JDBC, returning the ResultSet as a [[SchemaRDD]].
   * It gets MetaData from ResultSet of PreparedStatement to determine the schema.
   *
   * @group userf
   */
  def jdbcResultSet(
                     connectString: String,
                     sql: String): SchemaRDD = {
    jdbcResultSet(connectString, "", "", sql, 0, 0, 1)
  }

  def jdbcResultSet(
                     connectString: String,
                     username: String,
                     password: String,
                     sql: String): SchemaRDD = {
    jdbcResultSet(connectString, username, password, sql, 0, 0, 1)
  }

  def jdbcResultSet(
                     connectString: String,
                     sql: String,
                     lowerBound: Long,
                     upperBound: Long,
                     numPartitions: Int): SchemaRDD = {
    jdbcResultSet(connectString, "", "", sql, lowerBound, upperBound, numPartitions)
  }

  def jdbcResultSet(
                     connectString: String,
                     username: String,
                     password: String,
                     sql: String,
                     lowerBound: Long,
                     upperBound: Long,
                     numPartitions: Int): SchemaRDD = {
    val resultSetRDD = new JdbcRDDExt(
      sparkContext,
      () => {
        DriverManager.getConnection(connectString, username, password)
      },
      sql, lowerBound, upperBound, numPartitions,
      (r: ResultSet) => r
    )
    val appliedSchema = JdbcResultSetRDD.inferSchema(resultSetRDD)
    val rowRDD = JdbcResultSetRDD.jdbcResultSetToRow(resultSetRDD, appliedSchema)
    applySchema1(rowRDD, appliedSchema)
  }

  def applySchema1(rowRDD: RDD[InternalRow], schema: StructType): DataFrame = {
    // TODO: use MutableProjection when rowRDD is another SchemaRDD and the applied
    // schema differs from the existing schema on any field data type.
    val attributes = schema.fields.map(f => AttributeReference(f.name, f.dataType, f.nullable)())
    val logicalPlan = LogicalRDD(attributes, rowRDD)(this)
    new DataFrame(this, logicalPlan)
  }


}

object CarbonContext {
  /**
   * @param schemaName - Schema Name
   * @param cubeName   - Cube Name
   * @param factPath   - Raw CSV data path
   * @param targetPath - Target path where the file will be split as per partition
   * @param delimiter  - default file delimiter is comma(,)
   * @param quoteChar  - default quote character used in Raw CSV file, Default quote
   *                   character is double quote(")
   * @param fileHeader - Header should be passed if not available in Raw CSV File, else pass null,
   *                   Header will be read from CSV
   * @param escapeChar - This parameter by default will be null, there wont be any validation if
   *                   default escape character(\) is found on the RawCSV file
   * @param multiLine  - This parameter will be check for end of quote character if escape character
   *                     & quote character is set.
   *                   if set as false, it will check for end of quote character within the line
   *                   and skips only 1 line if end of quote not found
   *                   if set as true, By default it will check for 10000 characters in multiple
   *                   lines for end of quote & skip all lines if end of quote not found.
   */
  final def partitionData(
                           schemaName: String = null,
                           cubeName: String,
                           factPath: String,
                           targetPath: String,
                           delimiter: String = ",",
                           quoteChar: String = "\"",
                           fileHeader: String = null,
                           escapeChar: String = null,
                           multiLine: Boolean = false)(hiveContext: HiveContext): String = {
    updateCarbonPorpertiesPath(hiveContext)
    var schemaNameLocal = schemaName
    if (schemaNameLocal == null) {
      schemaNameLocal = "default"
    }
    val partitionDataClass = PartitionData(schemaName, cubeName, factPath, targetPath, delimiter,
      quoteChar, fileHeader, escapeChar, multiLine)
    partitionDataClass.run(hiveContext)
    partitionDataClass.partitionStatus
  }

  final def updateCarbonPorpertiesPath(hiveContext: HiveContext) {
    val carbonPropertiesFilePath = hiveContext.getConf("carbon.properties.filepath", null)
    val systemcarbonPropertiesFilePath = System.getProperty("carbon.properties.filepath", null);
    if (null != carbonPropertiesFilePath && null == systemcarbonPropertiesFilePath) {
      System.setProperty("carbon.properties.filepath",
        carbonPropertiesFilePath + "/" + "carbon.properties")
    }
    // configuring the zookeeper URl .
    var zooKeeperUrl = hiveContext.getConf("spark.deploy.zookeeper.url", "127.0.0.1:2181")

    CarbonProperties.getInstance().addProperty("spark.deploy.zookeeper.url", zooKeeperUrl)

  }

}
