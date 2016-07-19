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

import java.io.File

import scala.language.implicitConversions

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.sql.catalyst.{CatalystConf, ParserDialect}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, OverrideCatalog}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.ExtractPythonUDFs
import org.apache.spark.sql.execution.command.PartitionData
import org.apache.spark.sql.execution.datasources.{PreInsertCastAndRename, PreWriteCheck}
import org.apache.spark.sql.hive._
import org.apache.spark.sql.optimizer.CarbonOptimizer
import org.apache.spark.util.Utils

import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.carbondata.spark.rdd.CarbonDataFrameRDD

class CarbonContext(
    val sc: SparkContext,
    val storePath: String,
    metaStorePath: String) extends HiveContext(sc) with Logging {
  self =>

  def this (sc: SparkContext) = {
    this (sc,
      new File(CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL).getCanonicalPath,
      new File(CarbonCommonConstants.METASTORE_LOCATION_DEFAULT_VAL).getCanonicalPath)
  }

  def this (sc: SparkContext, storePath: String) = {
    this (sc,
      storePath,
      new File(CarbonCommonConstants.METASTORE_LOCATION_DEFAULT_VAL).getCanonicalPath)
  }

  CarbonContext.addInstance(sc, this)
  CodeGenerateFactory.init(sc.version)

  var lastSchemaUpdatedTime = System.currentTimeMillis()

  protected[sql] override lazy val conf: SQLConf = new CarbonSQLConf

  @transient
  override lazy val catalog = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.STORE_LOCATION, storePath)
    new CarbonMetastoreCatalog(this, storePath, metadataHive) with OverrideCatalog
  }

  @transient
  override protected[sql] lazy val analyzer =
    new Analyzer(catalog, functionRegistry, conf) {
      override val extendedResolutionRules =
        catalog.ParquetConversions ::
        catalog.CreateTables ::
        catalog.PreInsertionCasts ::
        ExtractPythonUDFs ::
        ResolveHiveWindowFunction ::
        PreInsertCastAndRename ::
        Nil

      override val extendedCheckRules = Seq(
        PreWriteCheck(catalog)
      )
    }

  @transient
  override protected[sql] lazy val optimizer: Optimizer =
    CarbonOptimizer.optimizer(
      CodeGenerateFactory.createDefaultOptimizer(conf, sc),
      conf.asInstanceOf[CarbonSQLConf],
      sc.version)

  protected[sql] override def getSQLDialect(): ParserDialect = new CarbonSQLDialect(this)

  experimental.extraStrategies = {
    val carbonStrategy = new CarbonStrategies(self)
    Seq(carbonStrategy.CarbonTableScan, carbonStrategy.DDLStrategies)
  }

  override protected def configure(): Map[String, String] = {
    sc.hadoopConfiguration.addResource("hive-site.xml")
    if (sc.hadoopConfiguration.get(CarbonCommonConstants.HIVE_CONNECTION_URL) == null) {
      val metaStorePathAbsolute = new File(metaStorePath).getCanonicalPath
      val hiveMetaStoreDB = metaStorePathAbsolute + "/metastore_db"
      logDebug(s"metastore db is going to be created in location : $hiveMetaStoreDB")
      super.configure() ++ Map((CarbonCommonConstants.HIVE_CONNECTION_URL,
              s"jdbc:derby:;databaseName=$hiveMetaStoreDB;create=true"),
        ("hive.metastore.warehouse.dir", metaStorePathAbsolute + "/hivemetadata"))
    } else {
      super.configure()
    }
  }

  @transient
  val LOGGER = LogServiceFactory.getLogService(CarbonContext.getClass.getName)

  override def sql(sql: String): SchemaRDD = {
    // queryId will be unique for each query, creting query detail holder
    val queryId: String = System.nanoTime() + ""
    this.setConf("queryId", queryId)

    CarbonContext.updateCarbonPorpertiesPath(this)
    val sqlString = sql.toUpperCase
    LOGGER.info(s"Query [$sqlString]")
    val logicPlan: LogicalPlan = parseSql(sql)
    val result = new CarbonDataFrameRDD(this, logicPlan)

    // We force query optimization to happen right away instead of letting it happen lazily like
    // when using the query DSL.  This is so DDL commands behave as expected.  This is only
    // generates the RDD lineage for DML queries, but do not perform any execution.
    result
  }

}

object CarbonContext {

  val datasourceName: String = "org.apache.carbondata.format"

  val datasourceShortName: String = "carbondata"

  /**
   * @param databaseName - Database Name
   * @param tableName   - Table Name
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
   *                   & quote character is set.
   *                   if set as false, it will check for end of quote character within the line
   *                   and skips only 1 line if end of quote not found
   *                   if set as true, By default it will check for 10000 characters in multiple
   *                   lines for end of quote & skip all lines if end of quote not found.
   */
  final def partitionData(
      databaseName: String = null,
      tableName: String,
      factPath: String,
      targetPath: String,
      delimiter: String = ",",
      quoteChar: String = "\"",
      fileHeader: String = null,
      escapeChar: String = null,
      multiLine: Boolean = false)(hiveContext: HiveContext): String = {
    updateCarbonPorpertiesPath(hiveContext)
    var databaseNameLocal = databaseName
    if (databaseNameLocal == null) {
      databaseNameLocal = "default"
    }
    val partitionDataClass = PartitionData(databaseName, tableName, factPath, targetPath, delimiter,
      quoteChar, fileHeader, escapeChar, multiLine)
    partitionDataClass.run(hiveContext)
    partitionDataClass.partitionStatus
  }

  final def updateCarbonPorpertiesPath(hiveContext: HiveContext) {
    val carbonPropertiesFilePath = hiveContext.getConf("carbon.properties.filepath", null)
    val systemcarbonPropertiesFilePath = System.getProperty("carbon.properties.filepath", null)
    if (null != carbonPropertiesFilePath && null == systemcarbonPropertiesFilePath) {
      System.setProperty("carbon.properties.filepath",
        carbonPropertiesFilePath + "/" + "carbon.properties")
    }
    // configuring the zookeeper URl .
    val zooKeeperUrl = hiveContext.getConf("spark.deploy.zookeeper.url", "127.0.0.1:2181")

    CarbonProperties.getInstance().addProperty("spark.deploy.zookeeper.url", zooKeeperUrl)

  }

  // this cache is used to avoid creating multiple CarbonContext from same SparkContext,
  // to avoid the derby problem for metastore
  private val cache = collection.mutable.Map[SparkContext, CarbonContext]()

  def getInstance(sc: SparkContext): CarbonContext = {
    cache(sc)
  }

  def addInstance(sc: SparkContext, cc: CarbonContext): Unit = {
    if (cache.contains(sc)) {
      sys.error("creating multiple instances of CarbonContext is not " +
                "allowed using the same SparkContext instance")
    }
    cache(sc) = cc
  }

}
