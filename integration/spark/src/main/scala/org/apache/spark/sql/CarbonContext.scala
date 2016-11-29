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

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.sql.catalyst.ParserDialect
import org.apache.spark.sql.catalyst.analysis.{Analyzer, OverrideCatalog}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.ExtractPythonUDFs
import org.apache.spark.sql.execution.datasources.{PreInsertCastAndRename, PreWriteCheck}
import org.apache.spark.sql.hive._
import org.apache.spark.sql.optimizer.CarbonOptimizer

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.carbon.querystatistics.{QueryStatistic, QueryStatisticsConstants}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, CarbonTimeStatisticsFactory}

class CarbonContext(
    val sc: SparkContext,
    val storePath: String,
    metaStorePath: String) extends HiveContext(sc) {
  self =>

  def this(sc: SparkContext) = {
    this(sc,
      new File(CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL).getCanonicalPath,
      new File(CarbonCommonConstants.METASTORE_LOCATION_DEFAULT_VAL).getCanonicalPath)
  }

  def this(sc: SparkContext, storePath: String) = {
    this(sc,
      storePath,
      new File(CarbonCommonConstants.METASTORE_LOCATION_DEFAULT_VAL).getCanonicalPath)
  }

  CarbonContext.addInstance(sc, this)
  CodeGenerateFactory.init(sc.version)
  CarbonEnv.init(this)

  var lastSchemaUpdatedTime = System.currentTimeMillis()
  val hiveClientInterface = metadataHive

  protected[sql] override lazy val conf: SQLConf = new CarbonSQLConf

  @transient
  override lazy val catalog = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.STORE_LOCATION, storePath)
    new CarbonMetastore(this, storePath, metadataHive, queryId) with OverrideCatalog
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
      logDebug(s"metastore db is going to be created in location: $hiveMetaStoreDB")
      super.configure() ++ Map((CarbonCommonConstants.HIVE_CONNECTION_URL,
        s"jdbc:derby:;databaseName=$hiveMetaStoreDB;create=true"),
        ("hive.metastore.warehouse.dir", metaStorePathAbsolute + "/hivemetadata"))
    } else {
      super.configure()
    }
  }

  @transient
  val LOGGER = LogServiceFactory.getLogService(CarbonContext.getClass.getName)

  var queryId: String = ""

  override def sql(sql: String): DataFrame = {
    // queryId will be unique for each query, creting query detail holder
    queryId = System.nanoTime() + ""
    this.setConf("queryId", queryId)

    CarbonContext.updateCarbonPorpertiesPath(this)
    val sqlString = sql.toUpperCase
    LOGGER.info(s"Query [$sqlString]")
    val recorder = CarbonTimeStatisticsFactory.createDriverRecorder()
    val statistic = new QueryStatistic()
    val logicPlan: LogicalPlan = parseSql(sql)
    statistic.addStatistics(QueryStatisticsConstants.SQL_PARSE, System.currentTimeMillis())
    recorder.recordStatisticsForDriver(statistic, queryId)
    val result = new DataFrame(this, logicPlan)

    // We force query optimization to happen right away instead of letting it happen lazily like
    // when using the query DSL.  This is so DDL commands behave as expected.  This is only
    // generates the RDD lineage for DML queries, but do not perform any execution.
    result
  }

}

object CarbonContext {

  val datasourceName: String = "org.apache.carbondata.format"

  val datasourceShortName: String = "carbondata"

  @transient
  val LOGGER = LogServiceFactory.getLogService(CarbonContext.getClass.getName)

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
