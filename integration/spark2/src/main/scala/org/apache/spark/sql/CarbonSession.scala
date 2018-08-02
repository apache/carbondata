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
import java.net.InetAddress
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.execution.command.CarbonSetCommand
import org.apache.spark.sql.internal.{SessionState, SharedState}
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.profiler.{Profiler, SQLStart}
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.common.annotations.InterfaceAudience
import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, CarbonSessionInfo, ThreadLocalSessionInfo}
import org.apache.carbondata.sdk.store.{CarbonStore, CarbonStoreFactory}
import org.apache.carbondata.sdk.store.conf.StoreConf
import org.apache.carbondata.sdk.store.descriptor.{ScanDescriptor, TableIdentifier}
import org.apache.carbondata.store.WorkerManager

/**
 * Session implementation for {org.apache.spark.sql.SparkSession}
 * Implemented this class only to use our own SQL DDL commands.
 * User needs to use {CarbonSession.getOrCreateCarbon} to create Carbon session.
 */
class CarbonSession(@transient val sc: SparkContext,
    @transient private val existingSharedState: Option[SharedState],
    @transient useHiveMetaStore: Boolean = true
) extends SparkSession(sc) { self =>

  def this(sc: SparkContext) {
    this(sc, None)
  }

  @transient
  override lazy val sessionState: SessionState = {
    CarbonReflectionUtils.getSessionState(sparkContext, this, useHiveMetaStore)
      .asInstanceOf[SessionState]
  }

  /**
   * State shared across sessions, including the `SparkContext`, cached data, listener,
   * and a catalog that interacts with external systems.
   */
  @transient
  override lazy val sharedState: SharedState = {
    existingSharedState match {
      case Some(_) =>
        val ss = existingSharedState.get
        if (ss == null) {
          new SharedState(sparkContext)
        } else {
          ss
        }
      case None =>
        new SharedState(sparkContext)
    }
  }

  override def newSession(): SparkSession = {
    new CarbonSession(sparkContext, Some(sharedState), useHiveMetaStore)
  }

  /**
   * Run search mode if enabled, otherwise run SparkSQL
   */
  override def sql(sqlText: String): DataFrame = {
    withProfiler(
      sqlText,
      (qe, sse) => {
        if (isSearchModeEnabled) {
          try {
            trySearchMode(qe, sse)
          } catch {
            case e: Exception =>
              log.error(String.format(
                "Exception when executing search mode: %s", e.getMessage))
              throw e;
          }
        } else {
          new Dataset[Row](self, qe, RowEncoder(qe.analyzed.schema))
        }
      }
    )
  }

  /**
   * Return true if the specified sql statement will hit the datamap
   * This API is for test purpose only
   */
  @InterfaceAudience.Developer(Array("DataMap"))
  def isDataMapHit(sqlStatement: String, dataMapName: String): Boolean = {
    val message = sql(s"EXPLAIN $sqlStatement").collect()
    message(0).getString(0).contains(dataMapName)
  }

  def isSearchModeEnabled: Boolean = store != null

  /**
   * Run SparkSQL directly
   */
  def sparkSql(sqlText: String): DataFrame = {
    withProfiler(
      sqlText,
      (qe, sse) => new Dataset[Row](self, qe, RowEncoder(qe.analyzed.schema))
    )
  }

  private def withProfiler(
      sqlText: String,
      generateDF: (QueryExecution, SQLStart) => DataFrame): DataFrame = {
    val sse = SQLStart(sqlText, CarbonSession.statementId.getAndIncrement())
    CarbonSession.threadStatementId.set(sse.statementId)
    sse.startTime = System.currentTimeMillis()

    try {
      val logicalPlan = sessionState.sqlParser.parsePlan(sqlText)
      sse.parseEnd = System.currentTimeMillis()

      val qe = sessionState.executePlan(logicalPlan)
      qe.assertAnalyzed()
      sse.isCommand = qe.analyzed match {
        case c: Command => true
        case u @ Union(children) if children.forall(_.isInstanceOf[Command]) => true
        case _ => false
      }
      sse.analyzerEnd = System.currentTimeMillis()
      generateDF(qe, sse)
    } finally {
      Profiler.invokeIfEnable {
        if (sse.isCommand) {
          sse.endTime = System.currentTimeMillis()
          Profiler.send(sse)
        } else {
          Profiler.addStatementMessage(sse.statementId, sse)
        }
      }
    }
  }

  /**
   * If the query is a simple query with filter, we will try to use Search Mode,
   * otherwise execute in SparkSQL
   */
  private def trySearchMode(qe: QueryExecution, sse: SQLStart): DataFrame = {
    val analyzed = qe.analyzed
    val LOG: LogService = LogServiceFactory.getLogService(this.getClass.getName)
    analyzed match {
      case _@Project(columns, _@Filter(expr, s: SubqueryAlias))
        if s.child.isInstanceOf[LogicalRelation] &&
           s.child.asInstanceOf[LogicalRelation].relation
             .isInstanceOf[CarbonDatasourceHadoopRelation] =>
        LOG.info(s"Search service started and supports filter: ${sse.sqlText}")
        runSearch(analyzed, columns, expr, s.child.asInstanceOf[LogicalRelation])
      case gl@GlobalLimit(_, ll@LocalLimit(_, p@Project(columns, _@Filter(expr, s: SubqueryAlias))))
        if s.child.isInstanceOf[LogicalRelation] &&
           s.child.asInstanceOf[LogicalRelation].relation
             .isInstanceOf[CarbonDatasourceHadoopRelation] =>
        val logicalRelation = s.child.asInstanceOf[LogicalRelation]
        LOG.info(s"Search service started and supports limit: ${sse.sqlText}")
        runSearch(analyzed, columns, expr, logicalRelation, gl.maxRows, ll.maxRows)
      case _ =>
        LOG.info(s"Search service started, but don't support: ${sse.sqlText}," +
          s" and will run it with SparkSQL")
        new Dataset[Row](self, qe, RowEncoder(qe.analyzed.schema))
    }
  }

  // variable that used in search mode
  @transient private var store: CarbonStore = _

  def startSearchMode(): Unit = {
    if (store == null) {
      val storeConf = new StoreConf()
      storeConf.conf(StoreConf.STORE_LOCATION, CarbonProperties.getStorePath)
      storeConf.conf(StoreConf.MASTER_HOST, InetAddress.getLocalHost.getHostAddress)
      storeConf.conf(StoreConf.REGISTRY_PORT, CarbonProperties.getSearchMasterPort)
      storeConf.conf(StoreConf.WORKER_HOST, InetAddress.getLocalHost.getHostAddress)
      storeConf.conf(StoreConf.WORKER_PORT, CarbonProperties.getSearchWorkerPort)
      storeConf.conf(StoreConf.WORKER_CORE_NUM, 2)

      store = CarbonStoreFactory.getRemoteStore("GlobalStore", storeConf)
      CarbonProperties.enableSearchMode(true)
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "false")
      WorkerManager.startAllWorker(this, storeConf)
    }
  }

  def stopSearchMode(): Unit = {
    if (store != null) {
      CarbonProperties.enableSearchMode(false)
      CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "true")
      try {
        CarbonStoreFactory.removeDistributedStore("GlobalStore")
        store = null
      } catch {
        case e: RuntimeException =>
          LogServiceFactory.getLogService(this.getClass.getCanonicalName)
            .error(s"Stop search mode failed: ${e.getMessage}")
          throw e
      }
    }
  }

  private def runSearch(
      logicalPlan: LogicalPlan,
      columns: Seq[NamedExpression],
      expr: Expression,
      relation: LogicalRelation,
      maxRows: Option[Long] = None,
      localMaxRows: Option[Long] = None): DataFrame = {
    val table = relation.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable
    val select = new ScanDescriptor(
      new TableIdentifier(table.getTableName, table.getDatabaseName),
      columns.map(_.name).toArray,
      if (expr != null) CarbonFilters.transformExpression(expr) else null,
      localMaxRows.getOrElse(Long.MaxValue)
    )
    val rows = store.scan(select).iterator()
    val output = new java.util.ArrayList[Row]()
    val maxRowCount = maxRows.getOrElse(Long.MaxValue)
    var rowCount = 0
    while (rows.hasNext && rowCount < maxRowCount) {
      val row = rows.next()
      output.add(Row.fromSeq(row.getData))
      rowCount = rowCount + 1
    }
    createDataFrame(output, logicalPlan.schema)
  }

}

object CarbonSession {

  private val statementId = new AtomicLong(0)

  private var enableInMemCatalog: Boolean = false

  private[sql] val threadStatementId = new ThreadLocal[Long]()

  implicit class CarbonBuilder(builder: Builder) {

    def enableInMemoryCatalog(): Builder = {
      enableInMemCatalog = true
      builder
    }
    def getOrCreateCarbonSession(): SparkSession = {
      getOrCreateCarbonSession(null, null)
    }

    def getOrCreateCarbonSession(storePath: String): SparkSession = {
      getOrCreateCarbonSession(
        storePath,
        new File(CarbonCommonConstants.METASTORE_LOCATION_DEFAULT_VAL).getCanonicalPath)
    }

    def getOrCreateCarbonSession(storePath: String,
        metaStorePath: String): SparkSession = synchronized {
      new CarbonSessionBuilder(builder).build(storePath, metaStorePath, enableInMemCatalog)
    }

    /**
     * It is a hack to get the private field from class.
     */
    def getValue(name: String, builder: Builder): Any = {
      val currentMirror = scala.reflect.runtime.currentMirror
      val instanceMirror = currentMirror.reflect(builder)
      val m = currentMirror.classSymbol(builder.getClass).
        toType.members.find { p =>
        p.name.toString.equals(name)
      }.get.asTerm
      instanceMirror.reflectField(m).get
    }
  }

  def threadSet(key: String, value: String): Unit = {
    var currentThreadSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (currentThreadSessionInfo == null) {
      currentThreadSessionInfo = new CarbonSessionInfo()
    }
    else {
      currentThreadSessionInfo = currentThreadSessionInfo.clone()
    }
    val threadParams = currentThreadSessionInfo.getThreadParams
    CarbonSetCommand.validateAndSetValue(threadParams, key, value)
    ThreadLocalSessionInfo.setCarbonSessionInfo(currentThreadSessionInfo)
  }


  def threadSet(key: String, value: Object): Unit = {
    var currentThreadSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (currentThreadSessionInfo == null) {
      currentThreadSessionInfo = new CarbonSessionInfo()
    }
    else {
      currentThreadSessionInfo = currentThreadSessionInfo.clone()
    }
    currentThreadSessionInfo.getThreadParams.setExtraInfo(key, value)
    ThreadLocalSessionInfo.setCarbonSessionInfo(currentThreadSessionInfo)
  }

  def threadUnset(key: String): Unit = {
    val currentThreadSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (currentThreadSessionInfo != null) {
      val currentThreadSessionInfoClone = currentThreadSessionInfo.clone()
      val threadParams = currentThreadSessionInfoClone.getThreadParams
      CarbonSetCommand.unsetValue(threadParams, key)
      threadParams.removeExtraInfo(key)
      ThreadLocalSessionInfo.setCarbonSessionInfo(currentThreadSessionInfoClone)
    }
  }

  def updateSessionInfoToCurrentThread(sparkSession: SparkSession): Unit = {
    val carbonSessionInfo = CarbonEnv.getInstance(sparkSession).carbonSessionInfo.clone()
    val currentThreadSessionInfoOrig = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (currentThreadSessionInfoOrig != null) {
      val currentThreadSessionInfo = currentThreadSessionInfoOrig.clone()
      // copy all the thread parameters to apply to session parameters
      currentThreadSessionInfo.getThreadParams.getAll.asScala
        .foreach(entry => carbonSessionInfo.getSessionParams.addProperty(entry._1, entry._2))
      carbonSessionInfo.setThreadParams(currentThreadSessionInfo.getThreadParams)
    }
    // preserve thread parameters across call
    ThreadLocalSessionInfo.setCarbonSessionInfo(carbonSessionInfo)
  }

}
