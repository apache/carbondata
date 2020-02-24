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
import java.util.concurrent.atomic.AtomicLong

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.mutation.merge.MergeDataSetBuilder
import org.apache.spark.sql.internal.{SessionState, SharedState, StaticSQLConf}
import org.apache.spark.sql.profiler.{Profiler, SQLStart}
import org.apache.spark.util.{CarbonReflectionUtils, Utils}

import org.apache.carbondata.common.annotations.InterfaceAudience
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.streaming.CarbonStreamingQueryListener

/**
 * Session implementation for {org.apache.spark.sql.SparkSession}
 * Implemented this class only to use our own SQL DDL commands.
 * User needs to use {CarbonSession.getOrCreateCarbon} to create Carbon session.
 */
class CarbonSession(@transient val sc: SparkContext,
    @transient private val existingSharedState: Option[SharedState],
    @transient private val useHiveMetaStore: Boolean = true
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
          new Dataset[Row](self, qe, RowEncoder(qe.analyzed.schema))
      }
    )
  }

  /**
   * Return true if the specified sql statement will hit the datamap
   * This API is for test purpose only
   */
  @InterfaceAudience.Developer(Array("DataMap"))
  def isDataMapHit(sqlStatement: String, dataMapName: String): Boolean = {
    // explain command will output the dataMap information only if enable.query.statistics = true
    val message = sql(s"EXPLAIN $sqlStatement").collect()
    message(0).getString(0).contains(dataMapName)
  }

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
}

object CarbonSession {

  private val statementId = new AtomicLong(0)

  private var enableInMemCatlog: Boolean = false

  private[sql] val threadStatementId = new ThreadLocal[Long]()

  implicit class CarbonBuilder(builder: Builder) {

    def enableInMemoryCatalog(): Builder = {
      enableInMemCatlog = true
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
      if (!enableInMemCatlog) {
        builder.enableHiveSupport()
      }
      val options =
        getValue("options", builder).asInstanceOf[scala.collection.mutable.HashMap[String, String]]
      val userSuppliedContext: Option[SparkContext] =
        getValue("userSuppliedContext", builder).asInstanceOf[Option[SparkContext]]
      CarbonReflectionUtils.updateCarbonSerdeInfo()

      if (StringUtils.isNotBlank(metaStorePath)) {
        val hadoopConf = new Configuration()
        val configFile = Utils.getContextOrSparkClassLoader.getResource("hive-site.xml")
        if (configFile != null) {
          hadoopConf.addResource(configFile)
        }
        if (options.get(CarbonCommonConstants.HIVE_CONNECTION_URL).isEmpty &&
            hadoopConf.get(CarbonCommonConstants.HIVE_CONNECTION_URL) == null) {
          val metaStorePathAbsolute = new File(metaStorePath).getCanonicalPath
          val hiveMetaStoreDB = metaStorePathAbsolute + "/metastore_db"
          options ++= Map[String, String]((CarbonCommonConstants.HIVE_CONNECTION_URL,
            s"jdbc:derby:;databaseName=$hiveMetaStoreDB;create=true"))
        }
      }

      // Get the session from current thread's active session.
      var session: SparkSession = SparkSession.getActiveSession match {
        case Some(sparkSession: CarbonSession) =>
          if ((sparkSession ne null) && !sparkSession.sparkContext.isStopped) {
            options.foreach { case (k, v) => sparkSession.sessionState.conf.setConfString(k, v) }
            sparkSession
          } else {
            null
          }
        case _ => null
      }
      if (session ne null) {
        return session
      }

      // Global synchronization so we will only set the default session once.
      SparkSession.synchronized {
        // If the current thread does not have an active session, get it from the global session.
        session = SparkSession.getDefaultSession match {
          case Some(sparkSession: CarbonSession) =>
            if ((sparkSession ne null) && !sparkSession.sparkContext.isStopped) {
              options.foreach { case (k, v) => sparkSession.sessionState.conf.setConfString(k, v) }
              sparkSession
            } else {
              null
            }
          case _ => null
        }
        if (session ne null) {
          return session
        }

        // No active nor global default session. Create a new one.
        val sparkContext = userSuppliedContext.getOrElse {
          // set app name if not given
          val randomAppName = java.util.UUID.randomUUID().toString
          val sparkConf = new SparkConf()
          options.foreach { case (k, v) => sparkConf.set(k, v) }
          if (!sparkConf.contains("spark.app.name")) {
            sparkConf.setAppName(randomAppName)
          }
          val sc = SparkContext.getOrCreate(sparkConf)
          // maybe this is an existing SparkContext, update its SparkConf which maybe used
          // by SparkSession
          options.foreach { case (k, v) => sc.conf.set(k, v) }
          if (!sc.conf.contains("spark.app.name")) {
            sc.conf.setAppName(randomAppName)
          }
          sc
        }

        session = new CarbonSession(sparkContext, None, !enableInMemCatlog)

        val carbonProperties = CarbonProperties.getInstance()
        if (StringUtils.isNotBlank(storePath)) {
          carbonProperties.addProperty(CarbonCommonConstants.STORE_LOCATION, storePath)
          // In case if it is in carbon.properties for backward compatible
        } else if (carbonProperties.getProperty(CarbonCommonConstants.STORE_LOCATION) == null) {
          carbonProperties.addProperty(CarbonCommonConstants.STORE_LOCATION,
            session.sessionState.conf.warehousePath)
        }
        options.foreach { case (k, v) => session.sessionState.conf.setConfString(k, v) }
        SparkSession.setDefaultSession(session)
        // Register a successfully instantiated context to the singleton. This should be at the
        // end of the class definition so that the singleton is updated only if there is no
        // exception in the construction of the instance.
        CarbonToSparkAdapter.addSparkListener(sparkContext)
        session.streams.addListener(new CarbonStreamingQueryListener(session))
      }

      session
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

  implicit class DataSetMerge(val ds: Dataset[Row]) {
    def merge(srcDS: Dataset[Row], expr: String): MergeDataSetBuilder = {
      new MergeDataSetBuilder(ds, srcDS, expr, ds.sparkSession)
    }

    def merge(srcDS: Dataset[Row], expr: Column): MergeDataSetBuilder = {
      new MergeDataSetBuilder(ds, srcDS, expr, ds.sparkSession)
    }
  }
}
