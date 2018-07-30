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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.profiler.Profiler
import org.apache.spark.util.Utils

import org.apache.carbondata.common.annotations.{InterfaceAudience, InterfaceStability}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil
import org.apache.carbondata.streaming.CarbonStreamingQueryListener

/**
 * Used for building CarbonSession, it can be called in Scala or Java
 * @param builder SparkSession builder
 */
@InterfaceAudience.User
@InterfaceStability.Evolving
class CarbonSessionBuilder(builder: Builder) {

  def build(
      storePath: String,
      metaStorePath: String,
      enableInMemCatalog: Boolean): SparkSession = synchronized {

    if (!enableInMemCatalog) {
      builder.enableHiveSupport()
    }
    val options =
      getValue("options", builder).asInstanceOf[scala.collection.mutable.HashMap[String, String]]
    val userSuppliedContext: Option[SparkContext] =
      getValue("userSuppliedContext", builder).asInstanceOf[Option[SparkContext]]

    if (metaStorePath != null) {
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
        CarbonInputFormatUtil.setS3Configurations(sc.hadoopConfiguration)
        // maybe this is an existing SparkContext, update its SparkConf which maybe used
        // by SparkSession
        options.foreach { case (k, v) => sc.conf.set(k, v) }
        if (!sc.conf.contains("spark.app.name")) {
          sc.conf.setAppName(randomAppName)
        }
        sc
      }

      session = new CarbonSession(sparkContext, None, !enableInMemCatalog)
      val carbonProperties = CarbonProperties.getInstance()
      if (storePath != null) {
        carbonProperties.addProperty(CarbonCommonConstants.STORE_LOCATION, storePath)
        // In case if it is in carbon.properties for backward compatible
      } else if (carbonProperties.getProperty(CarbonCommonConstants.STORE_LOCATION) == null) {
        carbonProperties.addProperty(CarbonCommonConstants.STORE_LOCATION,
          session.sessionState.conf.warehousePath)
      }
      options.foreach { case (k, v) => session.sessionState.conf.setConfString(k, v) }
      SparkSession.setDefaultSession(session)
      // Setup monitor end point and register CarbonMonitorListener
      Profiler.initialize(sparkContext)
      // Register a successfully instantiated context to the singleton. This should be at the
      // end of the class definition so that the singleton is updated only if there is no
      // exception in the construction of the instance.
      CarbonToSparkAdapater.addSparkListener(sparkContext)
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
