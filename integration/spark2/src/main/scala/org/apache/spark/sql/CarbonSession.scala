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
import org.apache.spark.sql.hive.CarbonSessionState
import org.apache.spark.sql.internal.{SessionState, SharedState}
import org.apache.spark.util.Utils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.datastore.row.LoadStatusType
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonStorePath

/**
 * Session implementation for {org.apache.spark.sql.SparkSession}
 * Implemented this class only to use our own SQL DDL commands.
 * User needs to use {CarbonSession.getOrCreateCarbon} to create Carbon session.
 */
class CarbonSession(@transient val sc: SparkContext,
    @transient private val existingSharedState: Option[SharedState]) extends SparkSession(sc) {

  def this(sc: SparkContext) {
    this(sc, None)
  }

  @transient
  override lazy val sessionState: SessionState = new CarbonSessionState(this)

  /**
   * State shared across sessions, including the `SparkContext`, cached data, listener,
   * and a catalog that interacts with external systems.
   */
  @transient
 override private[sql] lazy val sharedState: SharedState = {
    existingSharedState.getOrElse(new SharedState(sparkContext))
  }

  override def newSession(): SparkSession = {
    new CarbonSession(sparkContext, Some(sharedState))
  }

}

object CarbonSession {

  implicit class CarbonBuilder(builder: Builder) {

    private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

    def getOrCreateCarbonSession(): SparkSession = {
      getOrCreateCarbonSession(
        null,
        new File(CarbonCommonConstants.METASTORE_LOCATION_DEFAULT_VAL).getCanonicalPath)
    }

    def getOrCreateCarbonSession(storePath: String): SparkSession = {
      getOrCreateCarbonSession(
        storePath,
        new File(CarbonCommonConstants.METASTORE_LOCATION_DEFAULT_VAL).getCanonicalPath)
    }

    def getOrCreateCarbonSession(storePath: String,
        metaStorePath: String): SparkSession = synchronized {
      builder.enableHiveSupport()
      val options =
        getValue("options", builder).asInstanceOf[scala.collection.mutable.HashMap[String, String]]
      val userSuppliedContext: Option[SparkContext] =
        getValue("userSuppliedContext", builder).asInstanceOf[Option[SparkContext]]
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
        val carbonProperties = CarbonProperties.getInstance()
        if (storePath != null) {
          carbonProperties.addProperty(CarbonCommonConstants.STORE_LOCATION, storePath)
          // In case if it is in carbon.properties for backward compatible
        } else if (carbonProperties.getProperty(CarbonCommonConstants.STORE_LOCATION) == null) {
          carbonProperties.addProperty(CarbonCommonConstants.STORE_LOCATION,
            sparkContext.conf.get("spark.sql.warehouse.dir"))
        }
        session = new CarbonSession(sparkContext)
        options.foreach { case (k, v) => session.sessionState.conf.setConfString(k, v) }
        SparkSession.setDefaultSession(session)
        cleanInProgressSegments(
          carbonProperties.getProperty(CarbonCommonConstants.STORE_LOCATION), sparkContext)
        // Register a successfully instantiated context to the singleton. This should be at the
        // end of the class definition so that the singleton is updated only if there is no
        // exception in the construction of the instance.
        sparkContext.addSparkListener(new SparkListener {
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
            SparkSession.setDefaultSession(null)
            SparkSession.sqlListener.set(null)
          }
        })
      }

      return session
    }

    /**
     * The in-progress segments which are left when the driver is down will be marked as deleted
     * when driver is initializing.
     */
    private def cleanInProgressSegments(storePath: String, sparkContext: SparkContext): Unit = {
      val loaderDriver = CarbonProperties.getInstance().
        getProperty(CarbonCommonConstants.DATA_MANAGEMENT_DRIVER,
          CarbonCommonConstants.DATA_MANAGEMENT_DRIVER_DEFAULT).toBoolean
      if (!loaderDriver) {
        return
      }
      try {
        val fileType = FileFactory.getFileType(storePath)
        if (FileFactory.isFileExist(storePath, fileType)) {
          val file = FileFactory.getCarbonFile(storePath, fileType)
          val databaseFolders = file.listFiles()
          databaseFolders.foreach { databaseFolder =>
            if (databaseFolder.isDirectory) {
              val tableFolders = databaseFolder.listFiles()
              tableFolders.foreach { tableFolder =>
                if (tableFolder.isDirectory) {
                  val identifier =
                    AbsoluteTableIdentifier.from(storePath,
                      databaseFolder.getName, tableFolder.getName)
                  val carbonTablePath = CarbonStorePath.getCarbonTablePath(identifier)
                  val tableStatusFile = carbonTablePath.getTableStatusFilePath
                  if (FileFactory.isFileExist(tableStatusFile, fileType)) {
                    val segmentStatusManager = new SegmentStatusManager(identifier)
                    val carbonLock = segmentStatusManager.getTableStatusLock
                    try {
                      if (carbonLock.lockWithRetries) {
                        LOGGER.info("Acquired lock for table" +
                                    identifier.getCarbonTableIdentifier.getTableUniqueName
                                    + " for table status updation")
                        val listOfLoadFolderDetailsArray =
                          SegmentStatusManager.readLoadMetadata(
                            carbonTablePath.getMetadataDirectoryPath)
                        var loadInprogressExist = false
                        val staleFolders: Seq[CarbonFile] = Seq()
                        listOfLoadFolderDetailsArray.foreach { load =>
                          if (load.getLoadStatus.equals(LoadStatusType.IN_PROGRESS.getMessage) ||
                              load.getLoadStatus.equals(LoadStatusType.INSERT_OVERWRITE.getMessage))
                          {
                            load.setLoadStatus(CarbonCommonConstants.MARKED_FOR_DELETE)
                            staleFolders :+ FileFactory.getCarbonFile(
                              carbonTablePath.getCarbonDataDirectoryPath("0", load.getLoadName))
                            loadInprogressExist = true
                          }
                        }
                        if (loadInprogressExist) {
                          SegmentStatusManager
                            .writeLoadDetailsIntoFile(tableStatusFile, listOfLoadFolderDetailsArray)
                          staleFolders.foreach(CarbonUtil.deleteFoldersAndFiles(_))
                        }
                      }
                    } finally {
                      if (carbonLock.unlock) {
                        LOGGER.info(s"Released table status lock for table " +
                                    s"${identifier.getCarbonTableIdentifier.getTableUniqueName}")
                      } else {
                        LOGGER.error(s"Error while releasing table status lock for table " +
                                     s"${identifier.getCarbonTableIdentifier.getTableUniqueName}")
                      }
                    }
                  }
                }
              }
            }
          }
        }
      } catch {
        case s: java.io.FileNotFoundException =>
          // Create folders and files.
          LOGGER.error(s)
      }
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

}
