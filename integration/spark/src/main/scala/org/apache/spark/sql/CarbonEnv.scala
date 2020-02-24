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

import java.io.IOException
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.events.{MergeBloomIndexEventListener, MergeIndexEventListener}
import org.apache.spark.sql.hive._
import org.apache.spark.sql.listeners._
import org.apache.spark.sql.profiler.Profiler
import org.apache.spark.sql.secondaryindex.events._
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util._
import org.apache.carbondata.datamap.{TextMatchMaxDocUDF, TextMatchUDF}
import org.apache.carbondata.events._
import org.apache.carbondata.geo.InPolygonUDF
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadTablePostExecutionEvent, LoadTablePostStatusUpdateEvent, LoadTablePreExecutionEvent, LoadTablePreStatusUpdateEvent}
import org.apache.carbondata.spark.rdd.SparkReadSupport
import org.apache.carbondata.spark.readsupport.SparkRowReadSupportImpl

/**
 * Carbon Environment for unified context
 */
class CarbonEnv {

  var carbonMetaStore: CarbonMetaStore = _

  var sessionParams: SessionParams = _

  var carbonSessionInfo: CarbonSessionInfo = _

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  // set readSupport class global so that the executor can get it.
  SparkReadSupport.readSupportClass = classOf[SparkRowReadSupportImpl]

  var initialized = false

  def init(sparkSession: SparkSession): Unit = {
    val properties = CarbonProperties.getInstance()
    var storePath = properties.getProperty(CarbonCommonConstants.STORE_LOCATION)
    if (storePath == null) {
      storePath = FileFactory.getUpdatedFilePath(sparkSession.conf.get("spark.sql.warehouse.dir"))
      properties.addProperty(CarbonCommonConstants.STORE_LOCATION, storePath)
    }
    LOGGER.info(s"Initializing CarbonEnv, store location: $storePath")
    // Creating the index server temp folder where splits for select query is written
    CarbonUtil.createTempFolderForIndexServer(null);

    sparkSession.udf.register("getTupleId", () => "")
    sparkSession.udf.register("getPositionId", () => "")
    sparkSession.udf.register("NI", (anyRef: AnyRef) => true)

    // register for lucene datamap
    // TODO: move it to proper place, it should be registered by datamap implementation
    sparkSession.udf.register("text_match", new TextMatchUDF)
    sparkSession.udf.register("text_match_with_limit", new TextMatchMaxDocUDF)
    sparkSession.udf.register("in_polygon", new InPolygonUDF)

    // acquiring global level lock so global configuration will be updated by only one thread
    CarbonEnv.carbonEnvMap.synchronized {
      if (!initialized) {
        // update carbon session parameters , preserve thread parameters
        val currentThreadSesssionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
        carbonSessionInfo = new CarbonSessionInfo()
        // We should not corrupt the information in carbonSessionInfo object which is at the
        // session level. Instead create a new object and in that set the user specified values in
        // thread/session params
        val threadLevelCarbonSessionInfo = new CarbonSessionInfo()
        if (currentThreadSesssionInfo != null) {
          threadLevelCarbonSessionInfo.setThreadParams(currentThreadSesssionInfo.getThreadParams)
        }
        ThreadLocalSessionInfo.setCarbonSessionInfo(threadLevelCarbonSessionInfo)
        ThreadLocalSessionInfo.setConfigurationToCurrentThread(sparkSession
          .sessionState.newHadoopConf())
        val config = new CarbonSQLConf(sparkSession)
        if (sparkSession.conf.getOption(CarbonCommonConstants.ENABLE_UNSAFE_SORT).isEmpty) {
          config.addDefaultCarbonParams()
        }
        // add session params after adding DefaultCarbonParams
        config.addDefaultCarbonSessionParams()
        carbonMetaStore = {
          // trigger event for CarbonEnv create
          val operationContext = new OperationContext
          val carbonEnvInitPreEvent: CarbonEnvInitPreEvent =
            CarbonEnvInitPreEvent(sparkSession, carbonSessionInfo, storePath)
          OperationListenerBus.getInstance.fireEvent(carbonEnvInitPreEvent, operationContext)

          CarbonMetaStoreFactory.createCarbonMetaStore(sparkSession.conf)
        }
        CarbonProperties.getInstance
          .addNonSerializableProperty(CarbonCommonConstants.IS_DRIVER_INSTANCE, "true")
        initialized = true
        cleanChildTablesNotRegisteredInHive(sparkSession)
      }
    }
    Profiler.initialize(sparkSession.sparkContext)
    LOGGER.info("Initialize CarbonEnv completed...")
  }

  private def cleanChildTablesNotRegisteredInHive(sparkSession: SparkSession): Unit = {
    // If in case JDBC application is killed/stopped, when create datamap was in progress, datamap
    // table was created and datampschema was saved to the system, but table was not registered to
    // metastore. So, when we restart JDBC application, we need to clean up
    // stale tables and datamapschema's.
    val dataMapSchemas = DataMapStoreManager.getInstance().getAllDataMapSchemas
    dataMapSchemas.asScala.foreach {
      dataMapSchema =>
        if (null != dataMapSchema.getRelationIdentifier &&
            !dataMapSchema.isIndexDataMap) {
          val isTableExists = try {
            sparkSession.sessionState
              .catalog
              .tableExists(TableIdentifier(dataMapSchema.getRelationIdentifier.getTableName,
                Some(dataMapSchema.getRelationIdentifier.getDatabaseName)))
          } catch {
            // we need to take care of cleanup when the table does not exists, if table exists and
            // some other user tries to access the table, it might fail, that time no need to handle
            case ex: Exception =>
              LOGGER.error("Error while checking the table existence", ex)
              return
          }
          if (!isTableExists) {
            try {
              DataMapStoreManager.getInstance().dropDataMapSchema(dataMapSchema.getDataMapName)
            } catch {
              case e: IOException =>
                throw e
            } finally {
              if (FileFactory.isFileExist(dataMapSchema.getRelationIdentifier.getTablePath)) {
                CarbonUtil.deleteFoldersAndFilesSilent(FileFactory.getCarbonFile(dataMapSchema
                  .getRelationIdentifier
                  .getTablePath))
              }
            }
          }
        }
    }
  }
}

/**
 * @Deprecated
 */
object CarbonEnv {

  val carbonEnvMap = new ConcurrentHashMap[SparkSession, CarbonEnv]

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def getInstance(sparkSession: SparkSession): CarbonEnv = {
      var carbonEnv: CarbonEnv = carbonEnvMap.get(sparkSession)
      if (carbonEnv == null) {
        carbonEnv = new CarbonEnv
        carbonEnv.init(sparkSession)
        addSparkSessionListener(sparkSession)
        carbonEnvMap.put(sparkSession, carbonEnv)
      }
      carbonEnv
  }

  private def addSparkSessionListener(sparkSession: SparkSession): Unit = {
    sparkSession.sparkContext.addSparkListener(new SparkListener {
      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
        CarbonEnv.carbonEnvMap.remove(sparkSession)
        ThreadLocalSessionInfo.unsetAll()
      }
    })
  }

  /**
   * Method
   * 1. To initialize Listeners to their respective events in the OperationListenerBus
   * 2. To register common listeners
   * 3. Only initialize once for all the listeners in case of concurrent scenarios we have given
   * val, as val initializes once
   */
  def init(): Unit = {
    initListeners()
    CarbonReflectionUtils.updateCarbonSerdeInfo()
  }

  /**
   * Method to initialize Listeners to their respective events in the OperationListenerBus.
   */
  def initListeners(): Unit = {
    OperationListenerBus.getInstance()
      .addListener(classOf[IndexServerLoadEvent], PrePrimingEventListener)
      .addListener(classOf[LoadTablePreExecutionEvent], LoadMVTablePreListener)
      .addListener(classOf[AlterTableCompactionPreStatusUpdateEvent],
        AlterDataMaptableCompactionPostListener)
      .addListener(classOf[LoadTablePreStatusUpdateEvent], new MergeIndexEventListener)
      .addListener(classOf[LoadTablePostExecutionEvent], LoadPostDataMapListener)
      .addListener(classOf[UpdateTablePostEvent], LoadPostDataMapListener )
      .addListener(classOf[DeleteFromTablePostEvent], LoadPostDataMapListener )
      .addListener(classOf[AlterTableMergeIndexEvent], new MergeIndexEventListener)
      .addListener(classOf[BuildDataMapPostExecutionEvent], new MergeBloomIndexEventListener)
      .addListener(classOf[DropTableCacheEvent], DropCacheDataMapEventListener)
      .addListener(classOf[DropTableCacheEvent], DropCacheBloomEventListener)
      .addListener(classOf[ShowTableCacheEvent], ShowCachePreMVEventListener)
      .addListener(classOf[ShowTableCacheEvent], ShowCacheDataMapEventListener)
      .addListener(classOf[DeleteSegmentByIdPreEvent], DataMapDeleteSegmentPreListener)
      .addListener(classOf[DeleteSegmentByDatePreEvent], DataMapDeleteSegmentPreListener)
      .addListener(classOf[AlterTableDropColumnPreEvent], DataMapDropColumnPreListener)
      .addListener(classOf[AlterTableColRenameAndDataTypeChangePreEvent],
        DataMapChangeDataTypeorRenameColumnPreListener)
      .addListener(classOf[AlterTableAddColumnPreEvent], DataMapAddColumnsPreListener)
      .addListener(classOf[AlterTableDropPartitionMetaEvent],
        DataMapAlterTableDropPartitionMetaListener)
      .addListener(classOf[AlterTableDropPartitionPreStatusEvent],
        DataMapAlterTableDropPartitionPreStatusListener)
      .addListener(classOf[LoadTablePreStatusUpdateEvent], new SILoadEventListener)
      .addListener(classOf[LoadTablePostStatusUpdateEvent],
        new SILoadEventListenerForFailedSegments)
      .addListener(classOf[LookupRelationPostEvent], new SIRefreshEventListener)
      // TODO: get create relation event
      .addListener(classOf[CreateCarbonRelationPostEvent], new
          CreateCarbonRelationEventListener
      )
      .addListener(classOf[DropTablePreEvent], new SIDropEventListener)
      .addListener(classOf[AlterTableDropColumnPreEvent], new AlterTableDropColumnEventListener)
      .addListener(classOf[AlterTableRenamePostEvent], new AlterTableRenameEventListener)
      .addListener(classOf[AlterTableColRenameAndDataTypeChangePreEvent],
        new AlterTableColumnRenameEventListener)
      .addListener(classOf[AlterTableColRenameAndDataTypeChangePostEvent],
        new AlterTableColumnRenameEventListener)
      .addListener(classOf[DeleteSegmentByIdPostEvent], new DeleteSegmentByIdListener)
      .addListener(classOf[DeleteSegmentByDatePostEvent], new DeleteSegmentByDateListener)
      .addListener(classOf[CleanFilesPostEvent], new CleanFilesPostEventListener)
      .addListener(classOf[AlterTableCompactionPreStatusUpdateEvent],
        new AlterTableCompactionPostEventListener)
      .addListener(classOf[AlterTableMergeIndexEvent],
        new AlterTableMergeIndexSIEventListener)
      .addListener(classOf[UpdateTablePreEvent], new UpdateTablePreEventListener)
      .addListener(classOf[DeleteFromTablePostEvent], new DeleteFromTableEventListener)
      .addListener(classOf[DeleteFromTablePreEvent], new DeleteFromTableEventListener)
      .addListener(classOf[DropTableCacheEvent], DropCacheSIEventListener)
      .addListener(classOf[ShowTableCacheEvent], ShowCacheSIEventListener)
  }

  /**
   * Return carbon table instance from cache or by looking up table in `sparkSession`
   */
  def getCarbonTable(
      databaseNameOp: Option[String],
      tableName: String)
    (sparkSession: SparkSession): CarbonTable = {
    val catalog = getInstance(sparkSession).carbonMetaStore
    // if relation is not refreshed of the table does not exist in cache then
    if (isRefreshRequired(TableIdentifier(tableName, databaseNameOp))(sparkSession)) {
      catalog
        .lookupRelation(databaseNameOp, tableName)(sparkSession)
        .asInstanceOf[CarbonRelation]
        .carbonTable
    } else {
      CarbonMetadata.getInstance().getCarbonTable(databaseNameOp.getOrElse(sparkSession
        .catalog.currentDatabase), tableName)
    }
  }

  /**
   *
   * @return true is the relation was changes and was removed from cache. false is there is no
   *         change in the relation.
   */
  def isRefreshRequired(identifier: TableIdentifier)(sparkSession: SparkSession): Boolean = {
    val carbonEnv = getInstance(sparkSession)
    val databaseName = identifier.database.getOrElse(sparkSession.catalog.currentDatabase)
    val table = CarbonMetadata.getInstance().getCarbonTable(databaseName, identifier.table)
    if (table == null) {
      true
    } else {
      carbonEnv.carbonMetaStore.isSchemaRefreshed(AbsoluteTableIdentifier.from(table.getTablePath,
          identifier.database.getOrElse(sparkSession.sessionState.catalog.getCurrentDatabase),
          identifier.table, table.getTableInfo.getFactTable.getTableId), sparkSession)
    }
  }

  /**
   * Return carbon table instance by looking up table in `sparkSession`
   */
  def getCarbonTable(
      tableIdentifier: TableIdentifier)
    (sparkSession: SparkSession): CarbonTable = {
    getCarbonTable(tableIdentifier.database, tableIdentifier.table)(sparkSession)
  }

  /**
   * Return database name or get default name from sparkSession
   */
  def getDatabaseName(
      databaseNameOp: Option[String]
  )(sparkSession: SparkSession): String = {
    databaseNameOp.getOrElse(sparkSession.sessionState.catalog.getCurrentDatabase)
  }

  /**
   * Returns true with the database folder exists in file system. False in all other scenarios.
   */
  def databaseLocationExists(dbName: String,
      sparkSession: SparkSession, ifExists: Boolean): Boolean = {
    try {
      FileFactory.getCarbonFile(getDatabaseLocation(dbName, sparkSession)).exists()
    } catch {
      case e: NoSuchDatabaseException =>
        if (ifExists) {
          false
        } else {
          throw e
        }
    }
  }

  /**
   * The method returns the database location
   * if carbon.storeLocation does  point to spark.sql.warehouse.dir then returns
   * the database locationUri as database location else follows the old behaviour
   * making database location from carbon fixed store and database name.
   * @return database location
   */
  def getDatabaseLocation(dbName: String, sparkSession: SparkSession): String = {
    var databaseLocation =
      sparkSession.sessionState.catalog.asInstanceOf[SessionCatalog].getDatabaseMetadata(dbName)
        .locationUri.toString
    // for default database and db ends with .db
    // check whether the carbon store and hive store is same or different.
    if (dbName.equals("default") || databaseLocation.endsWith(".db")) {
      val carbonStorePath = FileFactory.getUpdatedFilePath(CarbonProperties.getStorePath())
      val hiveStorePath = FileFactory.getUpdatedFilePath(
        sparkSession.conf.get("spark.sql.warehouse.dir", carbonStorePath))
      // if carbon.store does not point to spark.sql.warehouse.dir then follow the old table path
      // format
      if (carbonStorePath != null && !hiveStorePath.equals(carbonStorePath)) {
        databaseLocation = CarbonProperties.getStorePath +
                           CarbonCommonConstants.FILE_SEPARATOR +
                           dbName
      }
    }

    FileFactory.getUpdatedFilePath(databaseLocation)
  }

  /**
   * Return table path from carbon table. If table does not exist, construct it using
   * database location and table name
   */
  def getTablePath(
      databaseNameOp: Option[String],
      tableName: String
  )(sparkSession: SparkSession): String = {
    try {
      getCarbonTable(databaseNameOp, tableName)(sparkSession).getTablePath
    } catch {
      case _: NoSuchTableException =>
        newTablePath(databaseNameOp, tableName)(sparkSession)
    }
  }

  private def newTablePath(
      databaseNameOp: Option[String],
      tableName: String
  )(sparkSession: SparkSession): String = {
    val dbName = getDatabaseName(databaseNameOp)(sparkSession)
    val dbLocation = getDatabaseLocation(dbName, sparkSession)
    dbLocation + CarbonCommonConstants.FILE_SEPARATOR + tableName
  }

  def createTablePath(
      databaseNameOp: Option[String],
      tableName: String,
      tableId: String,
      location: Option[String],
      isExternal: Boolean,
      isTransactionalTable: Boolean
  )(sparkSession: SparkSession): String = {
    val path = location.getOrElse(
      CarbonEnv.newTablePath(databaseNameOp, tableName)(sparkSession))
    if (!isExternal && isTransactionalTable && location.isEmpty &&
        FileFactory.getCarbonFile(path).exists()) {
      path + "_" + tableId
    } else {
      path
    }
  }

  def getIdentifier(
      databaseNameOp: Option[String],
      tableName: String
  )(sparkSession: SparkSession): AbsoluteTableIdentifier = {
    AbsoluteTableIdentifier.from(
      getTablePath(databaseNameOp, tableName)(sparkSession),
      getDatabaseName(databaseNameOp)(sparkSession),
      tableName)
  }

  def getThreadParam(key: String, defaultValue: String) : String = {
    val carbonSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (null != carbonSessionInfo) {
      carbonSessionInfo.getThreadParams.getProperty(key, defaultValue)
    } else {
      defaultValue
    }
  }

  def getSessionParam(key: String, defaultValue: String) : String = {
    val carbonSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (null != carbonSessionInfo) {
      carbonSessionInfo.getThreadParams.getProperty(key, defaultValue)
    } else {
      defaultValue
    }
  }

}
