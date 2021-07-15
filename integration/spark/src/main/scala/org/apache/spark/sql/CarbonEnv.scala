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

import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.events.{MergeBloomIndexEventListener, MergeIndexEventListener}
import org.apache.spark.sql.execution.command.CreateFunctionCommand
import org.apache.spark.sql.execution.command.mutation.merge.udf.BlockPathsUDF
import org.apache.spark.sql.hive._
import org.apache.spark.sql.listeners._
import org.apache.spark.sql.profiler.Profiler
import org.apache.spark.sql.secondaryindex.events._
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util._
import org.apache.carbondata.events._
import org.apache.carbondata.geo.GeoUdfRegister
import org.apache.carbondata.index.{TextMatchMaxDocUDF, TextMatchUDF}
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadTablePostExecutionEvent, LoadTablePostStatusUpdateEvent, LoadTablePreExecutionEvent, LoadTablePreStatusUpdateEvent}
import org.apache.carbondata.spark.rdd.SparkReadSupport
import org.apache.carbondata.spark.readsupport.SparkRowReadSupportImpl
import org.apache.carbondata.view.{MVFunctions, TimeSeriesFunction}

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

  def init(sparkSession: SparkSession): Unit = this.synchronized {
    // after locking, check initialized at first
    if (initialized) {
      return
    }
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
    sparkSession.udf.register("getBlockPaths", new BlockPathsUDF)
    // add NI as a temp function, for queries to not hit SI table, it will be added as HiveSimpleUDF
    CreateFunctionCommand(
      databaseName = None,
      functionName = "NI",
      className = "org.apache.spark.sql.hive.NonIndexUDFExpression",
      resources = Seq(),
      isTemp = true,
      ignoreIfExists = false,
      replace = true).run(sparkSession)

    // register for lucene indexSchema
    // TODO: move it to proper place, it should be registered by indexSchema implementation
    sparkSession.udf.register("text_match", new TextMatchUDF)
    sparkSession.udf.register("text_match_with_limit", new TextMatchMaxDocUDF)
    sparkSession.udf.register("insegment", new (String => Boolean) with Serializable {
      override def apply(v1: String): Boolean = true
    })

    // register udf for spatial index filters of querying
    GeoUdfRegister.registerQueryFilterUdf(sparkSession)

    // register udf for spatial index utils
    GeoUdfRegister.registerUtilUdf(sparkSession)

    // register udf for materialized view
    sparkSession.udf.register(MVFunctions.DUMMY_FUNCTION, () => "")
    // added for handling timeseries function like hour, minute, day, month, year
    sparkSession.udf.register(MVFunctions.TIME_SERIES_FUNCTION, new TimeSeriesFunction)

    // update carbon session parameters , preserve thread parameters
    val currentThreadSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
    carbonSessionInfo = new CarbonSessionInfo()
    // We should not corrupt the information in carbonSessionInfo object which is at the
    // session level. Instead create a new object and in that set the user specified values in
    // thread/session params
    val threadLevelCarbonSessionInfo = new CarbonSessionInfo()
    if (currentThreadSessionInfo != null) {
      threadLevelCarbonSessionInfo.setThreadParams(currentThreadSessionInfo.getThreadParams)
      threadLevelCarbonSessionInfo.setSessionParams(currentThreadSessionInfo.getSessionParams)
    }
    ThreadLocalSessionInfo.setCarbonSessionInfo(threadLevelCarbonSessionInfo)
    ThreadLocalSessionInfo.setConfigurationToCurrentThread(
      sparkSession.sessionState.newHadoopConf())
    // add default dynamic session params
    CarbonSQLConf.addDefaultSessionParams(sparkSession);
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
    Profiler.initialize(sparkSession.sparkContext)
    CarbonToSparkAdapter.addSparkSessionListener(sparkSession)
    if(sparkSession.sparkContext.version.startsWith("3.1")) {
      CarbonProperties.getInstance().addProperty(CarbonCommonConstants
        .CARBON_SPARK_VERSION_SPARK3, "true")
    }
    initialized = true
    LOGGER.info("Initialize CarbonEnv completed...")
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
      // if carbonEnv is null, need to create create CarbonEnv
      // use global level lock, it will be fine for only creating CarbonEnv object
      CarbonEnv.carbonEnvMap.synchronized {
        // need to double check whether CarbonEnv exists or not
        carbonEnv = carbonEnvMap.get(sparkSession)
        if (carbonEnv == null) {
          carbonEnv = new CarbonEnv
          carbonEnvMap.put(sparkSession, carbonEnv)
        }
      }
    }
    if (!carbonEnv.initialized) {
      carbonEnv.init(sparkSession)
    }
    carbonEnv
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
      .addListener(classOf[LoadTablePreStatusUpdateEvent], new MergeIndexEventListener)
      .addListener(classOf[AlterTableMergeIndexEvent], new MergeIndexEventListener)
      .addListener(classOf[BuildIndexPostExecutionEvent], new MergeBloomIndexEventListener)
      .addListener(classOf[DropTableCacheEvent], DropCacheMVEventListener)
      .addListener(classOf[DropTableCacheEvent], DropCacheBloomEventListener)
      .addListener(classOf[ShowTableCacheEvent], ShowCacheEventListener)
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
      // For materialized view
      .addListener(classOf[AlterTableCompactionPreStatusUpdateEvent], MVCompactionPostEventListener)
      .addListener(classOf[LoadTablePreExecutionEvent], MVLoadPreEventListener)
      .addListener(classOf[LoadTablePostExecutionEvent], MVLoadPostEventListener)
      .addListener(classOf[UpdateTablePostEvent], MVLoadPostEventListener)
      .addListener(classOf[DeleteFromTablePostEvent], MVLoadPostEventListener)
      .addListener(classOf[DeleteSegmentByIdPreEvent], MVDeleteSegmentPreEventListener)
      .addListener(classOf[DeleteSegmentByDatePreEvent], MVDeleteSegmentPreEventListener)
      .addListener(classOf[AlterTableAddColumnPreEvent], MVAddColumnsPreEventListener)
      .addListener(classOf[AlterTableDropColumnPreEvent], MVDropColumnPreEventListener)
      .addListener(classOf[AlterTableColRenameAndDataTypeChangePreEvent],
        MVAlterColumnPreEventListener)
      .addListener(classOf[AlterTableDropPartitionMetaEvent], MVDropPartitionMetaEventListener)
      .addListener(classOf[AlterTableDropPartitionPreStatusEvent], MVDropPartitionPreEventListener)
      .addListener(classOf[DropTablePreEvent], MVDropTablePreEventListener)
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
   * Return any kinds of table including non-carbon table
   */
  def getAnyTable(
      databaseNameOp: Option[String],
      tableName: String)
    (sparkSession: SparkSession): CarbonTable = {
    val catalog = getInstance(sparkSession).carbonMetaStore
    catalog
      .lookupAnyRelation(databaseNameOp, tableName)(sparkSession)
      .asInstanceOf[CarbonRelation]
      .carbonTable
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
    if ((!EnvHelper.isLegacy(sparkSession)) &&
        (dbName.equals("default") || databaseLocation.endsWith(".db"))) {
      val carbonStorePath = CarbonProperties.getStorePath()
      val hiveStorePath = sparkSession.conf.get("spark.sql.warehouse.dir")
      // if carbon.store does not point to spark.sql.warehouse.dir then follow the old table path
      // format
      if (carbonStorePath != null && !hiveStorePath.equals(carbonStorePath)) {
        databaseLocation = CarbonProperties.getStorePath +
                           CarbonCommonConstants.FILE_SEPARATOR +
                           dbName
      }
    }
    databaseLocation
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
    var tmpPath = location.getOrElse(
      CarbonEnv.newTablePath(databaseNameOp, tableName)(sparkSession))
    if (!isExternal && isTransactionalTable && location.isEmpty &&
        (FileFactory.getCarbonFile(tmpPath).exists() || EnvHelper.isLegacy(sparkSession))) {
      tmpPath = tmpPath + "_" + tableId
    }
    val path = new Path(tmpPath)
    val fs = path.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    fs.makeQualified(path).toString
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
