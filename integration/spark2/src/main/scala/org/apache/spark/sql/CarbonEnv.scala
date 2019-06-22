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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.events.{MergeBloomIndexEventListener, MergeIndexEventListener}
import org.apache.spark.sql.execution.command.cache._
import org.apache.spark.sql.execution.command.mv._
import org.apache.spark.sql.execution.command.preaaggregate._
import org.apache.spark.sql.execution.command.timeseries.TimeSeriesFunction
import org.apache.spark.sql.hive._

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util._
import org.apache.carbondata.datamap.{TextMatchMaxDocUDF, TextMatchUDF}
import org.apache.carbondata.events._
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadMetadataEvent, LoadTablePostExecutionEvent, LoadTablePostStatusUpdateEvent, LoadTablePreExecutionEvent, LoadTablePreStatusUpdateEvent}
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
      storePath = sparkSession.conf.get("spark.sql.warehouse.dir")
      properties.addProperty(CarbonCommonConstants.STORE_LOCATION, storePath)
    }
    LOGGER.info(s"Initializing CarbonEnv, store location: $storePath")

    sparkSession.udf.register("getTupleId", () => "")
    // added for handling preaggregate table creation. when user will fire create ddl for
    // create table we are adding a udf so no need to apply PreAggregate rules.
    sparkSession.udf.register("preAgg", () => "")
    // added to apply proper rules for loading data into pre-agg table. If this UDF is present
    // only then the CarbonPreAggregateDataLoadingRules would be applied to split the average
    // column to sum and count.
    sparkSession.udf.register("preAggLoad", () => "")

    // register for lucene datamap
    // TODO: move it to proper place, it should be registered by datamap implementation
    sparkSession.udf.register("text_match", new TextMatchUDF)
    sparkSession.udf.register("text_match_with_limit", new TextMatchMaxDocUDF)

    // added for handling timeseries function like hour, minute, day , month , year
    sparkSession.udf.register("timeseries", new TimeSeriesFunction)
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
      }
    }
    LOGGER.info("Initialize CarbonEnv completed...")
  }
}

object CarbonEnv {

  val carbonEnvMap = new ConcurrentHashMap[SparkSession, CarbonEnv]

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def getInstance(sparkSession: SparkSession): CarbonEnv = {
    if (sparkSession.isInstanceOf[CarbonSession]) {
      sparkSession.sessionState.catalog.asInstanceOf[CarbonSessionCatalog].getCarbonEnv
    } else {
      var carbonEnv: CarbonEnv = carbonEnvMap.get(sparkSession)
      if (carbonEnv == null) {
        carbonEnv = new CarbonEnv
        carbonEnv.init(sparkSession)
        carbonEnvMap.put(sparkSession, carbonEnv)
      }
      carbonEnv
    }
  }

  /**
   * Method
   * 1. To initialize Listeners to their respective events in the OperationListenerBus
   * 2. To register common listeners
   * 3. Only initialize once for all the listeners in case of concurrent scenarios we have given
   * val, as val initializes once
   */
  val init = {
    initListeners
  }

  /**
   * Method to initialize Listeners to their respective events in the OperationListenerBus.
   */
  def initListeners(): Unit = {
    OperationListenerBus.getInstance()
      .addListener(classOf[LoadTablePreStatusUpdateEvent], LoadPostAggregateListener)
      .addListener(classOf[DeleteSegmentByIdPreEvent], PreAggregateDeleteSegmentByIdPreListener)
      .addListener(classOf[DeleteSegmentByDatePreEvent], PreAggregateDeleteSegmentByDatePreListener)
      .addListener(classOf[UpdateTablePreEvent], UpdatePreAggregatePreListener)
      .addListener(classOf[DeleteFromTablePreEvent], DeletePreAggregatePreListener)
      .addListener(classOf[DeleteFromTablePreEvent], DeletePreAggregatePreListener)
      .addListener(classOf[AlterTableDropColumnPreEvent], PreAggregateDropColumnPreListener)
      .addListener(classOf[AlterTableRenamePreEvent], RenameTablePreListener)
      .addListener(classOf[AlterTableColRenameAndDataTypeChangePreEvent],
        PreAggregateDataTypeChangePreListener)
      .addListener(classOf[AlterTableAddColumnPreEvent], PreAggregateAddColumnsPreListener)
      .addListener(classOf[LoadTablePreExecutionEvent], LoadPreAggregateTablePreListener)
      .addListener(classOf[AlterTableCompactionPreStatusUpdateEvent],
        AlterPreAggregateTableCompactionPostListener)
      .addListener(classOf[AlterTableCompactionPreStatusUpdateEvent],
        AlterDataMaptableCompactionPostListener)
      .addListener(classOf[LoadMetadataEvent], LoadProcessMetaListener)
      .addListener(classOf[LoadMetadataEvent], CompactionProcessMetaListener)
      .addListener(classOf[LoadTablePostStatusUpdateEvent], CommitPreAggregateListener)
      .addListener(classOf[AlterTableCompactionPostStatusUpdateEvent], CommitPreAggregateListener)
      .addListener(classOf[AlterTableDropPartitionPreStatusEvent],
        AlterTableDropPartitionPreStatusListener)
      .addListener(classOf[AlterTableDropPartitionPostStatusEvent],
        AlterTableDropPartitionPostStatusListener)
      .addListener(classOf[AlterTableDropPartitionMetaEvent], AlterTableDropPartitionMetaListener)
      .addListener(classOf[LoadTablePreStatusUpdateEvent], new MergeIndexEventListener)
      .addListener(classOf[LoadTablePostExecutionEvent], LoadPostDataMapListener)
      .addListener(classOf[UpdateTablePostEvent], LoadPostDataMapListener )
      .addListener(classOf[DeleteFromTablePostEvent], LoadPostDataMapListener )
      .addListener(classOf[AlterTableMergeIndexEvent], new MergeIndexEventListener)
      .addListener(classOf[BuildDataMapPostExecutionEvent], new MergeBloomIndexEventListener)
      .addListener(classOf[DropTableCacheEvent], DropCacheDataMapEventListener)
      .addListener(classOf[DropTableCacheEvent], DropCacheBloomEventListener)
      .addListener(classOf[ShowTableCacheEvent], ShowCachePreAggEventListener)
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
  }

  /**
   * Return carbon table instance from cache or by looking up table in `sparkSession`
   */
  def getCarbonTable(
      databaseNameOp: Option[String],
      tableName: String)
    (sparkSession: SparkSession): CarbonTable = {
    refreshRelationFromCache(TableIdentifier(tableName, databaseNameOp))(sparkSession)
    val databaseName = getDatabaseName(databaseNameOp)(sparkSession)
    val catalog = getInstance(sparkSession).carbonMetaStore
    // refresh cache
    catalog.checkSchemasModifiedTimeAndReloadTable(TableIdentifier(tableName, databaseNameOp))

    // try to get it from catch, otherwise lookup in catalog
    catalog.getTableFromMetadataCache(databaseName, tableName)
      .getOrElse(
        catalog
          .lookupRelation(databaseNameOp, tableName)(sparkSession)
          .asInstanceOf[CarbonRelation]
          .carbonTable)
  }

  def refreshRelationFromCache(identifier: TableIdentifier)(sparkSession: SparkSession): Boolean = {
    var isRefreshed = false
    val carbonEnv = getInstance(sparkSession)
    val table = carbonEnv.carbonMetaStore.getTableFromMetadataCache(
      identifier.database.getOrElse(sparkSession.sessionState.catalog.getCurrentDatabase),
      identifier.table)
    if (carbonEnv.carbonMetaStore
          .checkSchemasModifiedTimeAndReloadTable(identifier)  && table.isDefined) {
      sparkSession.sessionState.catalog.refreshTable(identifier)
      val tablePath = table.get.getTablePath
      DataMapStoreManager.getInstance().
        clearDataMaps(AbsoluteTableIdentifier.from(tablePath,
          identifier.database.getOrElse(sparkSession.sessionState.catalog.getCurrentDatabase),
          identifier.table, table.get.getTableInfo.getFactTable.getTableId))
      isRefreshed = true
    }
    isRefreshed
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
      val properties = CarbonProperties.getInstance()
      val carbonStorePath =
        FileFactory.getUpdatedFilePath(properties.getProperty(CarbonCommonConstants.STORE_LOCATION))
      val hiveStorePath =
        FileFactory.getUpdatedFilePath(sparkSession.conf.get("spark.sql.warehouse.dir"))
      // if carbon.store does not point to spark.sql.warehouse.dir then follow the old table path
      // format
      if (!hiveStorePath.equals(carbonStorePath)) {
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
        val dbName = getDatabaseName(databaseNameOp)(sparkSession)
        val dbLocation = getDatabaseLocation(dbName, sparkSession)
        dbLocation + CarbonCommonConstants.FILE_SEPARATOR + tableName
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
