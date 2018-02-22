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
import org.apache.spark.sql.execution.command.preaaggregate._
import org.apache.spark.sql.execution.command.timeseries.TimeSeriesFunction
import org.apache.spark.sql.hive.{HiveSessionCatalog, _}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util._
import org.apache.carbondata.events._
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadMetadataEvent, LoadTablePreExecutionEvent, LoadTablePreStatusUpdateEvent}
import org.apache.carbondata.spark.rdd.SparkReadSupport
import org.apache.carbondata.spark.readsupport.SparkRowReadSupportImpl


/**
 * Carbon Environment for unified context
 */
class CarbonEnv {

  var carbonMetastore: CarbonMetaStore = _

  var sessionParams: SessionParams = _

  var carbonSessionInfo: CarbonSessionInfo = _

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  // set readsupport class global so that the executor can get it.
  SparkReadSupport.readSupportClass = classOf[SparkRowReadSupportImpl]

  var initialized = false

  def init(sparkSession: SparkSession): Unit = {
    sparkSession.udf.register("getTupleId", () => "")
    // added for handling preaggregate table creation. when user will fire create ddl for
    // create table we are adding a udf so no need to apply PreAggregate rules.
    sparkSession.udf.register("preAgg", () => "")
    // added to apply proper rules for loading data into pre-agg table. If this UDF is present
    // only then the CarbonPreAggregateDataLoadingRules would be applied to split the average
    // column to sum and count.
    sparkSession.udf.register("preAggLoad", () => "")

    // added for handling timeseries function like hour, minute, day , month , year
    sparkSession.udf.register("timeseries", new TimeSeriesFunction)
    synchronized {
      if (!initialized) {
        // update carbon session parameters , preserve thread parameters
        val currentThreadSesssionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
        carbonSessionInfo = new CarbonSessionInfo()
        sessionParams = carbonSessionInfo.getSessionParams
        if (currentThreadSesssionInfo != null) {
          carbonSessionInfo.setThreadParams(currentThreadSesssionInfo.getThreadParams)
        }
        carbonSessionInfo.setSessionParams(sessionParams)
        ThreadLocalSessionInfo.setCarbonSessionInfo(carbonSessionInfo)
        val config = new CarbonSQLConf(sparkSession)
        if (sparkSession.conf.getOption(CarbonCommonConstants.ENABLE_UNSAFE_SORT).isEmpty) {
          config.addDefaultCarbonParams()
        }
        // add session params after adding DefaultCarbonParams
        config.addDefaultCarbonSessionParams()
        carbonMetastore = {
          val properties = CarbonProperties.getInstance()
          var storePath = properties.getProperty(CarbonCommonConstants.STORE_LOCATION)
          if (storePath == null) {
            storePath = sparkSession.conf.get("spark.sql.warehouse.dir")
            properties.addProperty(CarbonCommonConstants.STORE_LOCATION, storePath)
          }
          LOGGER.info(s"carbon env initial: $storePath")
          // trigger event for CarbonEnv create
          val operationContext = new OperationContext
          val carbonEnvInitPreEvent: CarbonEnvInitPreEvent =
            CarbonEnvInitPreEvent(sparkSession, storePath)
          OperationListenerBus.getInstance.fireEvent(carbonEnvInitPreEvent, operationContext)

          CarbonMetaStoreFactory.createCarbonMetaStore(sparkSession.conf)
        }
        CarbonProperties.getInstance.addProperty(CarbonCommonConstants.IS_DRIVER_INSTANCE, "true")
        initialized = true
      }
    }
  }
}

object CarbonEnv {

  val carbonEnvMap = new ConcurrentHashMap[SparkSession, CarbonEnv]

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def getInstance(sparkSession: SparkSession): CarbonEnv = {
    if (sparkSession.isInstanceOf[CarbonSession]) {
      sparkSession.sessionState.catalog.asInstanceOf[CarbonSessionCatalog].carbonEnv
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
      .addListener(classOf[AlterTableRenamePreEvent], PreAggregateRenameTablePreListener)
      .addListener(classOf[AlterTableDataTypeChangePreEvent], PreAggregateDataTypeChangePreListener)
      .addListener(classOf[AlterTableAddColumnPreEvent], PreAggregateAddColumnsPreListener)
      .addListener(classOf[LoadTablePreExecutionEvent], LoadPreAggregateTablePreListener)
      .addListener(classOf[AlterTableCompactionPreStatusUpdateEvent],
        AlterPreAggregateTableCompactionPostListener)
      .addListener(classOf[LoadMetadataEvent], LoadProcessMetaListener)
      .addListener(classOf[LoadMetadataEvent], CompactionProcessMetaListener)
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
    val catalog = getInstance(sparkSession).carbonMetastore
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
    if (carbonEnv.carbonMetastore.checkSchemasModifiedTimeAndReloadTable(identifier)) {
      sparkSession.sessionState.catalog.refreshTable(identifier)
      DataMapStoreManager.getInstance().
        clearDataMaps(AbsoluteTableIdentifier.from(CarbonProperties.getStorePath,
          identifier.database.getOrElse("default"), identifier.table))
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
      sparkSession.sessionState.catalog.asInstanceOf[HiveSessionCatalog].getDatabaseMetadata(dbName)
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

}
