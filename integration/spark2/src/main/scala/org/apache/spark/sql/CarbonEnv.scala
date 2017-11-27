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

import java.util.Map
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.{CarbonMetaStore, CarbonMetaStoreFactory, CarbonRelation, CarbonSessionCatalog, CarbonSQLConf}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util._
import org.apache.carbondata.core.util.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.events.{CarbonEnvInitPreEvent, OperationContext, OperationListenerBus}
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
          // trigger event for CarbonEnv init
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

  val carbonEnvMap: Map[SparkSession, CarbonEnv] =
    new ConcurrentHashMap[SparkSession, CarbonEnv]

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
   * Return carbon table instance by looking up table in `sparkSession`
   */
  def getCarbonTable(
      databaseNameOp: Option[String],
      tableName: String)
    (sparkSession: SparkSession): CarbonTable = {
    CarbonEnv
      .getInstance(sparkSession)
      .carbonMetastore
      .lookupRelation(databaseNameOp, tableName)(sparkSession)
      .asInstanceOf[CarbonRelation]
      .carbonTable
  }

  /**
   * Return carbon table instance by looking up table in `sparkSession`
   */
  def getCarbonTable(
      tableIdentifier: TableIdentifier)
    (sparkSession: SparkSession): CarbonTable = {
    CarbonEnv
      .getInstance(sparkSession)
      .carbonMetastore
      .lookupRelation(tableIdentifier)(sparkSession)
      .asInstanceOf[CarbonRelation]
      .carbonTable
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
   * Return table path for specified table
   */
  def getTablePath(
      databaseNameOp: Option[String],
      tableName: String
  )(sparkSession: SparkSession): String = {
    val dbLocation = GetDB.getDatabaseLocation(
      databaseNameOp.getOrElse(sparkSession.sessionState.catalog.getCurrentDatabase),
      sparkSession,
      CarbonProperties.getStorePath)
    dbLocation + CarbonCommonConstants.FILE_SEPARATOR + tableName
  }

  /**
   * Return metadata path for specified table
   */
  def getMetadataPath(
      databaseNameOp: Option[String],
      tableName: String
  )(sparkSession: SparkSession): String = {
    val absoluteTableIdentifier = AbsoluteTableIdentifier.from(
      getTablePath(databaseNameOp, tableName)(sparkSession),
      getDatabaseName(databaseNameOp)(sparkSession),
      tableName)
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(absoluteTableIdentifier)
    val schemaFilePath = carbonTablePath.getSchemaFilePath
    CarbonTablePath.getFolderContainingFile(schemaFilePath)
  }
}
