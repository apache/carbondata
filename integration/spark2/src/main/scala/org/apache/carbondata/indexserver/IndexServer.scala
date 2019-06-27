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
package org.apache.carbondata.indexserver

import java.net.InetSocketAddress
import java.security.PrivilegedAction
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.ipc.{ProtocolInfo, RPC}
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.security.{KerberosInfo, SecurityUtil, UserGroupInformation}
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.{CarbonSession, SparkSession}
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DistributableDataMapFormat
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.ExtendedBlockletWrapperContainer
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties

@ProtocolInfo(protocolName = "Server", protocolVersion = 1)
@KerberosInfo(serverPrincipal = "spark.carbon.indexserver.principal",
  clientPrincipal = "spark.carbon.indexserver.principal")
trait ServerInterface {
  /**
   * Used to prune and cache the datamaps for the table.
   */
  def getSplits(request: DistributableDataMapFormat): ExtendedBlockletWrapperContainer

  /**
   * Get the cache size for the specified tables.
   */
  def showCache(tableNames: String) : Array[String]

  /**
   * Invalidate the cache for the specified segments only. Used in case of compaction/Update/Delete.
   */
  def invalidateSegmentCache(carbonTable: CarbonTable,
      segmentIds: Array[String], jobGroupId: String = ""): Unit
}

/**
 * An instance of a distributed Index Server which will be used for:
 * 1. Pruning the datamaps in a distributed way by using the executors.
 * 2. Caching the pruned datamaps in executor size to be reused in the next query.
 * 3. Getting the size of the datamaps cached in the executors.
 * 4. Clearing the datamaps for a table or for the specified invalid segments.
 *
 * Start using ./bin/start-indexserver.sh
 * Stop using ./bin/stop-indexserver.sh
 */
object IndexServer extends ServerInterface {

  val isDistributedPruning: Boolean =
    CarbonProperties.getInstance().isDistributedPruningEnabled("", "")

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  private val serverIp: String = CarbonProperties.getInstance().getIndexServerIP

  private lazy val serverPort: Int = CarbonProperties.getInstance().getIndexServerPort

  private val numHandlers: Int = CarbonProperties.getInstance().getNumberOfHandlersForIndexServer

  private val isExecutorLRUConfigured: Boolean =
    CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.CARBON_MAX_EXECUTOR_LRU_CACHE_SIZE) != null

  /**
   * Getting sparkSession from ActiveSession because in case of embedded mode the session would
   * have already been created whereas in case of distributed mode the session would be
   * created by the main method after some validations.
   */
  private lazy val sparkSession: SparkSession = SparkSQLUtil.getSparkSession

  private def doAs[T](f: => T): T = {
    UserGroupInformation.getLoginUser.doAs(new PrivilegedAction[T] {
      override def run(): T = {
        f
      }
    })
  }

  def getSplits(request: DistributableDataMapFormat): ExtendedBlockletWrapperContainer = {
    doAs {
      if (!request.isFallbackJob) {
        sparkSession.sparkContext.setLocalProperty("spark.jobGroup.id", request.getTaskGroupId)
        sparkSession.sparkContext
          .setLocalProperty("spark.job.description", request.getTaskGroupDesc)
      }
      val splits = new DistributedPruneRDD(sparkSession, request).collect()
      if (!request.isFallbackJob) {
        DistributedRDDUtils.updateExecutorCacheSize(splits.map(_._1).toSet)
      }
      if (request.isJobToClearDataMaps) {
        DistributedRDDUtils.invalidateTableMapping(request.getCarbonTable.getTableUniqueName)
      }
      if (!request.getInvalidSegments.isEmpty) {
        DistributedRDDUtils
          .invalidateSegmentMapping(request.getCarbonTable.getTableUniqueName,
            request.getInvalidSegments.asScala)
      }
      new ExtendedBlockletWrapperContainer(splits.map(_._2), request.isFallbackJob)
    }
  }

  override def invalidateSegmentCache(carbonTable: CarbonTable,
      segmentIds: Array[String], jobGroupId: String = ""): Unit = doAs {
    val databaseName = carbonTable.getDatabaseName
    val tableName = carbonTable.getTableName
    val jobgroup: String = " Invalided Segment Cache for " + databaseName + "." + tableName
    sparkSession.sparkContext.setLocalProperty("spark.job.description", jobgroup)
    sparkSession.sparkContext.setLocalProperty("spark.jobGroup.id", jobGroupId)
    new InvalidateSegmentCacheRDD(sparkSession, carbonTable, segmentIds.toList)
      .collect()
    if (segmentIds.nonEmpty) {
      DistributedRDDUtils
        .invalidateSegmentMapping(s"${databaseName}_$tableName",
          segmentIds)
    }
  }

  override def showCache(tableName: String = ""): Array[String] = doAs {
    val jobgroup: String = "Show Cache for " + (tableName match {
      case "" => "for all tables"
      case table => s"for $table"
    })
    sparkSession.sparkContext.setLocalProperty("spark.jobGroup.id", UUID.randomUUID().toString)
    sparkSession.sparkContext.setLocalProperty("spark.job.description", jobgroup)
    new DistributedShowCacheRDD(sparkSession, tableName).collect()
  }

  def main(args: Array[String]): Unit = {
    if (serverIp.isEmpty) {
      throw new RuntimeException(s"Please set the server IP to use Index Cache Server")
    } else if (!isDistributedPruning) {
      throw new RuntimeException(
        s"Please set ${ CarbonCommonConstants.CARBON_ENABLE_INDEX_SERVER }" +
        s" as true to use index server")
    } else if (!isExecutorLRUConfigured) {
      throw new RuntimeException(s"Executor LRU cache size is not set. Please set using " +
                                 s"${ CarbonCommonConstants.CARBON_MAX_EXECUTOR_LRU_CACHE_SIZE }")
    } else {
      createCarbonSession()
      LOGGER.info("Starting Index Cache Server")
      val conf = new Configuration()
      val server: RPC.Server = new RPC.Builder(conf).setInstance(this)
        .setBindAddress(serverIp)
        .setPort(serverPort)
        .setNumHandlers(numHandlers)
        .setProtocol(classOf[ServerInterface]).build
      server.start()
      SecurityUtil.login(sparkSession.asInstanceOf[CarbonSession].sessionState.newHadoopConf(),
        "spark.carbon.indexserver.keytab", "spark.carbon.indexserver.principal")
      sparkSession.sparkContext.addSparkListener(new SparkListener {
        override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
          LOGGER.info("Spark Application has ended. Stopping the Index Server")
          server.stop()
        }
      })
      LOGGER.info(s"Index cache server running on ${ server.getPort } port")
    }
  }

  private def createCarbonSession(): SparkSession = {
    import org.apache.spark.sql.CarbonSession._
    val spark = SparkSession
      .builder().config(new SparkConf())
      .appName("DistributedIndexServer")
      .enableHiveSupport()
      .getOrCreateCarbonSession(CarbonProperties.getStorePath)
    SparkSession.setActiveSession(spark)
    SparkSession.setDefaultSession(spark)
    spark
  }

  /**
   * @return Return a new Client to communicate with the Index Server.
   */
  def getClient: ServerInterface = {
    val configuration = SparkSQLUtil.sessionState(sparkSession).newHadoopConf()
    import org.apache.hadoop.ipc.RPC
    val indexServerUser = sparkSession.sparkContext.getConf
      .get("spark.carbon.indexserver.principal", "")
    val indexServerKeyTab = sparkSession.sparkContext.getConf
      .get("spark.carbon.indexserver.keytab", "")
    val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(indexServerUser,
      indexServerKeyTab)
    LOGGER.info("Login successful for user " + indexServerUser)
    RPC.getProxy(classOf[ServerInterface],
      RPC.getProtocolVersion(classOf[ServerInterface]),
      new InetSocketAddress(serverIp, serverPort), ugi,
      FileFactory.getConfiguration, NetUtils.getDefaultSocketFactory(configuration))
  }
}
