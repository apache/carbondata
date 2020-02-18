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
import java.util.concurrent.{Executors, ExecutorService}
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.ipc.{ProtocolInfo, RPC, Server}
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.security.{KerberosInfo, UserGroupInformation}
import org.apache.hadoop.security.authorize.{PolicyProvider, Service}
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.index.DistributableIndexFormat
import org.apache.carbondata.core.indexstore.{ExtendedBlockletWrapperContainer, SegmentWrapperContainer}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events.{IndexServerEvent, OperationContext, OperationListenerBus}

@ProtocolInfo(protocolName = "org.apache.carbondata.indexserver.ServerInterface",
  protocolVersion = 1)
@KerberosInfo(serverPrincipal = "spark.carbon.indexserver.principal",
  clientPrincipal = "spark.carbon.indexserver.principal")
trait ServerInterface {
  /**
   * Used to prune and cache the index for the table.
   */
  def getSplits(request: DistributableIndexFormat): ExtendedBlockletWrapperContainer

  /**
   * Get the cache size for the specified tables.
   */
  def showCache(tableIds: String, executorCache: Boolean) : Array[String]

  /**
   * Invalidate the cache for the specified segments only. Used in case of compaction/Update/Delete.
   */
  def invalidateSegmentCache(carbonTable: CarbonTable,
  segmentIds: Array[String], jobGroupId: String = "", isFallBack: Boolean = false): Unit

  def getCount(request: DistributableIndexFormat): LongWritable

  def getPrunedSegments(request: DistributableIndexFormat): SegmentWrapperContainer

}

/**
 * An instance of a distributed Index Server which will be used for:
 * 1. Pruning the index in a distributed way by using the executors.
 * 2. Caching the pruned indexes in executor size to be reused in the next query.
 * 3. Getting the size of the indexes cached in the executors.
 * 4. Clearing the indexes for a table or for the specified invalid segments.
 *
 * Start using ./bin/start-indexserver.sh
 * Stop using ./bin/stop-indexserver.sh
 */
object IndexServer extends ServerInterface {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  private val serverIp: String = CarbonProperties.getInstance().getIndexServerIP

  private lazy val serverPort: Int = CarbonProperties.getInstance().getIndexServerPort

  private val numHandlers: Int = CarbonProperties.getInstance().getNumberOfHandlersForIndexServer

  private lazy val indexServerExecutorService: Option[ExecutorService] = {
    if (CarbonProperties.getInstance().isDistributedPruningEnabled("", "")) {
      Some(Executors.newFixedThreadPool(1))
    } else {
      None
    }
  }

  private val isExecutorLRUConfigured: Boolean =
    CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.CARBON_MAX_EXECUTOR_LRU_CACHE_SIZE) != null
  private val operationContext: OperationContext = new OperationContext

  /**
   * Perform the operation 'f' on behalf of the login user.
   */
  private def doAs[T](f: => T): T = {
    UserGroupInformation.getLoginUser.doAs(new PrivilegedAction[T] {
      override def run(): T = {
        f
      }
    })
  }

  private def submitAsyncTask[T](t: => Unit): Unit = {
    indexServerExecutorService.get.submit(new Runnable {
      override def run(): Unit = {
        t
      }
    })
  }

  def getCount(request: DistributableIndexFormat): LongWritable = {
    doAs {
      val sparkSession = SparkSQLUtil.getSparkSession
      var currentUser: String = null
      if (!request.isFallbackJob) {
        currentUser = Server.getRemoteUser.getShortUserName
      }
      lazy val getCountTask = {
        if (!request.isFallbackJob) {
          sparkSession.sparkContext.setLocalProperty("spark.jobGroup.id", request.getTaskGroupId)
          sparkSession.sparkContext
            .setLocalProperty("spark.job.description", request.getTaskGroupDesc)
        }
        // Fire Generic Event like ACLCheck..etc
        val indexServerEvent = IndexServerEvent(sparkSession, request.getCarbonTable,
          currentUser)
        OperationListenerBus.getInstance().fireEvent(indexServerEvent, operationContext)
        val splits = new DistributedCountRDD(sparkSession, request).collect()
        if (!request.isFallbackJob) {
          DistributedRDDUtils.updateExecutorCacheSize(splits.map(_._1).toSet)
        }
        new LongWritable(splits.map(_._2.toLong).sum)
      }
      if (request.ifAsyncCall) {
        submitAsyncTask(getCountTask)
        new LongWritable(0)
      } else {
        getCountTask
      }
    }
  }

  def getSplits(request: DistributableIndexFormat): ExtendedBlockletWrapperContainer = {
    doAs {
      val sparkSession = SparkSQLUtil.getSparkSession
      if (!request.isFallbackJob) {
        sparkSession.sparkContext.setLocalProperty("spark.jobGroup.id", request.getTaskGroupId)
        sparkSession.sparkContext
          .setLocalProperty("spark.job.description", request.getTaskGroupDesc)
        // Fire Generic Event like ACLCheck..etc
        val indexServerEvent = IndexServerEvent(sparkSession, request.getCarbonTable,
          Server.getRemoteUser.getShortUserName)
        OperationListenerBus.getInstance().fireEvent(indexServerEvent, operationContext)
      }
      if (!request.getInvalidSegments.isEmpty) {
        DistributedRDDUtils
          .invalidateSegmentMapping(request.getCarbonTable.getTableUniqueName,
            request.getInvalidSegments.asScala)
      }
      val splits = new DistributedPruneRDD(sparkSession, request).collect()
      if (!request.isFallbackJob) {
        DistributedRDDUtils.updateExecutorCacheSize(splits.map(_._1).toSet)
      }
      if (request.isJobToClearIndex) {
        DistributedRDDUtils.invalidateTableMapping(request.getCarbonTable.getTableUniqueName)
      }
      new ExtendedBlockletWrapperContainer(splits.map(_._2), request.isFallbackJob)
    }
  }

  override def invalidateSegmentCache(carbonTable: CarbonTable,
      segmentIds: Array[String], jobGroupId: String = "", isFallBack: Boolean = false): Unit = {
    doAs {
      val sparkSession = SparkSQLUtil.getSparkSession
      val databaseName = carbonTable.getDatabaseName
      val tableName = carbonTable.getTableName
      val jobgroup: String = " Invalided Segment Cache for " + databaseName + "." + tableName
      sparkSession.sparkContext.setLocalProperty("spark.job.description", jobgroup)
      sparkSession.sparkContext.setLocalProperty("spark.jobGroup.id", jobGroupId)
      if (!isFallBack) {
        val indexServerEvent = IndexServerEvent(sparkSession,
          carbonTable,
          Server.getRemoteUser.getShortUserName)
        OperationListenerBus.getInstance().fireEvent(indexServerEvent, operationContext)
      }
      new InvalidateSegmentCacheRDD(sparkSession, carbonTable, segmentIds.toList)
        .collect()
      if (segmentIds.nonEmpty) {
        DistributedRDDUtils
          .invalidateSegmentMapping(s"${databaseName}_$tableName",
            segmentIds)
      }
    }
  }

  override def showCache(tableId: String = "", executorCache: Boolean): Array[String] = {
    doAs {
      val jobgroup: String = "Show Cache " + (tableId match {
        case "" =>
          if (executorCache) {
            "for all the Executors."
          } else {
            "for all tables."
          }
        case table => s"for $table"
      })
      val sparkSession = SparkSQLUtil.getSparkSession
      sparkSession.sparkContext.setLocalProperty("spark.jobGroup.id", UUID.randomUUID().toString)
      sparkSession.sparkContext.setLocalProperty("spark.job.description", jobgroup)
      new DistributedShowCacheRDD(sparkSession, tableId, executorCache).collect()
    }
  }

  override def getPrunedSegments(request: DistributableIndexFormat): SegmentWrapperContainer =
    doAs {
      val sparkSession = SparkSQLUtil.getSparkSession
      sparkSession.sparkContext.setLocalProperty("spark.jobGroup.id", request.getTaskGroupId)
      sparkSession.sparkContext.setLocalProperty("spark.job.description", request.getTaskGroupDesc)
      val splits = new SegmentPruneRDD(sparkSession, request).collect()
      DistributedRDDUtils.updateExecutorCacheSize(splits.map(_._1).toSet)
      val segmentWrappers = splits.map(_._2)
      new SegmentWrapperContainer(segmentWrappers)
    }

  def main(args: Array[String]): Unit = {
    if (serverIp.isEmpty) {
      throw new RuntimeException(s"Please set the server IP to use Index Cache Server")
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
      // Define the Authorization Policy provider
      server.refreshServiceAcl(conf, new IndexServerPolicyProvider)
      val sparkSession = SparkSQLUtil.getSparkSession
      sparkSession.sparkContext.addSparkListener(new SparkListener {
        override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
          LOGGER.info("Spark Application has ended. Stopping the Index Server")
          server.stop()
        }
      })
      CarbonProperties.getInstance().addProperty(CarbonCommonConstants
        .CARBON_ENABLE_INDEX_SERVER, "true")
      CarbonProperties.getInstance().addNonSerializableProperty(CarbonCommonConstants
        .IS_DRIVER_INSTANCE, "true")
      LOGGER.info(s"Index cache server running on ${ server.getPort } port")
    }
  }

  private def createCarbonSession(): SparkSession = {
    val spark = SparkSession
      .builder().config(new SparkConf())
      .appName("DistributedIndexServer")
      .enableHiveSupport()
      .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
      .getOrCreate()
    CarbonEnv.getInstance(spark)

    SparkSession.setActiveSession(spark)
    SparkSession.setDefaultSession(spark)
    if (spark.sparkContext.getConf
      .get("spark.dynamicAllocation.enabled", "false").equalsIgnoreCase("true")) {
      throw new RuntimeException("Index server is not supported with dynamic allocation enabled")
    }
    spark
  }

  /**
   * @return Return a new Client to communicate with the Index Server.
   */
  def getClient: ServerInterface = {
    val sparkSession = SparkSQLUtil.getSparkSession
    val configuration = SparkSQLUtil.sessionState(sparkSession).newHadoopConf()
    import org.apache.hadoop.ipc.RPC
    RPC.getProtocolProxy(classOf[ServerInterface],
      RPC.getProtocolVersion(classOf[ServerInterface]),
      new InetSocketAddress(serverIp, serverPort),
      UserGroupInformation.getLoginUser, configuration,
      NetUtils.getDefaultSocketFactory(configuration)).getProxy
  }

  /**
   * This class to define the acl for indexserver ,similar to HDFSPolicyProvider.
   * key in Service can be configured in hadoop-policy.xml or in  Configuration().This ACL
   * will be used for Authorization in
   * org.apache.hadoop.security.authorize.ServiceAuthorizationManager#authorize
   */
  class IndexServerPolicyProvider extends PolicyProvider {
    override def getServices: Array[Service] = {
      Array(new Service("security.indexserver.protocol.acl", classOf[ServerInterface]))
    }
  }
}
