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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.ipc.{ProtocolInfo, RPC}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.DistributableDataMapFormat
import org.apache.carbondata.core.exception.InvalidConfigurationException
import org.apache.carbondata.core.indexstore.ExtendedBlocklet
import org.apache.carbondata.core.metadata.schema.table.TableInfo
import org.apache.carbondata.core.util.CarbonProperties

@ProtocolInfo(protocolName = "Server", protocolVersion = 1)
trait ServerInterface {
  def getSplits(request: DistributableDataMapFormat): Array[(String, ExtendedBlocklet)]
  def invalidateCache(tableInfo: TableInfo): Unit
  def showCache()
}

object IndexServer extends ServerInterface {
  val prunePolicy: String = CarbonProperties.getInstance().getIndexServerPolicy

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
  private val serverIp: String = CarbonProperties.getInstance().getIndexServerIP

  private val serverPort: Int = CarbonProperties.getInstance().getIndexServerPort

  def getSplits(request: DistributableDataMapFormat): Array[(String, ExtendedBlocklet)] = {
    new DistributedPruneRDD(SparkSQLUtil.getSparkSession, request, null).collect()
  }

  override def invalidateCache(tableInfo: TableInfo): Unit = {}

  override def showCache(): Unit = {}

  def main(args: Array[String]): Unit = {
    if (serverIp.isEmpty) {
      throw new RuntimeException(s"Please set the server IP to use $prunePolicy as the " +
                                 s"pruning policy")
    } else {
      val (storePath, masterURL) = if (args.length == 1) {
        (args.head, "local")
      } else if (args.length == 2) {
        (args.head, args.tail.head)
      } else {
        (null, "local")
      }
      createCarbonSession(storePath, masterURL)
      LOGGER.info("Starting Index Cache Server")
      val conf = new Configuration()
      val server: RPC.Server = new RPC.Builder(conf).setInstance(this)
        .setBindAddress(serverIp)
        .setPort(serverPort)
        .setProtocol(classOf[ServerInterface]).build
      server.start()
      LOGGER.info(s"Index cache server running on ${ server.getPort } port")
    }
  }

  private def createCarbonSession(storePath: String, masterURL: String): SparkSession = {
    import org.apache.spark.sql.CarbonSession._
    val master = System.getProperty("spark.master.url", masterURL)
    if (master == null) {
      throw new InvalidConfigurationException("Spark master URL is not set.")
    }
    val spark = SparkSession
      .builder().config(new SparkConf())
      .master(master)
      .appName("DistributedIndexServer")
      .enableHiveSupport()
      .config("spark.dynamicAllocation.enabled", "false")
      .getOrCreateCarbonSession(storePath)
    SparkSession.setActiveSession(spark)
    SparkSession.setDefaultSession(spark)
    spark
  }

  def getClient: ServerInterface = {
    import org.apache.hadoop.ipc.RPC
    RPC.getProxy(classOf[ServerInterface],
      RPC.getProtocolVersion(classOf[ServerInterface]),
      new InetSocketAddress(serverIp, serverPort), new Configuration())
  }
}
