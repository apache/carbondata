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
package org.apache.spark.sql.hive

import java.net.{InetAddress, InterfaceAddress, NetworkInterface}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.carbon.datastore.block.Distributable
import org.apache.carbondata.spark.load.CarbonLoaderUtil
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend

import scala.collection.JavaConverters._

object DistributionUtil {
  @transient
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /*
   * This method will return the list of executers in the cluster.
   * For this we take the  memory status of all node with getExecutorMemoryStatus
   * and extract the keys. getExecutorMemoryStatus also returns the driver memory also
   * In client mode driver will run in the localhost
   * There can be executor spawn in same drive node. So we can remove first occurance of
   * localhost for retriving executor list
   */
  def getNodeList(sparkContext: SparkContext): Array[String] = {
    val arr = sparkContext.getExecutorMemoryStatus.map { kv =>
      kv._1.split(":")(0)
    }.toSeq
    val localhostIPs = getLocalhostIPs
    val selectedLocalIPList = localhostIPs.filter(arr.contains(_))

    val nodelist: List[String] = withoutDriverIP(arr.toList)(selectedLocalIPList.contains(_))
    val masterMode = sparkContext.getConf.get("spark.master")
    if (nodelist.nonEmpty) {
      // Specific for Yarn Mode
      if ("yarn-cluster".equals(masterMode) || "yarn-client".equals(masterMode)) {
        val nodeNames = nodelist.map { x =>
          val addr = InetAddress.getByName(x)
          addr.getHostName
        }
        nodeNames.toArray
      } else {
        // For Standalone cluster, node IPs will be returned.
        nodelist.toArray
      }
    } else {
      Seq(InetAddress.getLocalHost.getHostName).toArray
    }
  }

  private def getLocalhostIPs = {
    val iface = NetworkInterface.getNetworkInterfaces
    var addresses: List[InterfaceAddress] = List.empty
    while (iface.hasMoreElements) {
      addresses = iface.nextElement().getInterfaceAddresses.asScala.toList ++ addresses
    }
    val inets = addresses.map(_.getAddress.getHostAddress)
    inets
  }

  /*
   * This method will remove the first occurance of any of the ips  mentioned in the predicate.
   * Eg: l = List(Master,slave1,Master,slave2,slave3) is the list of nodes where first Master is
   * the Driver  node.
   * this method withoutFirst (l)(x=> x == 'Master') will remove the first occurance of Master.
   * The resulting List containt List(slave1,Master,slave2,slave3)
   */
  def withoutDriverIP[A](xs: List[A])(p: A => Boolean): List[A] = {
    xs match {
      case x :: rest => if (p(x)) {
        rest
      } else {
        x :: withoutDriverIP(rest)(p)
      }
      case _ => Nil
    }
  }

  /**
   *
   * Checking if the existing executors is greater than configured executors, if yes
   * returning configured executors.
   *
   * @param blockList
   * @param sparkContext
   * @return
   */
  def ensureExecutorsAndGetNodeList(blockList: Seq[Distributable],
      sparkContext: SparkContext): Seq[String] = {
    val nodeMapping = CarbonLoaderUtil.getRequiredExecutors(blockList.toSeq.asJava)
    ensureExecutorsByNumberAndGetNodeList(nodeMapping.size(), sparkContext)
  }

  def ensureExecutorsByNumberAndGetNodeList(nodesOfData: Int,
      sparkContext: SparkContext): Seq[String] = {
    var confExecutorsTemp: String = null
    if (sparkContext.getConf.contains("spark.executor.instances")) {
      confExecutorsTemp = sparkContext.getConf.get("spark.executor.instances")
    } else if (sparkContext.getConf.contains("spark.dynamicAllocation.enabled")
               && sparkContext.getConf.get("spark.dynamicAllocation.enabled").trim
                 .equalsIgnoreCase("true")) {
      if (sparkContext.getConf.contains("spark.dynamicAllocation.maxExecutors")) {
        confExecutorsTemp = sparkContext.getConf.get("spark.dynamicAllocation.maxExecutors")
      }
    }

    val confExecutors = if (null != confExecutorsTemp) confExecutorsTemp.toInt else 1
    val requiredExecutors = if (nodesOfData < 1 || nodesOfData > confExecutors) {
      confExecutors
    } else {
      nodesOfData
    }

    val startTime = System.currentTimeMillis()
    ensureExecutors(sparkContext, requiredExecutors)
    var nodes = DistributionUtil.getNodeList(sparkContext)
    var maxTimes = 30
    while (nodes.length < requiredExecutors && maxTimes > 0) {
      Thread.sleep(500)
      nodes = DistributionUtil.getNodeList(sparkContext)
      maxTimes = maxTimes - 1
    }
    val timDiff = System.currentTimeMillis() - startTime
    LOGGER.info(s"Total Time taken to ensure the required executors: $timDiff")
    LOGGER.info(s"Time elapsed to allocate the required executors: ${ (30 - maxTimes) * 500 }")
    nodes.distinct
  }

  /**
    *
    * Requesting the extra executors other than the existing ones.
    *
    * @param sc
    * @param numExecutors
    * @return
    */
  def ensureExecutors(sc: SparkContext, numExecutors: Int): Boolean = {
    sc.schedulerBackend match {
      case b: CoarseGrainedSchedulerBackend =>
        val requiredExecutors = numExecutors - b.numExistingExecutors
        LOGGER.info(s"number of executors is =$numExecutors existing executors are =" +
            s"${ b.numExistingExecutors }")
        if (requiredExecutors > 0) {
          b.requestExecutors(requiredExecutors)
        }
        true
      case _ =>
        false
    }
  }
}
