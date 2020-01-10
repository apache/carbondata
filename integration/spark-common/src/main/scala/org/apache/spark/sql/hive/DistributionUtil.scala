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

import scala.collection.JavaConverters._
import scala.util.control.Breaks._

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.block.Distributable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.processing.util.CarbonLoaderUtil

object DistributionUtil {
  @transient
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  /*
   *  minimum required registered resource for starting block distribution
   */
  lazy val minRegisteredResourceRatio: Double = {
    val value: String = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO,
        CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO_DEFAULT)
    java.lang.Double.parseDouble(value)
  }

  /*
   * node registration wait time
   */
  lazy val dynamicAllocationSchTimeOut: Integer = {
    val value: String = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT,
        CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT_DEFAULT)
    // milli second
    java.lang.Integer.parseInt(value) * 1000
  }

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

  def getExecutors(sparkContext: SparkContext): Map[String, Seq[String]] = {
    val bm = sparkContext.env.blockManager
    bm.master.getPeers(bm.blockManagerId)
      .groupBy(blockManagerId => blockManagerId.host).map {
      case (host, blockManagerIds) => (host, blockManagerIds.map(_.executorId))
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
   * @param blockList total number of blocks in the identified segments
   * @param sparkContext
   * @return
   */
  def ensureExecutorsAndGetNodeList(blockList: Seq[Distributable],
      sparkContext: SparkContext): Seq[String] = {
    val nodeMapping = CarbonLoaderUtil.nodeBlockMapping(blockList.asJava)
    ensureExecutorsByNumberAndGetNodeList(nodeMapping, blockList, sparkContext)
  }

  def ensureExecutorsByNumberAndGetNodeList(nodesOfData: Int,
      sparkContext: SparkContext): Seq[String] = {
    val confExecutors: Int = getConfiguredExecutors(sparkContext)
    LOGGER.info(s"Executors configured : $confExecutors")
    val requiredExecutors = if (nodesOfData < 1 || nodesOfData > confExecutors) {
      confExecutors
    } else {
      nodesOfData
    }
    // request for starting the number of required executors
    ensureExecutors(sparkContext, requiredExecutors)
    getDistinctNodesList(sparkContext, requiredExecutors)
  }

  /**
   * This method will ensure that the required/configured number of executors are requested
   * for processing the identified blocks
   *
   * @param nodeMapping
   * @param blockList
   * @param sparkContext
   * @return
   */
  private def ensureExecutorsByNumberAndGetNodeList(
      nodeMapping: java.util.Map[String, java.util.List[Distributable]],
      blockList: Seq[Distributable],
      sparkContext: SparkContext): Seq[String] = {
    val nodesOfData = nodeMapping.size()
    val confExecutors: Int = getConfiguredExecutors(sparkContext)
    LOGGER.info(s"Executors configured : $confExecutors")
    val requiredExecutors = if (nodesOfData < 1) {
      1
    } else if (nodesOfData > confExecutors) {
      confExecutors
    } else if (confExecutors > nodesOfData) {
      var totalExecutorsToBeRequested = nodesOfData
      // If total number of blocks are greater than the nodes identified then ensure
      // that the configured number of max executors can be opened based on the difference of
      // block list size and nodes identified
      if (blockList.size > nodesOfData) {
        // e.g 1. blockList size = 40, confExecutors = 6, then all 6 executors
        // need to be opened
        // 2. blockList size = 4, confExecutors = 6, then
        // total 4 executors need to be opened
        if (blockList.size > confExecutors) {
          totalExecutorsToBeRequested = confExecutors
        } else {
          totalExecutorsToBeRequested = blockList.size
        }
      }
      LOGGER.info(s"Total executors requested: $totalExecutorsToBeRequested")
      totalExecutorsToBeRequested
    } else {
      nodesOfData
    }
    // request for starting the number of required executors
    ensureExecutors(sparkContext, requiredExecutors, blockList.size)
    getDistinctNodesList(sparkContext, requiredExecutors)
  }

  /**
   * This method will return the configured executors
   *
   * @param sparkContext
   * @return
   */
  def getConfiguredExecutors(sparkContext: SparkContext): Int = {
    var confExecutors: Int = 0
    if (sparkContext.getConf.getBoolean("spark.dynamicAllocation.enabled", false)) {
      // default value for spark.dynamicAllocation.maxExecutors is infinity
      confExecutors = sparkContext.getConf.getInt("spark.dynamicAllocation.maxExecutors", 1)
      LOGGER.info(s"spark.dynamicAllocation.maxExecutors property is set to = $confExecutors")
    } else {
      // default value for spark.executor.instances is 2
      confExecutors = sparkContext.getConf.getInt("spark.executor.instances", 1)
      LOGGER.info(s"spark.executor.instances property is set to = $confExecutors")
    }
    confExecutors
  }

  /**
   * This method will return the distinct nodes list
   *
   * @param sparkContext
   * @param requiredExecutors
   * @return
   */
  private def getDistinctNodesList(sparkContext: SparkContext,
      requiredExecutors: Int): Seq[String] = {
    val startTime = System.currentTimeMillis()
    var nodes = DistributionUtil.getNodeList(sparkContext)
    // calculate the number of times loop has to run to check for starting
    // the requested number of executors
    val threadSleepTime =
    CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_THREAD_SLEEP_TIME
    val maxRetryCount = calculateMaxRetry
    var maxTimes = maxRetryCount
    breakable {
      val len = nodes.length
      while (requiredExecutors > len && maxTimes > 0) {
        Thread.sleep(threadSleepTime);
        nodes = DistributionUtil.getNodeList(sparkContext)
        maxTimes = maxTimes - 1;
        val resourceRatio = (nodes.length.toDouble / requiredExecutors)
        if (resourceRatio.compareTo(minRegisteredResourceRatio) >= 0) {
          break
        }
      }
    }
    val timDiff = System.currentTimeMillis() - startTime
    LOGGER.info(s"Total Time taken to ensure the required executors : $timDiff")
    LOGGER.info(s"Time elapsed to allocate the required executors: " +
      s"${(maxRetryCount - maxTimes) * threadSleepTime}")
    nodes.distinct.toSeq
  }

  /**
   * Requesting the extra executors other than the existing ones.
   *
   * @param sc                   sparkContext
   * @param requiredExecutors         required number of executors to be requested
   * @param localityAwareTasks   The number of pending tasks which is locality required
   * @param hostToLocalTaskCount A map to store hostname with its possible task number running on it
   * @return
   */
  def ensureExecutors(sc: SparkContext,
      requiredExecutors: Int,
      localityAwareTasks: Int = 0,
      hostToLocalTaskCount: Map[String, Int] = Map.empty): Boolean = {
    sc.schedulerBackend match {
      case b: CoarseGrainedSchedulerBackend =>
        if (requiredExecutors > 0) {
          LOGGER.info(s"Requesting total executors: $requiredExecutors")
          b.requestTotalExecutors(requiredExecutors, localityAwareTasks, hostToLocalTaskCount)
        }
        true
      case _ =>
        false
    }
  }

  /**
   * This method will calculate how many times a loop will run with an interval of given sleep
   * time to wait for requested executors to come up
    *
    * @return The max retry count
   */
  def calculateMaxRetry(): Int = {
    val remainder = dynamicAllocationSchTimeOut % CarbonCommonConstants
      .CARBON_DYNAMIC_ALLOCATION_SCHEDULER_THREAD_SLEEP_TIME
    val retryCount: Int = dynamicAllocationSchTimeOut / CarbonCommonConstants
      .CARBON_DYNAMIC_ALLOCATION_SCHEDULER_THREAD_SLEEP_TIME
    if (remainder > 0) {
      retryCount + 1
    } else {
      retryCount
    }
  }
}
