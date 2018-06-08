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

package org.apache.spark.rpc

import java.io.IOException
import java.net.{BindException, InetAddress}
import java.util.{List => JList, Map => JMap, Objects, Random, UUID}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SecurityManager, SerializableWritable, SparkConf}
import org.apache.spark.rpc.netty.NettyRpcEnvFactory
import org.apache.spark.search._
import org.apache.spark.util.ThreadUtils

import org.apache.carbondata.common.annotations.InterfaceAudience
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.block.Distributable
import org.apache.carbondata.core.datastore.row.CarbonRow
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit
import org.apache.carbondata.hadoop.api.CarbonInputFormat
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.store.worker.Status

/**
 * Master of CarbonSearch.
 * It provides a Registry service for worker to register.
 * And it provides search API to fire RPC call to workers.
 */
@InterfaceAudience.Internal
class Master(sparkConf: SparkConf) {
  private val LOG = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  // worker host address map to EndpointRef

  private val random = new Random

  private var rpcEnv: RpcEnv = _

  private val scheduler: Scheduler = new Scheduler

  /** start service and listen on port passed in constructor */
  def startService(): Unit = {
    if (rpcEnv == null) {
      LOG.info("Start search mode master thread")
      val isStarted: AtomicBoolean = new AtomicBoolean(false)
      new Thread(new Runnable {
        override def run(): Unit = {
          val hostAddress = InetAddress.getLocalHost.getHostAddress
          var port = CarbonProperties.getSearchMasterPort
          var exception: BindException = null
          var numTry = 100  // we will try to create service at worse case 100 times
          do {
            try {
              LOG.info(s"starting registry-service on $hostAddress:$port")
              val config = RpcUtil.getRpcEnvConfig(
                sparkConf, "registry-service", hostAddress, "", port,
                new SecurityManager(sparkConf), clientMode = false)
              rpcEnv = new NettyRpcEnvFactory().create(config)
              numTry = 0
            } catch {
              case e: BindException =>
                // port is occupied, increase the port number and try again
                exception = e
                LOG.error(s"start registry-service failed: ${e.getMessage}")
                port = port + 1
                numTry = numTry - 1
            }
          } while (numTry > 0)
          if (rpcEnv == null) {
            // we have tried many times, but still failed to find an available port
            throw exception
          }
          val registryEndpoint: RpcEndpoint = new Registry(rpcEnv, Master.this)
          rpcEnv.setupEndpoint("registry-service", registryEndpoint)
          if (isStarted.compareAndSet(false, false)) {
            synchronized {
              isStarted.compareAndSet(false, true)
            }
          }
          LOG.info("registry-service started")
          rpcEnv.awaitTermination()
        }
      }).start()
      var count = 0
      val countThreshold = 5000
      while (isStarted.compareAndSet(false, false) && count < countThreshold) {
        LOG.info(s"Waiting search mode master to start, retrying $count times")
        Thread.sleep(10)
        count = count + 1;
      }
      if (count >= countThreshold) {
        LOG.error(s"Search mode try $countThreshold times to start master but failed")
        throw new RuntimeException(
          s"Search mode try $countThreshold times to start master but failed")
      } else {
        LOG.info("Search mode master started")
      }
    } else {
      LOG.info("Search mode master has already started")
    }
  }

  def stopService(): Unit = {
    if (rpcEnv != null) {
      rpcEnv.shutdown()
      rpcEnv = null
    }
  }

  def stopAllWorkers(): Unit = {
    val futures = scheduler.getAllWorkers.toSeq.map { case (address, schedulable) =>
      (address, schedulable.ref.ask[ShutdownResponse](ShutdownRequest("user")))
    }
    futures.foreach { case (address, future) =>
      ThreadUtils.awaitResult(future, Duration.apply("10s"))
      future.value match {
        case Some(result) =>
          result match {
            case Success(response) => scheduler.removeWorker(address)
            case Failure(throwable) => throw new IOException(throwable.getMessage)
          }
        case None => throw new ExecutionTimeoutException
      }
    }
  }

  /** A new searcher is trying to register, add it to the map and connect to this searcher */
  def addWorker(request: RegisterWorkerRequest): RegisterWorkerResponse = {
    LOG.info(s"Receive Register request from worker ${request.hostAddress}:${request.port} " +
             s"with ${request.cores} cores")
    val workerId = UUID.randomUUID().toString
    val workerAddress = request.hostAddress
    val workerPort = request.port
    LOG.info(s"connecting to worker ${request.hostAddress}:${request.port}, workerId $workerId")

    val endPointRef =
      rpcEnv.setupEndpointRef(RpcAddress(workerAddress, workerPort), "search-service")
    scheduler.addWorker(workerAddress,
      new Schedulable(workerId, workerAddress, workerPort, request.cores, endPointRef))
    LOG.info(s"worker ${request.hostAddress}:${request.port} registered")
    RegisterWorkerResponse(workerId)
  }

  /**
   * Execute search by firing RPC call to worker, return the result rows
   * @param table table to search
   * @param columns projection column names
   * @param filter filter expression
   * @param globalLimit max number of rows required in Master
   * @param localLimit max number of rows required in Worker
   * @return
   */
  def search(table: CarbonTable, columns: Array[String], filter: Expression,
      globalLimit: Long, localLimit: Long): Array[CarbonRow] = {
    Objects.requireNonNull(table)
    Objects.requireNonNull(columns)
    if (globalLimit < 0 || localLimit < 0) {
      throw new IllegalArgumentException("limit should be positive")
    }

    val queryId = random.nextInt
    var rowCount = 0
    val output = new ArrayBuffer[CarbonRow]

    def onSuccess(result: SearchResult): Unit = {
      // in case of RPC success, collect all rows in response message
      if (result.queryId != queryId) {
        throw new IOException(
          s"queryId in response does not match request: ${result.queryId} != $queryId")
      }
      if (result.status != Status.SUCCESS.ordinal()) {
        throw new IOException(s"failure in worker: ${ result.message }")
      }

      val itor = result.rows.iterator
      while (itor.hasNext && rowCount < globalLimit) {
        output += new CarbonRow(itor.next())
        rowCount = rowCount + 1
      }
      LOG.info(s"[SearchId:$queryId] accumulated result size $rowCount")
    }
    def onFaiure(e: Throwable) = throw new IOException(s"exception in worker: ${ e.getMessage }")
    def onTimedout() = throw new ExecutionTimeoutException()

    // prune data and get a mapping of worker hostname to list of blocks,
    // then add these blocks to the SearchRequest and fire the RPC call
    val nodeBlockMapping: JMap[String, JList[Distributable]] = pruneBlock(table, columns, filter)
    val tuple = nodeBlockMapping.asScala.map { case (splitAddress, blocks) =>
      // Build a SearchRequest
      val split = new SerializableWritable[CarbonMultiBlockSplit](
        new CarbonMultiBlockSplit(blocks, splitAddress))
      val request =
        SearchRequest(queryId, split, table.getTableInfo, columns, filter, localLimit)

      // Find an Endpoind and send the request to it
      // This RPC is non-blocking so that we do not need to wait before send to next worker
      scheduler.sendRequestAsync[SearchResult](splitAddress, request)
    }

    // loop to get the result of each Worker
    tuple.foreach { case (worker: Schedulable, future: Future[SearchResult]) =>

      // if we have enough data already, we do not need to collect more result
      if (rowCount < globalLimit) {
        // wait for worker
        val timeout = CarbonProperties
          .getInstance()
          .getProperty(CarbonCommonConstants.CARBON_SEARCH_QUERY_TIMEOUT,
            CarbonCommonConstants.CARBON_SEARCH_QUERY_TIMEOUT_DEFAULT)
        ThreadUtils.awaitResult(future, Duration.apply(timeout))
        LOG.info(s"[SearchId:$queryId] receive search response from worker " +
          s"${worker.address}:${worker.port}")
        try {
          future.value match {
            case Some(response: Try[SearchResult]) =>
              response match {
                case Success(result) => onSuccess(result)
                case Failure(e) => onFaiure(e)
              }
            case None => onTimedout()
          }
        } finally {
          worker.workload.decrementAndGet()
        }
      }
    }
    output.toArray
  }

  /**
   * Prune data by using CarbonInputFormat.getSplit
   * Return a mapping of host address to list of block
   */
  private def pruneBlock(
      table: CarbonTable,
      columns: Array[String],
      filter: Expression): JMap[String, JList[Distributable]] = {
    val jobConf = new JobConf(new Configuration)
    val job = new Job(jobConf)
    val format = CarbonInputFormatUtil.createCarbonTableInputFormat(
      job, table, columns, filter, null, null)

    // We will do FG pruning in reader side, so don't do it here
    CarbonInputFormat.setFgDataMapPruning(job.getConfiguration, false)
    val splits = format.getSplits(job)
    val distributables = splits.asScala.map { split =>
      split.asInstanceOf[Distributable]
    }
    CarbonLoaderUtil.nodeBlockMapping(
      distributables.asJava,
      -1,
      getWorkers.asJava,
      CarbonLoaderUtil.BlockAssignmentStrategy.BLOCK_NUM_FIRST)
  }

  /** return hostname of all workers */
  def getWorkers: Seq[String] = scheduler.getAllWorkers.map(_._1).toSeq
}

// Exception if execution timed out in search mode
class ExecutionTimeoutException extends RuntimeException
