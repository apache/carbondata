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

package org.apache.carbondata.store

import java.io.IOException
import java.util.concurrent.{Callable, Executors, Future}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

import org.apache.carbondata.common.annotations.InterfaceAudience
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.store.rpc.QueryService
import org.apache.carbondata.store.rpc.model.{QueryRequest, QueryResponse}

/**
 * [[Master]] uses Scheduler to pick a Worker to send request
 */
@InterfaceAudience.Internal
private[store] class Scheduler {
  // mapping of worker IP address to worker instance
  private val workers = mutable.Map[String, Schedulable]()
  private val random = new Random()
  private val executors = Executors.newCachedThreadPool()
  private val LOG = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Pick a Worker according to the address and workload of the Worker
   * Invoke the RPC and return Future result
   */
  def sendRequestAsync(
      splitAddress: String,
      request: QueryRequest): (Schedulable, Future[QueryResponse]) = {
    require(splitAddress != null)
    if (workers.isEmpty) {
      throw new IOException("No worker is available")
    }
    var worker: Schedulable = pickWorker(splitAddress)

    // check whether worker exceed max workload, if exceeded, pick next worker
    val maxWorkload = CarbonProperties.getMaxWorkloadForWorker(worker.cores)
    var numTry = workers.size
    do {
      if (worker.workload.get() >= maxWorkload) {
        LOG.info(s"worker ${worker.address}:${worker.port} reach limit, re-select worker...")
        worker = pickNextWorker(worker)
        numTry = numTry - 1
      } else {
        numTry = -1
      }
    } while (numTry > 0)
    if (numTry == 0) {
      // tried so many times and still not able to find Worker
      throw new WorkerTooBusyException(
        s"All workers are busy, number of workers: ${workers.size}, workload limit: $maxWorkload")
    }
    LOG.info(s"sending search request to worker ${worker.address}:${worker.port}")
    val future = executors.submit(
      new Callable[QueryResponse] {
        override def call(): QueryResponse = worker.service.query(request)
      }
    )
    worker.workload.incrementAndGet()
    (worker, future)
  }

  private def pickWorker[T: ClassTag](splitAddress: String) = {
    try {
      workers(splitAddress)
    } catch {
      case e: NoSuchElementException =>
        // no local worker available, choose one worker randomly
        pickRandomWorker()
    }
  }

  /** pick a worker randomly */
  private def pickRandomWorker() = {
    val index = random.nextInt(workers.size)
    workers.toSeq(index)._2
  }

  /** pick the next worker of the input worker in the [[Scheduler.workers]] */
  private def pickNextWorker(worker: Schedulable) = {
    val index = workers.zipWithIndex.find { case ((address, w), index) =>
      w == worker
    }.get._2
    if (index == workers.size - 1) {
      workers.toSeq.head._2
    } else {
      workers.toSeq(index + 1)._2
    }
  }

  /** A new searcher is trying to register, add it to the map and connect to this searcher */
  def addWorker(address: String, schedulable: Schedulable): Unit = {
    require(schedulable != null)
    require(address.equals(schedulable.address))
    workers(address) = schedulable
  }

  def removeWorker(address: String): Unit = {
    workers.remove(address)
  }

  def getAllWorkers: Iterator[(String, Schedulable)] = workers.iterator
}

/**
 * Represent a Worker which [[Scheduler]] can send
 * Search request on it
 * @param id Worker ID, a UUID string
 * @param cores, number of cores in Worker
 * @param service RPC service reference
 * @param workload number of outstanding request sent to Worker
 */
private[store] class Schedulable(
    val id: String,
    val address: String,
    val port: Int,
    val cores: Int,
    val service: QueryService,
    var workload: AtomicInteger) {
  def this(id: String, address: String, port: Int, cores: Int, service: QueryService) = {
    this(id, address, port, cores, service, new AtomicInteger())
  }
}

class WorkerTooBusyException(message: String) extends RuntimeException(message)
