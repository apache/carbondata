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
import java.net.{BindException, InetAddress}

import org.apache.hadoop.ipc.RPC

import org.apache.carbondata.common.annotations.InterfaceAudience
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.store.rpc.{QueryService, RegistryService, ServiceFactory}
import org.apache.carbondata.store.rpc.impl.QueryServiceImpl
import org.apache.carbondata.store.rpc.model.RegisterWorkerRequest

@InterfaceAudience.Internal
private[store] object Worker {
  private val LOG = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  private val hostAddress = InetAddress.getLocalHost.getHostAddress
  private var port: Int = _
  private var registry: RegistryService = _

  def init(masterHostAddress: String, masterPort: Int): Unit = {
    LOG.info(s"initializing worker...")
    startService()
    LOG.info(s"registering to master $masterHostAddress:$masterPort")
    val workerId = registerToMaster(masterHostAddress, masterPort)
    LOG.info(s"worker registered to master, workerId: $workerId")
  }

  def buildServer(serverHost: String, serverPort: Int): RPC.Server = {
    val hadoopConf = FileFactory.getConfiguration
    val builder = new RPC.Builder(hadoopConf)
    builder
      .setNumHandlers(Runtime.getRuntime.availableProcessors)
      .setBindAddress(serverHost)
      .setPort(serverPort)
      .setProtocol(classOf[QueryService])
      .setInstance(new QueryServiceImpl)
      .build
  }

  /**
   * Start to listen on port [[CarbonProperties.getSearchWorkerPort]]
   */
  private def startService(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        port = CarbonProperties.getSearchWorkerPort
        var searchServer: RPC.Server = null
        var exception: BindException = null
        var numTry = 100  // we will try to create service at worse case 100 times
        do {
          try {
            LOG.info(s"building search-service on $hostAddress:$port")
            searchServer = buildServer(hostAddress, port)
            numTry = 0
          } catch {
            case e: BindException =>
              // port is occupied, increase the port number and try again
              exception = e
              LOG.error(s"start search-service failed: ${e.getMessage}")
              port = port + 1
              numTry = numTry - 1
          }
        } while (numTry > 0)
        if (searchServer == null) {
          // we have tried many times, but still failed to find an available port
          throw exception
        }
        LOG.info("starting search-service")
        searchServer.start()
        LOG.info("search-service started")
      }
    }).start()
  }

  private def registerToMaster(registryHostAddress: String, registryPort: Int): String = {
    LOG.info(s"trying to register to master $registryHostAddress:$registryPort")
    if (registry == null) {
      registry = ServiceFactory.createRegistryService(registryHostAddress, registryPort)
    }
    val cores = Runtime.getRuntime.availableProcessors()
    val request = new RegisterWorkerRequest(hostAddress, port, cores)
    val response = try {
      registry.registerWorker(request)
    } catch {
      case throwable: Throwable =>
        LOG.error(s"worker failed to registered: $throwable")
        throw new IOException(throwable)
    }

    LOG.info("worker registered")
    response.getWorkerId
  }
}
