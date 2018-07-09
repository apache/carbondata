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

package org.apache.carbondata.store.impl.master;

import java.io.IOException;
import java.net.BindException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.store.api.conf.StoreConf;
import org.apache.carbondata.store.impl.rpc.RegistryService;
import org.apache.carbondata.store.impl.rpc.ServiceFactory;
import org.apache.carbondata.store.impl.rpc.StoreService;
import org.apache.carbondata.store.impl.rpc.model.RegisterWorkerRequest;
import org.apache.carbondata.store.impl.rpc.model.RegisterWorkerResponse;
import org.apache.carbondata.store.util.StoreUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * Master of CarbonSearch.
 * It provides a Registry service for worker to register.
 */
class Master {

  private static Master instance = null;

  private static LogService LOGGER = LogServiceFactory.getLogService(Master.class.getName());

  // worker host address map to EndpointRef
  private StoreConf conf;
  private Configuration hadoopConf;
  private RPC.Server registryServer = null;

  // mapping of worker IP address to worker instance
  Map<String, Schedulable> workers = new ConcurrentHashMap<>();

  private Master(StoreConf conf) {
    this.conf = conf;
    this.hadoopConf = conf.newHadoopConf();
  }

  /**
   * start service and listen on port passed in constructor
   */
  public void startService() throws IOException {
    if (registryServer == null) {

      BindException exception;
      // we will try to create service at worse case 100 times
      int numTry = 100;
      String host = conf.masterHost();
      int port = conf.masterPort();
      LOGGER.info("building registry-service on " + host + ":" + port);

      RegistryService registryService = new RegistryServiceImpl(this);
      do {
        try {
          registryServer = new RPC.Builder(hadoopConf)
              .setBindAddress(host)
              .setPort(port)
              .setProtocol(RegistryService.class)
              .setInstance(registryService)
              .build();

          registryServer.start();
          numTry = 0;
          exception = null;
        } catch (BindException e) {
          // port is occupied, increase the port number and try again
          exception = e;
          LOGGER.error(e, "start registry-service failed");
          port = port + 1;
          numTry = numTry - 1;
        }
      } while (numTry > 0);
      if (exception != null) {
        // we have tried many times, but still failed to find an available port
        throw exception;
      }
      LOGGER.info("registry-service started");
    } else {
      LOGGER.info("Search mode master has already started");
    }
  }

  public void stopService() throws InterruptedException {
    if (registryServer != null) {
      registryServer.stop();
      registryServer.join();
      registryServer = null;
    }
  }

  /**
   * A new searcher is trying to register, add it to the map and connect to this searcher
   */
  public RegisterWorkerResponse addWorker(RegisterWorkerRequest request) throws IOException {
    LOGGER.info(
        "Receive Register request from worker " + request.getHostAddress() + ":" + request.getPort()
            + " with " + request.getCores() + " cores");
    String workerId = UUID.randomUUID().toString();
    String workerAddress = request.getHostAddress();
    int workerPort = request.getPort();
    LOGGER.info(
        "connecting to worker " + request.getHostAddress() + ":" + request.getPort() + ", workerId "
            + workerId);

    StoreService searchService = ServiceFactory.createStoreService(workerAddress, workerPort);
    addWorker(
        new Schedulable(workerId, workerAddress, workerPort, request.getCores(), searchService));
    LOGGER.info("worker " + request + " registered");
    return new RegisterWorkerResponse(workerId);
  }

  /**
   * A new searcher is trying to register, add it to the map and connect to this searcher
   */
  private void addWorker(Schedulable schedulable) {
    workers.put(schedulable.getAddress(), schedulable);
  }

  public static synchronized Master getInstance(StoreConf conf) {
    if (instance == null) {
      instance = new Master(conf);
    }
    return instance;
  }

  public static void main(String[] args) throws InterruptedException {
    if (args.length != 2) {
      System.err.println("Usage: Master <log4j file> <properties file>");
      return;
    }

    StoreUtil.initLog4j(args[0]);
    StoreConf conf = new StoreConf(args[1]);
    Master master = getInstance(conf);
    master.stopService();
  }

}