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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.sdk.store.conf.StoreConf;
import org.apache.carbondata.sdk.store.service.StoreService;
import org.apache.carbondata.sdk.store.util.StoreUtil;
import org.apache.carbondata.store.impl.Schedulable;
import org.apache.carbondata.store.impl.service.PruneService;
import org.apache.carbondata.store.impl.service.RegistryService;
import org.apache.carbondata.store.impl.service.model.RegisterWorkerRequest;
import org.apache.carbondata.store.impl.service.model.RegisterWorkerResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * Master of CarbonStore.
 * It provides a Registry service and Prune service.
 */
@InterfaceAudience.Internal
public class Master {

  private static Master instance = null;

  private static LogService LOGGER = LogServiceFactory.getLogService(Master.class.getName());

  private StoreConf storeConf;
  private Configuration hadoopConf;
  private RPC.Server registryServer = null;
  private RPC.Server pruneServer = null;
  private RPC.Server storeServer = null;

  // mapping of worker IP address to worker instance
  Map<String, Schedulable> workers = new ConcurrentHashMap<>();

  public Master(StoreConf storeConf) {
    this.storeConf = storeConf;
    this.hadoopConf = storeConf.newHadoopConf();
  }

  /**
   * start registry service
   */
  private void startRegistryService() throws IOException {
    if (registryServer == null) {
      BindException exception;
      // we will try to create service at worse case 100 times
      int numTry = 100;
      String host = storeConf.masterHost();
      int port = storeConf.registryServicePort();
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
      LOGGER.info("registry-service has already started");
    }
  }

  private void stopRegistryService() throws InterruptedException {
    if (registryServer != null) {
      registryServer.stop();
      registryServer.join();
      registryServer = null;
    }
  }

  /**
   * start prune service
   */
  private void startPruneService() throws IOException {
    if (pruneServer == null) {
      BindException exception;
      // we will try to create service at worse case 100 times
      int numTry = 100;
      String host = storeConf.masterHost();
      int port = storeConf.pruneServicePort();
      LOGGER.info("building prune-service on " + host + ":" + port);

      PruneService pruneService = new PruneServiceImpl(storeConf, new Scheduler(this));
      do {
        try {
          pruneServer = new RPC.Builder(hadoopConf)
              .setBindAddress(host)
              .setPort(port)
              .setProtocol(PruneService.class)
              .setInstance(pruneService)
              .build();
          pruneServer.start();
          numTry = 0;
          exception = null;
        } catch (BindException e) {
          // port is occupied, increase the port number and try again
          exception = e;
          LOGGER.error(e, "start prune-service failed");
          port = port + 1;
          numTry = numTry - 1;
        }
      } while (numTry > 0);
      if (exception != null) {
        // we have tried many times, but still failed to find an available port
        throw exception;
      }
      LOGGER.info("prune-service started");
    } else {
      LOGGER.info("prune-service has already started");
    }
  }

  private void stopPruneService() throws InterruptedException {
    if (pruneServer != null) {
      pruneServer.stop();
      pruneServer.join();
      pruneServer = null;
    }
  }

  /**
   * start store service
   */
  private void startStoreService() throws IOException {
    if (storeServer == null) {
      BindException exception;
      // we will try to create service at worse case 100 times
      int numTry = 100;
      String host = storeConf.masterHost();
      int port = storeConf.storeServicePort();
      LOGGER.info("building store-service on " + host + ":" + port);

      StoreService storeService = new StoreServiceImpl(storeConf);
      do {
        try {
          storeServer = new RPC.Builder(hadoopConf)
              .setBindAddress(host)
              .setPort(port)
              .setProtocol(StoreService.class)
              .setInstance(storeService)
              .build();

          storeServer.start();
          numTry = 0;
          exception = null;
        } catch (BindException e) {
          // port is occupied, increase the port number and try again
          exception = e;
          LOGGER.error(e, "start store-service failed");
          port = port + 1;
          numTry = numTry - 1;
        }
      } while (numTry > 0);
      if (exception != null) {
        // we have tried many times, but still failed to find an available port
        throw exception;
      }
      LOGGER.info("store-service started");
    } else {
      LOGGER.info("store-service has already started");
    }
  }

  private void stopStoreService() throws InterruptedException {
    if (storeServer != null) {
      storeServer.stop();
      storeServer.join();
      storeServer = null;
    }
  }

  /**
   * A new searcher is trying to register, add it to the map and connect to this searcher
   */
  RegisterWorkerResponse addWorker(RegisterWorkerRequest request) throws IOException {
    LOGGER.info(
        "Receive Register request from worker " + request.getHostAddress() + ":" + request.getPort()
            + " with " + request.getCores() + " cores");
    String workerId = UUID.randomUUID().toString();
    String workerAddress = request.getHostAddress();
    int workerPort = request.getPort();
    LOGGER.info(
        "connecting to worker " + request.getHostAddress() + ":" + request.getPort() + ", workerId "
            + workerId);

    addWorker(new Schedulable(workerId, workerAddress, workerPort, request.getCores()));
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

  public void stopService() throws InterruptedException {
    stopRegistryService();
    stopPruneService();
  }

  public List<String> getAllWorkerAddresses() {
    return new ArrayList<>(workers.keySet());
  }

  public static void main(String[] args) throws InterruptedException, IOException {
    if (args.length != 2) {
      System.err.println("Usage: Master <log4j file> <properties file>");
      return;
    }

    StoreUtil.initLog4j(args[0]);
    StoreConf conf = new StoreConf(args[1]);
    Master master = getInstance(conf);
    master.startStoreService();
    master.startRegistryService();
    master.startPruneService();
    Thread.sleep(Long.MAX_VALUE);
  }

}