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

package org.apache.carbondata.store.impl.worker;

import java.io.IOException;
import java.net.BindException;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.sdk.store.conf.StoreConf;
import org.apache.carbondata.sdk.store.util.StoreUtil;
import org.apache.carbondata.store.impl.rpc.RegistryService;
import org.apache.carbondata.store.impl.rpc.ServiceFactory;
import org.apache.carbondata.store.impl.rpc.StoreService;
import org.apache.carbondata.store.impl.rpc.model.RegisterWorkerRequest;
import org.apache.carbondata.store.impl.rpc.model.RegisterWorkerResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

@InterfaceAudience.Internal
public class Worker {

  private static LogService LOGGER = LogServiceFactory.getLogService(Worker.class.getName());

  private String id;
  private RegistryService registry;
  private StoreConf conf;
  private Configuration hadoopConf;
  private RPC.Server server;

  public Worker(StoreConf conf) {
    this.conf = conf;
    this.hadoopConf = this.conf.newHadoopConf();
  }

  public void start() {
    try {
      startService();
      registerToMaster();
    } catch (IOException e) {
      LOGGER.error(e, "worker failed to start");
    }
  }

  private void startService() throws IOException {
    BindException exception;
    // we will try to create service at worse case 100 times
    int numTry = 100;
    int coreNum = conf.workerCoreNum();
    String host = conf.workerHost();
    int port = conf.workerPort();
    StoreService queryService = new StoreServiceImpl(this);
    do {
      try {
        server = new RPC.Builder(hadoopConf)
            .setNumHandlers(coreNum)
            .setBindAddress(host)
            .setPort(port)
            .setProtocol(StoreService.class)
            .setInstance(queryService)
            .build();
        server.start();

        numTry = 0;
        exception = null;
      } catch (BindException e) {
        // port is occupied, increase the port number and try again
        exception = e;
        port = port + 1;
        numTry = numTry - 1;
      }
    } while (numTry > 0);

    if (exception != null) {
      // we have tried many times, but still failed to find an available port
      LOGGER.error(exception, "worker failed to start");
      throw exception;
    }

    conf.conf(StoreConf.WORKER_PORT, port);
    LOGGER.info("worker started on " + host + ":" + port + " successfully");

  }

  public void stop() {
    try {
      stopService();
    } catch (InterruptedException e) {
      LOGGER.error(e, "worker failed to start");
    }
  }

  private void stopService() throws InterruptedException {
    if (server != null) {
      server.stop();
      server.join();
      server = null;
    }
  }

  private void registerToMaster() throws IOException {
    LOGGER.info("trying to register to master " + conf.masterHost() + ":" + conf.masterPort());
    if (registry == null) {
      registry = ServiceFactory.createRegistryService(conf.masterHost(), conf.masterPort());
    }
    RegisterWorkerRequest request =
        new RegisterWorkerRequest(conf.workerHost(), conf.workerPort(), conf.workerCoreNum());
    try {
      RegisterWorkerResponse response = registry.registerWorker(request);
      id = response.getWorkerId();
    } catch (Throwable throwable) {
      LOGGER.error(throwable, "worker failed to register");
      throw new IOException(throwable);
    }

    LOGGER.info("worker " + id + " registered successfully");
  }

  public String getId() {
    return id;
  }

  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Usage: Worker <log4j file> <properties file>");
      return;
    }

    StoreUtil.initLog4j(args[0]);
    Worker worker = new Worker(new StoreConf(args[1]));
    worker.start();
  }

  public StoreConf getConf() {
    return conf;
  }

  public void setConf(StoreConf conf) {
    this.conf = conf;
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public void setHadoopConf(Configuration hadoopConf) {
    this.hadoopConf = hadoopConf;
  }
}
