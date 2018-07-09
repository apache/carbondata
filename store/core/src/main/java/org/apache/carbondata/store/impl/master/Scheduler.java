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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.store.api.conf.StoreConf;
import org.apache.carbondata.store.api.exception.SchedulerException;
import org.apache.carbondata.store.impl.rpc.model.BaseResponse;
import org.apache.carbondata.store.impl.rpc.model.LoadDataRequest;
import org.apache.carbondata.store.impl.rpc.model.QueryResponse;
import org.apache.carbondata.store.impl.rpc.model.Scan;
import org.apache.carbondata.store.impl.rpc.model.ShutdownRequest;

/**
 * [[Master]] uses Scheduler to pick a Worker to send request
 */
@InterfaceAudience.Internal
public class Scheduler {

  private static LogService LOGGER = LogServiceFactory.getLogService(Scheduler.class.getName());

  private AtomicInteger count = new AtomicInteger(0);
  private ExecutorService executors = Executors.newCachedThreadPool();
  private Master master;

  public Scheduler(StoreConf storeConf) throws IOException {
    master = Master.getInstance(storeConf);
    master.startService();
  }

  /**
   * Pick a Worker according to the address and workload of the Worker
   * Invoke the RPC and return Future result
   */
  public Future<QueryResponse> sendRequestAsync(
      final Schedulable worker, final Scan scan) {
    LOGGER.info("sending search request to worker " + worker);
    worker.workload.incrementAndGet();
    return executors.submit(new Callable<QueryResponse>() {
      @Override public QueryResponse call() {
        return worker.service.query(scan);
      }
    });
  }

  public BaseResponse sendRequest(final Schedulable worker,
      final LoadDataRequest request) {

    LOGGER.info("sending load data request to worker " + worker);
    worker.workload.incrementAndGet();
    return worker.service.loadData(request);
  }

  public Schedulable pickWorker(String splitAddress) {
    Schedulable worker = master.workers.get(splitAddress);
    // no local worker available, choose one worker randomly
    if (worker == null) {
      worker = pickNexWorker();
    }
    // check whether worker exceed max workload, if exceeded, pick next worker
    int maxWorkload = CarbonProperties.getMaxWorkloadForWorker(worker.getCores());
    int numTry = master.workers.size();
    do {
      if (worker.workload.get() >= maxWorkload) {
        LOGGER.info("worker " + worker + " reach limit, re-select worker...");
        worker = pickNexWorker();
        numTry = numTry - 1;
      } else {
        numTry = -1;
      }
    } while (numTry > 0);
    if (numTry == 0) {
      // tried so many times and still not able to find Worker
      throw new SchedulerException(
          "All workers are busy, number of workers: " + master.workers.size() +
              ", workload limit: " + maxWorkload);
    }

    return worker;
  }

  public Schedulable pickNexWorker() {
    if (master.workers.size() == 0) {
      throw new SchedulerException("No worker is available");
    }
    int index = count.getAndIncrement() % master.workers.size();
    return new ArrayList<>(master.workers.values()).get(index);
  }

  public void stopAllWorkers() throws IOException {
    for (Map.Entry<String, Schedulable> entry : master.workers.entrySet()) {
      try {
        entry.getValue().service.shutdown(new ShutdownRequest("user"));
      } catch (Throwable throwable) {
        throw new IOException(throwable);
      }
      master.workers.remove(entry.getKey());
    }
  }

  public void stopService() throws InterruptedException {
    master.stopService();
  }

  public List<String> getAllWorkerAddresses() {
    return new ArrayList<>(master.workers.keySet());
  }
}

