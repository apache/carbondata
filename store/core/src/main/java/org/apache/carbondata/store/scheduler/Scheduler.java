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

package org.apache.carbondata.store.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.store.exception.WorkerTooBusyException;
import org.apache.carbondata.store.rpc.model.BaseResponse;
import org.apache.carbondata.store.rpc.model.LoadDataRequest;
import org.apache.carbondata.store.rpc.model.QueryRequest;
import org.apache.carbondata.store.rpc.model.QueryResponse;

/**
 * [[Master]] uses Scheduler to pick a Worker to send request
 */
public class Scheduler {

  private static LogService LOGGER = LogServiceFactory.getLogService(Scheduler.class.getName());

  // mapping of worker IP address to worker instance
  private Map<String, Schedulable> ipMapWorker = new HashMap<>();
  private List<Schedulable> workers = new ArrayList<>();
  private AtomicLong index = new AtomicLong(0);
  private ExecutorService executors = Executors.newCachedThreadPool();

  /**
   * Pick a Worker according to the address and workload of the Worker
   * Invoke the RPC and return Future result
   */
  public Future<QueryResponse> sendRequestAsync(final Schedulable worker,
      final QueryRequest request) {

    LOGGER.info("sending search request to worker " + worker);
    worker.workload.incrementAndGet();
    return executors.submit(new Callable<QueryResponse>() {
      @Override public QueryResponse call() {
        return worker.service.query(request);
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
    Schedulable worker = ipMapWorker.get(splitAddress);
    // no local worker available, choose one worker randomly
    if (worker == null) {
      worker = pickNexWorker();
    }
    // check whether worker exceed max workload, if exceeded, pick next worker
    int maxWorkload = CarbonProperties.getMaxWorkloadForWorker(worker.getCores());
    int numTry = workers.size();
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
      throw new WorkerTooBusyException(
          "All workers are busy, number of workers: " + workers.size() + ", workload limit:"
              + maxWorkload);
    }

    return worker;
  }

  public Schedulable pickNexWorker() {
    return workers.get((int) (index.get() % workers.size()));
  }

  /**
   * A new searcher is trying to register, add it to the map and connect to this searcher
   */
  public void addWorker(Schedulable schedulable) {
    workers.add(schedulable);
    ipMapWorker.put(schedulable.getAddress(), schedulable);
  }

  public void removeWorker(String address) {
    Schedulable schedulable = ipMapWorker.get(address);
    if (schedulable != null) {
      ipMapWorker.remove(address);
      workers.remove(schedulable);
    }
  }

  public List<Schedulable> getAllWorkers() {
    return workers;
  }

  public List<String> getAllWorkerAddresses() {
    List<String> addresses = new ArrayList<>(workers.size());
    for (Schedulable worker : workers) {
      addresses.add(worker.getAddress());
    }
    return addresses;
  }
}

