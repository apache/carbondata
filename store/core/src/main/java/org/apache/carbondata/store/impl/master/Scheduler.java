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

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.sdk.store.Schedulable;
import org.apache.carbondata.sdk.store.exception.SchedulerException;

/**
 * [[Master]] uses Scheduler to pick a Worker to send request
 */
@InterfaceAudience.Internal
class Scheduler {

  private static LogService LOGGER = LogServiceFactory.getLogService(Scheduler.class.getName());

  private AtomicInteger count = new AtomicInteger(0);

  private Master master;

  Scheduler(Master master) {
    this.master = master;
  }

  Schedulable pickWorker(String splitAddress) {
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

  Schedulable pickNexWorker() {
    if (master.workers.size() == 0) {
      throw new SchedulerException("No worker is available");
    }
    int index = count.getAndIncrement() % master.workers.size();
    return new ArrayList<>(master.workers.values()).get(index);
  }
}

