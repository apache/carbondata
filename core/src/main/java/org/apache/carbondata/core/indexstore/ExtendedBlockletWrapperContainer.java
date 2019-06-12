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

package org.apache.carbondata.core.indexstore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonThreadFactory;

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * below class will be used to send split information from index driver to
 * main driver.
 * Main driver will Deserialize the extended blocklet object and get the split
 * to run the query
 */
public class ExtendedBlockletWrapperContainer implements Writable {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(ExtendedBlockletWrapperContainer.class.getName());

  private ExtendedBlockletWrapper[] extendedBlockletWrappers;

  private boolean isFallbackJob;

  public ExtendedBlockletWrapperContainer() {

  }

  public ExtendedBlockletWrapperContainer(ExtendedBlockletWrapper[] extendedBlockletWrappers,
      boolean isFallbackJob) {
    this.extendedBlockletWrappers = extendedBlockletWrappers;
    this.isFallbackJob = isFallbackJob;
  }

  public List<ExtendedBlocklet> getExtendedBlockets(String tablePath, String queryId)
      throws IOException {
    if (!isFallbackJob) {
      int numOfThreads = CarbonProperties.getNumOfThreadsForPruning();
      ExecutorService executorService = Executors
          .newFixedThreadPool(numOfThreads, new CarbonThreadFactory("SplitDeseralizerPool", true));
      int numberOfWrapperPerThread = extendedBlockletWrappers.length / numOfThreads;
      int leftOver = extendedBlockletWrappers.length % numOfThreads;
      int[] split = null;
      if (numberOfWrapperPerThread > 0) {
        split = new int[numOfThreads];
      } else {
        split = new int[leftOver];
      }
      Arrays.fill(split, numberOfWrapperPerThread);
      for (int i = 0; i < leftOver; i++) {
        split[i] += 1;
      }
      int start = 0;
      int end = 0;
      List<Future<List<ExtendedBlocklet>>> futures = new ArrayList<>();
      for (int i = 0; i < split.length; i++) {
        end += split[i];
        futures.add(executorService
            .submit(new ExtendedBlockletDeserializerThread(start, end, tablePath, queryId)));
        start += split[i];
      }
      executorService.shutdown();
      try {
        executorService.awaitTermination(1, TimeUnit.HOURS);
      } catch (InterruptedException e) {
        LOGGER.error(e);
        throw new RuntimeException(e);
      }
      List<ExtendedBlocklet> extendedBlocklets = new ArrayList<>();
      for (int i = 0; i < futures.size(); i++) {
        try {
          extendedBlocklets.addAll(futures.get(i).get());
        } catch (InterruptedException | ExecutionException e) {
          LOGGER.error(e);
          throw new RuntimeException(e);
        }
      }
      return extendedBlocklets;
    } else {
      List<ExtendedBlocklet> extendedBlocklets = new ArrayList<>();
      for (ExtendedBlockletWrapper extendedBlockletWrapper: extendedBlockletWrappers) {
        extendedBlocklets.addAll(extendedBlockletWrapper.readBlocklet(tablePath, queryId));
      }
      return extendedBlocklets;
    }
  }

  private class ExtendedBlockletDeserializerThread implements Callable<List<ExtendedBlocklet>> {

    private int start;

    private int end;

    private String tablePath;

    private String queryId;

    public ExtendedBlockletDeserializerThread(int start, int end, String tablePath,
        String queryId) {
      this.start = start;
      this.end = end;
      this.tablePath = tablePath;
      this.queryId = queryId;
    }

    @Override public List<ExtendedBlocklet> call() throws Exception {
      List<ExtendedBlocklet> extendedBlocklets = new ArrayList<>();
      for (int i = start; i < end; i++) {
        extendedBlocklets.addAll(extendedBlockletWrappers[i].readBlocklet(tablePath, queryId));
      }
      return extendedBlocklets;
    }
  }

  @Override public void write(DataOutput out) throws IOException {
    out.writeInt(extendedBlockletWrappers.length);
    for (int i = 0; i < extendedBlockletWrappers.length; i++) {
      extendedBlockletWrappers[i].write(out);
    }
  }

  @Override public void readFields(DataInput in) throws IOException {
    extendedBlockletWrappers = new ExtendedBlockletWrapper[in.readInt()];
    for (int i = 0; i < extendedBlockletWrappers.length; i++) {
      ExtendedBlockletWrapper extendedBlockletWrapper = new ExtendedBlockletWrapper();
      extendedBlockletWrapper.readFields(in);
      extendedBlockletWrappers[i] = extendedBlockletWrapper;
    }
  }
}
