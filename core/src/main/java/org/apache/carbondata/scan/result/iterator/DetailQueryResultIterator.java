/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.scan.result.iterator;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.scan.model.QueryModel;
import org.apache.carbondata.scan.result.BatchResult;

/**
 * In case of detail query we cannot keep all the records in memory so for
 * executing that query are returning a iterator over block and every time next
 * call will come it will execute the block and return the result
 */
public class DetailQueryResultIterator extends AbstractDetailQueryResultIterator {

  private ExecutorService execService = Executors.newFixedThreadPool(1);

  private Future<BatchResult> future;

  private final Object lock = new Object();

  public DetailQueryResultIterator(List<BlockExecutionInfo> infos, QueryModel queryModel) {
    super(infos, queryModel);
  }

  @Override public BatchResult next() {
    BatchResult result;
    long startTime = System.currentTimeMillis();
    try {
      if (future == null) {
        future = execute();
      }
      result = future.get();
      nextBatch = false;
      if (hasNext()) {
        nextBatch = true;
        future = execute();
      } else {
        execService.shutdown();
        execService.awaitTermination(1, TimeUnit.HOURS);
        fileReader.finish();
      }
      totalScanTime += System.currentTimeMillis() - startTime;
    } catch (Exception ex) {
      execService.shutdown();
      fileReader.finish();
      throw new RuntimeException(ex);
    }
    return result;
  }

  private Future<BatchResult> execute() {
    return execService.submit(new Callable<BatchResult>() {
      @Override public BatchResult call() throws QueryExecutionException {
        BatchResult batchResult = new BatchResult();
        synchronized (lock) {
          updateDataBlockIterator();
          if (dataBlockIterator != null) {
            batchResult.setRows(dataBlockIterator.next());
          }
        }
        return batchResult;
      }
    });
  }
}
