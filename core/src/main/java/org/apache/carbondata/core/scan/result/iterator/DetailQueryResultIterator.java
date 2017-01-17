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
package org.apache.carbondata.core.scan.result.iterator;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.BatchResult;

/**
 * In case of detail query we cannot keep all the records in memory so for
 * executing that query are returning a iterator over block and every time next
 * call will come it will execute the block and return the result
 */
public class DetailQueryResultIterator extends AbstractDetailQueryResultIterator<BatchResult> {

  private final Object lock = new Object();
  private Future<BatchResult> future;

  public DetailQueryResultIterator(List<BlockExecutionInfo> infos, QueryModel queryModel,
      ExecutorService execService) {
    super(infos, queryModel, execService);
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
        fileReader.finish();
      }
      totalScanTime += System.currentTimeMillis() - startTime;
    } catch (Exception ex) {
      try {
        fileReader.finish();
      } finally {
        throw new RuntimeException(ex);
      }
    }
    return result;
  }

  private Future<BatchResult> execute() {
    return execService.submit(new Callable<BatchResult>() {
      @Override public BatchResult call() {
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
