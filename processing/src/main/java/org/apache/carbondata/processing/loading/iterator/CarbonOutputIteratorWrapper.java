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

package org.apache.carbondata.processing.loading.iterator;

import java.util.concurrent.ArrayBlockingQueue;

import org.apache.carbondata.common.CarbonIterator;

/**
 * It is wrapper class to hold the rows in batches when record writer writes the data and allows
 * to iterate on it during data load. It uses blocking queue to coordinate between read and write.
 */
public class CarbonOutputIteratorWrapper extends CarbonIterator<String[]> {

  private boolean close = false;

  /**
   * Number of rows kept in memory at most will be batchSize * queue size
   */
  private int batchSize = 1000;

  private RowBatch loadBatch = new RowBatch(batchSize);

  private RowBatch readBatch;

  private ArrayBlockingQueue<RowBatch> queue = new ArrayBlockingQueue<>(10);

  public void write(String[] row) throws InterruptedException {
    if (!loadBatch.addRow(row)) {
      loadBatch.readyRead();
      queue.put(loadBatch);
      loadBatch = new RowBatch(batchSize);
    }
  }

  @Override
  public boolean hasNext() {
    return !queue.isEmpty() || !close || readBatch != null && readBatch.hasNext();
  }

  @Override
  public String[] next() {
    if (readBatch == null || !readBatch.hasNext()  && !close) {
      try {
        readBatch = queue.take();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    return readBatch.next();
  }

  @Override
  public void close() {
    if (loadBatch.isLoading()) {
      try {
        loadBatch.readyRead();
        if (loadBatch.size > 0) {
          queue.put(loadBatch);
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    close = true;
  }

  private static class RowBatch extends CarbonIterator<String[]> {

    private int counter;

    private String[][] batch;

    private int size;

    private boolean isLoading = true;

    private RowBatch(int size) {
      batch = new String[size][];
      this.size = size;
    }

    /**
     * Add row to the batch, it can hold rows till the batch size.
     * @param row
     * @return false if the row cannot be added as batch is full.
     */
    public boolean addRow(String[] row) {
      batch[counter++] = row;
      return counter < size;
    }

    public void readyRead() {
      size = counter;
      counter = 0;
      isLoading = false;
    }

    public boolean isLoading() {
      return isLoading;
    }

    @Override
    public boolean hasNext() {
      return counter < size;
    }

    @Override
    public String[] next() {
      assert (counter < size);
      return batch[counter++];
    }
  }

}
