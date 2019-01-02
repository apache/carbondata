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
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.log4j.Logger;

/**
 * It is wrapper class to hold the rows in batches when record writer writes the data and allows
 * to iterate on it during data load. It uses blocking queue to coordinate between read and write.
 */
public class CarbonOutputIteratorWrapper extends CarbonIterator<Object[]> {

  private static final Logger LOG =
      LogServiceFactory.getLogService(CarbonOutputIteratorWrapper.class.getName());

  private boolean close;

  /**
   * Number of rows kept in memory at most will be batchSize * queue size
   */
  private int batchSize = CarbonProperties.getInstance().getBatchSize();

  private RowBatch loadBatch = new RowBatch(batchSize);

  private RowBatch readBatch;

  private ArrayBlockingQueue<RowBatch> queue = new ArrayBlockingQueue<>(10);

  public void write(Object[] row) throws InterruptedException {
    if (close) {
      // already might be closed forcefully
      return;
    }
    if (!loadBatch.addRow(row)) {
      loadBatch.readyRead();
      queue.put(loadBatch);
      loadBatch = new RowBatch(batchSize);
    }
  }

  @Override
  public boolean hasNext() {
    if (readBatch == null || !readBatch.hasNext()) {
      try {
        if (!close) {
          readBatch = queue.poll(5, TimeUnit.MINUTES);
          if (readBatch == null) {
            LOG.warn("This scenario should not happen");
            return false;
          }
        } else {
          readBatch = queue.poll();
          if (readBatch == null) {
            return false;
          }
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    return readBatch.hasNext();
  }

  @Override
  public Object[] next() {
    return readBatch.next();
  }

  public void closeWriter(boolean isForceClose) {
    if (close) {
      // already might be closed forcefully
      return;
    }
    try {
      if (isForceClose) {
        // first make close is set to true, when force close happens because of dead consumer.
        // so that, write() method will stop taking input rows.
        close = true;
        // once write() method stops taking input rows, clear the queue.
        // If queue is cleared before close is set to true, then queue will be again filled
        // by .write() and it can go to blocking put() forever as consumer is dead.
        queue.clear();
        return;
      }
      // below code will ensure that the last RowBatch is consumed properly
      loadBatch.readyRead();
      if (loadBatch.size > 0) {
        queue.put(loadBatch);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    close = true;
    // It is required if the thread waits for take.
    if (queue.isEmpty()) {
      if (!queue.offer(new RowBatch(0))) {
        LOG.warn("The default last element is not added to queue");
      }
    }
  }

  private static class RowBatch extends CarbonIterator<Object[]> {

    private int counter;

    private Object[][] batch;

    private int size;

    private RowBatch(int size) {
      batch = new Object[size][];
      this.size = size;
    }

    /**
     * Add row to the batch, it can hold rows till the batch size.
     * @param row
     * @return false if the row cannot be added as batch is full.
     */
    public boolean addRow(Object[] row) {
      batch[counter++] = row;
      return counter < size;
    }

    public void readyRead() {
      size = counter;
      counter = 0;
    }

    @Override
    public boolean hasNext() {
      return counter < size;
    }

    @Override
    public Object[] next() {
      assert (counter < size);
      return batch[counter++];
    }
  }

}
