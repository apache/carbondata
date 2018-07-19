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
package org.apache.carbondata.processing.loading.steps;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonThreadFactory;
import org.apache.carbondata.processing.loading.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.parser.RowParser;
import org.apache.carbondata.processing.loading.parser.impl.RowParserImpl;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

/**
 * It reads data from record reader and sends data to next step.
 */
public class InputProcessorStepImpl extends AbstractDataLoadProcessorStep {

  private RowParser rowParser;

  private CarbonIterator<Object[]>[] inputIterators;

  /**
   * executor service to execute the query
   */
  public ExecutorService executorService;
  boolean isRawDataRequired = false;
  public InputProcessorStepImpl(CarbonDataLoadConfiguration configuration,
      CarbonIterator<Object[]>[] inputIterators) {
    super(configuration, null);
    this.inputIterators = inputIterators;
  }

  @Override public DataField[] getOutput() {
    return configuration.getDataFields();
  }

  @Override public void initialize() throws IOException {
    super.initialize();
    rowParser = new RowParserImpl(getOutput(), configuration);
    executorService = Executors.newCachedThreadPool(new CarbonThreadFactory(
        "InputProcessorPool:" + configuration.getTableIdentifier().getCarbonTableIdentifier()
            .getTableName()));
    // if logger is enabled then raw data will be required.
    this.isRawDataRequired = CarbonDataProcessorUtil.isRawDataRequired(configuration);
  }

  @Override public Iterator<CarbonRowBatch>[] execute() {
    int batchSize = CarbonProperties.getInstance().getBatchSize();
    List<CarbonIterator<Object[]>>[] readerIterators =
        CarbonDataProcessorUtil.partitionInputReaderIterators(inputIterators);
    Iterator<CarbonRowBatch>[] outIterators = new Iterator[readerIterators.length];
    for (int i = 0; i < outIterators.length; i++) {
      outIterators[i] =
          new InputProcessorIterator(readerIterators[i], rowParser, batchSize,
              configuration.isPreFetch(), executorService, rowCounter, isRawDataRequired);
    }
    return outIterators;
  }

  @Override public void close() {
    if (!closed) {
      super.close();
      if (null != executorService) {
        executorService.shutdownNow();
      }
      for (CarbonIterator inputIterator : inputIterators) {
        inputIterator.close();
      }
    }
  }

  @Override protected String getStepName() {
    return "Input Processor";
  }

  /**
   * This iterator wraps the list of iterators and it starts iterating the each
   * iterator of the list one by one. It also parse the data while iterating it.
   */
  public static class InputProcessorIterator extends CarbonIterator<CarbonRowBatch> {

    private List<CarbonIterator<Object[]>> inputIterators;

    private CarbonIterator<Object[]> currentIterator;

    private int counter;

    private int batchSize;

    private RowParser rowParser;

    private Future<CarbonRowBatch> future;

    private ExecutorService executorService;

    private boolean nextBatch;

    private boolean firstTime;

    private boolean preFetch;

    private AtomicLong rowCounter;

    private boolean isRawDataRequired = false;

    public InputProcessorIterator(List<CarbonIterator<Object[]>> inputIterators,
        RowParser rowParser, int batchSize, boolean preFetch, ExecutorService executorService,
        AtomicLong rowCounter, boolean isRawDataRequired) {
      this.inputIterators = inputIterators;
      this.batchSize = batchSize;
      this.rowParser = rowParser;
      this.counter = 0;
      // Get the first iterator from the list.
      currentIterator = inputIterators.get(counter++);
      this.executorService = executorService;
      this.rowCounter = rowCounter;
      this.preFetch = preFetch;
      this.nextBatch = false;
      this.firstTime = true;
      this.isRawDataRequired = isRawDataRequired;
    }

    @Override
    public boolean hasNext() {
      return nextBatch || internalHasNext();
    }

    private boolean internalHasNext() {
      if (firstTime) {
        firstTime = false;
        currentIterator.initialize();
      }
      boolean hasNext = currentIterator.hasNext();
      // If iterator is finished then check for next iterator.
      if (!hasNext) {
        currentIterator.close();
        // Check next iterator is available in the list.
        if (counter < inputIterators.size()) {
          // Get the next iterator from the list.
          currentIterator = inputIterators.get(counter++);
          // Initialize the new iterator
          currentIterator.initialize();
          hasNext = internalHasNext();
        }
      }
      return hasNext;
    }

    @Override
    public CarbonRowBatch next() {
      if (preFetch) {
        return getCarbonRowBatchWithPreFetch();
      } else {
        return getBatch();
      }
    }

    private CarbonRowBatch getCarbonRowBatchWithPreFetch() {
      CarbonRowBatch result = null;
      if (future == null) {
        future = getCarbonRowBatch();
      }
      try {
        result = future.get();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
      nextBatch = false;
      if (hasNext()) {
        nextBatch = true;
        future = getCarbonRowBatch();
      }

      return result;
    }

    private Future<CarbonRowBatch> getCarbonRowBatch() {
      return executorService.submit(new Callable<CarbonRowBatch>() {
        @Override public CarbonRowBatch call() throws Exception {
          return getBatch();

        }
      });
    }

    private CarbonRowBatch getBatch() {
      // Create batch and fill it.
      CarbonRowBatch carbonRowBatch = new CarbonRowBatch(batchSize);
      int count = 0;
      if (isRawDataRequired) {
        while (internalHasNext() && count < batchSize) {
          Object[] rawRow = currentIterator.next();
          carbonRowBatch.addRow(new CarbonRow(rowParser.parseRow(rawRow), rawRow));
          count++;
        }
      } else {
        while (internalHasNext() && count < batchSize) {
          carbonRowBatch.addRow(new CarbonRow(rowParser.parseRow(currentIterator.next())));
          count++;
        }
      }
      rowCounter.getAndAdd(carbonRowBatch.getSize());
      return carbonRowBatch;
    }
  }

}
