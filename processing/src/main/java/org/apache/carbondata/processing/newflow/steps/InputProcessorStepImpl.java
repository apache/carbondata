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
package org.apache.carbondata.processing.newflow.steps;

import java.util.ArrayList;
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
import org.apache.carbondata.processing.newflow.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.parser.RowParser;
import org.apache.carbondata.processing.newflow.parser.impl.RowParserImpl;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;

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

  public InputProcessorStepImpl(CarbonDataLoadConfiguration configuration,
      CarbonIterator<Object[]>[] inputIterators) {
    super(configuration, null);
    this.inputIterators = inputIterators;
  }

  @Override public DataField[] getOutput() {
    return configuration.getDataFields();
  }

  @Override public void initialize() throws CarbonDataLoadingException {
    rowParser = new RowParserImpl(getOutput(), configuration);
    executorService = Executors.newCachedThreadPool();
  }

  @Override public Iterator<CarbonRowBatch>[] execute() {
    int batchSize = CarbonProperties.getInstance().getBatchSize();
    List<CarbonIterator<Object[]>>[] readerIterators = partitionInputReaderIterators();
    Iterator<CarbonRowBatch>[] outIterators = new Iterator[readerIterators.length];
    for (int i = 0; i < outIterators.length; i++) {
      outIterators[i] =
          new InputProcessorIterator(readerIterators[i], rowParser, batchSize,
              configuration.isPreFetch(), executorService, rowCounter);
    }
    return outIterators;
  }

  /**
   * Partition input iterators equally as per the number of threads.
   * @return
   */
  private List<CarbonIterator<Object[]>>[] partitionInputReaderIterators() {
    // Get the number of cores configured in property.
    int numberOfCores = CarbonProperties.getInstance().getNumberOfCores();
    // Get the minimum of number of cores and iterators size to get the number of parallel threads
    // to be launched.
    int parallelThreadNumber = Math.min(inputIterators.length, numberOfCores);

    List<CarbonIterator<Object[]>>[] iterators = new List[parallelThreadNumber];
    for (int i = 0; i < parallelThreadNumber; i++) {
      iterators[i] = new ArrayList<>();
    }
    // Equally partition the iterators as per number of threads
    for (int i = 0; i < inputIterators.length; i++) {
      iterators[i % parallelThreadNumber].add(inputIterators[i]);
    }
    return iterators;
  }

  @Override protected CarbonRow processRow(CarbonRow row) {
    return null;
  }

  @Override public void close() {
    if (!closed) {
      super.close();
      executorService.shutdown();
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
  private static class InputProcessorIterator extends CarbonIterator<CarbonRowBatch> {

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

    public InputProcessorIterator(List<CarbonIterator<Object[]>> inputIterators,
        RowParser rowParser, int batchSize, boolean preFetch, ExecutorService executorService,
        AtomicLong rowCounter) {
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
      while (internalHasNext() && count < batchSize) {
        carbonRowBatch.addRow(new CarbonRow(rowParser.parseRow(currentIterator.next())));
        count++;
      }
      rowCounter.getAndAdd(carbonRowBatch.getSize());
      return carbonRowBatch;
    }
  }

}
