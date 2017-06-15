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

package org.apache.carbondata.processing.newflow;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;

/**
 * This base abstract class for data loading.
 * It can do transformation jobs as per the implementation.
 *
 * Life cycle of this class is
 * First initialize() is called to initialize the step
 * then execute() is called to process the step logic and
 * then close() is called to close any resources if any opened in the step.
 */
public abstract class AbstractDataLoadProcessorStep {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AbstractDataLoadProcessorStep.class.getName());

  protected CarbonDataLoadConfiguration configuration;

  protected AbstractDataLoadProcessorStep child;

  protected AtomicLong rowCounter;

  protected boolean closed;

  public AbstractDataLoadProcessorStep(CarbonDataLoadConfiguration configuration,
      AbstractDataLoadProcessorStep child) {
    this.configuration = configuration;
    this.child = child;
    this.rowCounter = new AtomicLong();
    this.closed = false;

    if (LOGGER.isInfoEnabled()) {
      // This thread prints the rows processed in each step for every 10 seconds.
      new Thread() {
        @Override public void run() {
          while (!closed) {
            try {
              LOGGER.info("Rows processed in step " + getStepName() + " : " + rowCounter.get());
              Thread.sleep(10000);
            } catch (InterruptedException e) {
              //ignore
              LOGGER.error(e.getMessage());
            }
          }
        }
      }.start();
    }
  }

  /**
   * The output meta for this step. The data returns from this step is as per this meta.
   *
   */
  public abstract DataField[] getOutput();

  /**
   * Initialization process for this step.
   *
   * @throws IOException
   */
  public abstract void initialize() throws IOException;

  /**
   * Tranform the data as per the implementation.
   *
   * @return Array of Iterator with data. It can be processed parallel if implementation class wants
   * @throws CarbonDataLoadingException
   */
  public Iterator<CarbonRowBatch>[] execute() throws CarbonDataLoadingException {
    Iterator<CarbonRowBatch>[] childIters = child.execute();
    Iterator<CarbonRowBatch>[] iterators = new Iterator[childIters.length];
    for (int i = 0; i < childIters.length; i++) {
      iterators[i] = getIterator(childIters[i]);
    }
    return iterators;
  }

  /**
   * Create the iterator using child iterator.
   *
   * @param childIter
   * @return new iterator with step specific processing.
   */
  protected Iterator<CarbonRowBatch> getIterator(final Iterator<CarbonRowBatch> childIter) {
    return new CarbonIterator<CarbonRowBatch>() {
      @Override public boolean hasNext() {
        return childIter.hasNext();
      }

      @Override public CarbonRowBatch next() {
        return processRowBatch(childIter.next());
      }
    };
  }

  /**
   * Process the batch of rows as per the step logic.
   *
   * @param rowBatch
   * @return processed row.
   */
  protected CarbonRowBatch processRowBatch(CarbonRowBatch rowBatch) {
    CarbonRowBatch newBatch = new CarbonRowBatch(rowBatch.getSize());
    while (rowBatch.hasNext()) {
      newBatch.addRow(processRow(rowBatch.next()));
    }
    return newBatch;
  }

  /**
   * Process the row as per the step logic.
   *
   * @param row
   * @return processed row.
   */
  protected abstract CarbonRow processRow(CarbonRow row);

  /**
   * Get the step name for logging purpose.
   * @return Step name
   */
  protected abstract String getStepName();


  /**
   * Close all resources.This method is called after execute() is finished.
   * It will be called in both success and failure cases.
   */
  public void close() {
    if (!closed) {
      closed = true;
      LOGGER.info("Total rows processed in step " + this.getStepName() + ": " + rowCounter.get());
      if (child != null) {
        child.close();
      }
    }
  }

}
