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

package org.apache.carbondata.processing.loading;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.processing.datamap.DataMapWriterListener;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;

import org.apache.log4j.Logger;

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

  private static final Logger LOGGER =
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
  }

  /**
   * The output meta for this step. The data returns from this step is as per this meta.
   *
   */
  public DataField[] getOutput() {
    return child.getOutput();
  }

  /**
   * Initialization process for this step.
   *
   * @throws IOException
   */
  public void initialize() throws IOException {
    if (LOGGER.isDebugEnabled()) {
      // This thread prints the rows processed in each step for every 10 seconds.
      new Thread() {
        @Override public void run() {
          while (!closed) {
            try {
              LOGGER.debug("Rows processed in step " + getStepName() + " : " + rowCounter.get());
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
   * Tranform the data as per the implementation.
   *
   * @return Array of Iterator with data. It can be processed parallel if implementation class wants
   * @throws CarbonDataLoadingException
   */
  public abstract Iterator<CarbonRowBatch>[] execute() throws CarbonDataLoadingException;

  /**
   * Process the batch of rows as per the step logic.
   *
   * @param rowBatch
   * @return processed row.
   */
  protected CarbonRowBatch processRowBatch(CarbonRowBatch rowBatch) {
    CarbonRowBatch newBatch = new CarbonRowBatch(rowBatch.getSize());
    while (rowBatch.hasNext()) {
      newBatch.addRow(null);
    }
    return newBatch;
  }

  /**
   * Get the step name for logging purpose.
   * @return Step name
   */
  protected abstract String getStepName();

  /**
   * This method registers all writer listeners and returns the listener
   * @param bucketId bucketId
   * @return
   */
  protected DataMapWriterListener getDataMapWriterListener(int bucketId) {
    // todo: this method is useless, will remove it later
    return null;
  }

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
