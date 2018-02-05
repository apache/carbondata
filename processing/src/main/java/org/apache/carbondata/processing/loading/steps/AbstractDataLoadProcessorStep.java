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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;

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

  private final LogService LOGGER =
      LogServiceFactory.getLogService(this.getClass().getCanonicalName());

  protected CarbonDataLoadConfiguration configuration;

  protected AbstractDataLoadProcessorStep child;

  protected AtomicLong rowCounter;

  boolean closed;

  AbstractDataLoadProcessorStep(CarbonDataLoadConfiguration configuration,
      AbstractDataLoadProcessorStep child) {
    this.configuration = configuration;
    this.child = child;
    this.rowCounter = new AtomicLong();
    this.closed = false;
  }

  /**
   * The output meta for this step. The data returns from this step is as per this meta.
   */
  DataField[] getOutput() {
    return child.getOutput();
  }

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
  public abstract Iterator<CarbonRowBatch>[] execute() throws CarbonDataLoadingException;

  /**
   * Get the step name for logging purpose.
   */
  private String getStepName() {
    return this.getClass().getSimpleName();
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
