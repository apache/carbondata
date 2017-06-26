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

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.processing.newflow.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;

/**
 * DummyClassForTest
 */
public class DummyClassForTest extends AbstractDataLoadProcessorStep {

  private ExecutorService executorService;

  public DummyClassForTest(CarbonDataLoadConfiguration configuration,
      AbstractDataLoadProcessorStep child) {
    super(configuration, child);
  }

  @Override public DataField[] getOutput() {
    return child.getOutput();
  }

  @Override public void initialize() throws CarbonDataLoadingException {

  }

  @Override protected String getStepName() {
    return "Dummy";
  }

  @Override public Iterator<CarbonRowBatch>[] execute() throws CarbonDataLoadingException {
    Iterator<CarbonRowBatch>[] iterators = child.execute();
    this.executorService = Executors.newFixedThreadPool(iterators.length);

    try {
      for (int i = 0; i < iterators.length; i++) {
        executorService.submit(new DummyThread(iterators[i]));
      }
      executorService.shutdown();
      executorService.awaitTermination(2, TimeUnit.DAYS);
    } catch (Exception e) {
      throw new CarbonDataLoadingException("Problem while shutdown the server ", e);
    }
    return null;
  }

  @Override protected CarbonRow processRow(CarbonRow row) {
    return null;
  }
}

/**
 * This thread iterates the iterator
 */
class DummyThread implements Callable<Void> {

  private Iterator<CarbonRowBatch> iterator;

  public DummyThread(Iterator<CarbonRowBatch> iterator) {
    this.iterator = iterator;
  }

  @Override public Void call() throws CarbonDataLoadingException {
    try {
      while (iterator.hasNext()) {
        CarbonRowBatch batch = iterator.next();
        while (batch.hasNext()) {
          CarbonRow row = batch.next();
          // do nothing
        }
      }

    } catch (Exception e) {
      throw new CarbonDataLoadingException(e);
    }
    return null;
  }
}
