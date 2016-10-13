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

package org.apache.carbondata.processing.newflow;

import java.util.Iterator;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;

/**
 * This base interface for data loading. It can do transformation jobs as per the implementation.
 */
public abstract class AbstractDataLoadProcessorStep {

  protected CarbonDataLoadConfiguration configuration;

  protected AbstractDataLoadProcessorStep child;

  public AbstractDataLoadProcessorStep(CarbonDataLoadConfiguration configuration,
      AbstractDataLoadProcessorStep child) {
    this.configuration = configuration;
    this.child = child;
  }

  /**
   * The output meta for this step. The data returns from this step is as per this meta.
   *
   * @return
   */
  public abstract DataField[] getOutput();

  /**
   * Intialization process for this step.
   *
   * @throws CarbonDataLoadingException
   */
  public abstract void intialize() throws CarbonDataLoadingException;

  /**
   * Tranform the data as per the implemetation.
   *
   * @return Iterator of data
   * @throws CarbonDataLoadingException
   */
  public Iterator<Object[]> execute() throws CarbonDataLoadingException {
    final Iterator<Object[]> childIter = child.execute();
    return new CarbonIterator<Object[]>() {
      @Override public boolean hasNext() {
        return childIter.hasNext();
      }

      @Override public Object[] next() {
        return processRow(childIter.next());
      }
    };
  }

  /**
   * Process the row as per the step logic.
   *
   * @param row
   * @return processed row.
   */
  protected abstract Object[] processRow(Object[] row);

  /**
   * It is called when task is called successfully.
   */
  public abstract void finish();

  /**
   * Closing of resources after step execution can be done here.
   */
  public abstract void close();

}
