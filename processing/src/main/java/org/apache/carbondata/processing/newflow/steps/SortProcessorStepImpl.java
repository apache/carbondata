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
package org.apache.carbondata.processing.newflow.steps;

import java.util.Iterator;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.newflow.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;
import org.apache.carbondata.processing.newflow.sort.Sorter;
import org.apache.carbondata.processing.newflow.sort.impl.ParallelReadMergeSorterImpl;
import org.apache.carbondata.processing.newflow.sort.impl.UnsafeParallelReadMergeSorterImpl;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortParameters;

/**
 * It sorts the data and write them to intermediate temp files. These files will be further read
 * by next step for writing to carbondata files.
 */
public class SortProcessorStepImpl extends AbstractDataLoadProcessorStep {

  private Sorter sorter;

  public SortProcessorStepImpl(CarbonDataLoadConfiguration configuration,
      AbstractDataLoadProcessorStep child) {
    super(configuration, child);
  }

  @Override
  public DataField[] getOutput() {
    return child.getOutput();
  }

  @Override
  public void initialize() throws CarbonDataLoadingException {
    child.initialize();
    SortParameters sortParameters = SortParameters.createSortParameters(configuration);
    boolean offheapsort = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT,
            CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT));
    if (offheapsort) {
      sorter = new UnsafeParallelReadMergeSorterImpl(child.getOutput());
    } else {
      sorter = new ParallelReadMergeSorterImpl(child.getOutput());
    }
    sorter.initialize(sortParameters);
  }

  @Override
  public Iterator<CarbonRowBatch>[] execute() throws CarbonDataLoadingException {
    final Iterator<CarbonRowBatch>[] iterators = child.execute();
    Iterator<CarbonRowBatch>[] sortedIterators = sorter.sort(iterators);
    child.close();
    return sortedIterators;
  }

  @Override
  protected CarbonRow processRow(CarbonRow row) {
    return null;
  }

  @Override
  public void close() {
    sorter.close();
  }

}
