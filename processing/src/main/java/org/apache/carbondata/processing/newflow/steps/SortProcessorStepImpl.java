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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.processing.newflow.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;
import org.apache.carbondata.processing.newflow.sort.CarbonSorter;
import org.apache.carbondata.processing.newflow.sort.impl.CarbonParallelReadMergeSorterImpl;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortParameters;

/**
 * It sorts the data and write them to intermediate temp files. These files will be further read
 * by next step for writing to carbondata files.
 */
public class SortProcessorStepImpl extends AbstractDataLoadProcessorStep {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SortProcessorStepImpl.class.getName());

  private CarbonSorter carbonSorter;

  public SortProcessorStepImpl(CarbonDataLoadConfiguration configuration,
      AbstractDataLoadProcessorStep child) {
    super(configuration, child);
  }

  @Override public DataField[] getOutput() {
    return child.getOutput();
  }

  @Override public void intialize() throws CarbonDataLoadingException {
    SortParameters sortParameters = SortParameters.createSortParameters(configuration);
    carbonSorter = new CarbonParallelReadMergeSorterImpl(child.getOutput());
    carbonSorter.initialize(sortParameters);
  }

  @Override public Iterator<CarbonRowBatch>[] execute() throws CarbonDataLoadingException {
    final Iterator<CarbonRowBatch>[] iterators = child.execute();
    Iterator<CarbonRowBatch>[] sortedIterators = carbonSorter.sort(iterators);
    child.finish();
    return sortedIterators;
  }

  @Override protected CarbonRow processRow(CarbonRow row) {
    return null;
  }

  @Override public void close() {
    carbonSorter.close();
  }

  @Override public void finish() {

  }
}
