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

import org.apache.carbondata.processing.loading.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;
import org.apache.carbondata.processing.loading.sort.Sorter;
import org.apache.carbondata.processing.loading.sort.SorterFactory;
import org.apache.carbondata.processing.sort.sortdata.SortParameters;

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
  public void initialize() throws IOException {
    super.initialize();
    child.initialize();
    SortParameters sortParameters = SortParameters.createSortParameters(configuration);
    sorter = SorterFactory.createSorter(configuration, rowCounter);
    sorter.initialize(sortParameters);
  }

  @Override
  public Iterator<CarbonRowBatch>[] execute() throws CarbonDataLoadingException {
    final Iterator<CarbonRowBatch>[] iterators = child.execute();
    return sorter.sort(iterators);
  }

  @Override
  public void close() {
    if (!closed) {
      super.close();
      if (sorter != null) {
        sorter.close();
      }
    }
  }

  @Override protected String getStepName() {
    return "Sort Processor";
  }
}
