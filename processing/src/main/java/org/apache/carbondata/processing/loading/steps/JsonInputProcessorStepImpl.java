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

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.loading.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.parser.RowParser;
import org.apache.carbondata.processing.loading.parser.impl.JsonRowParser;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

/**
 * It reads data from record reader and sends data to next step.
 */
public class JsonInputProcessorStepImpl extends AbstractDataLoadProcessorStep {

  private RowParser rowParser;

  private CarbonIterator<Object[]>[] inputIterators;

  private boolean isRawDataRequired = false;

  // cores used in SDK writer, set by the user
  private short sdkWriterCores;

  public JsonInputProcessorStepImpl(CarbonDataLoadConfiguration configuration,
      CarbonIterator<Object[]>[] inputIterators) {
    super(configuration, null);
    this.inputIterators = inputIterators;
    sdkWriterCores = configuration.getWritingCoresCount();
  }

  @Override public DataField[] getOutput() {
    return configuration.getDataFields();
  }

  @Override public void initialize() throws IOException {
    super.initialize();
    rowParser = new JsonRowParser(getOutput());
    // if logger is enabled then raw data will be required.
    this.isRawDataRequired = CarbonDataProcessorUtil.isRawDataRequired(configuration);
  }

  @Override public Iterator<CarbonRowBatch>[] execute() {
    int batchSize = CarbonProperties.getInstance().getBatchSize();
    List<CarbonIterator<Object[]>>[] readerIterators =
        CarbonDataProcessorUtil.partitionInputReaderIterators(inputIterators, sdkWriterCores);
    Iterator<CarbonRowBatch>[] outIterators = new Iterator[readerIterators.length];
    for (int i = 0; i < outIterators.length; i++) {
      outIterators[i] =
          new InputProcessorStepImpl.InputProcessorIterator(readerIterators[i], rowParser,
              batchSize, false, null, rowCounter, isRawDataRequired);
    }
    return outIterators;
  }

  @Override public void close() {
    if (!closed) {
      super.close();
      for (CarbonIterator inputIterator : inputIterators) {
        inputIterator.close();
      }
    }
  }

  @Override protected String getStepName() {
    return "Json Input Processor";
  }
}
