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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.processing.loading.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.loading.BadRecordsLogger;
import org.apache.carbondata.processing.loading.BadRecordsLoggerProvider;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.converter.RowConverter;
import org.apache.carbondata.processing.loading.converter.impl.RowConverterImpl;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;
import org.apache.carbondata.processing.util.CarbonBadRecordUtil;

/**
 * Replace row data fields with dictionary values if column is configured dictionary encoded.
 * And nondictionary columns as well as complex columns will be converted to byte[].
 */
public class DataConverterProcessorStepImpl extends AbstractDataLoadProcessorStep {

  private List<RowConverter> converters;
  private BadRecordsLogger badRecordLogger;

  public DataConverterProcessorStepImpl(CarbonDataLoadConfiguration configuration,
      AbstractDataLoadProcessorStep child) {
    super(configuration, child);
  }

  @Override
  public DataField[] getOutput() {
    return child.getOutput();
  }

  @Override
  public void initialize() throws IOException {
    super.initialize();
    child.initialize();
    converters = new ArrayList<>();
    badRecordLogger = BadRecordsLoggerProvider.createBadRecordLogger(configuration);
    RowConverter converter =
        new RowConverterImpl(child.getOutput(), configuration, badRecordLogger);
    configuration.setCardinalityFinder(converter);
    converters.add(converter);
    converter.initialize();
  }

  /**
   * Create the iterator using child iterator.
   *
   * @param childIter
   * @return new iterator with step specific processing.
   */
  @Override
  protected Iterator<CarbonRowBatch> getIterator(final Iterator<CarbonRowBatch> childIter) {
    return new CarbonIterator<CarbonRowBatch>() {
      private boolean first = true;
      private RowConverter localConverter;
      @Override public boolean hasNext() {
        if (first) {
          first = false;
          localConverter = converters.get(0).createCopyForNewThread();
          synchronized (converters) {
            converters.add(localConverter);
          }
        }
        return childIter.hasNext();
      }
      @Override public CarbonRowBatch next() {
        return processRowBatch(childIter.next(), localConverter);
      }
    };
  }

  /**
   * Process the batch of rows as per the step logic.
   *
   * @param rowBatch
   * @return processed row.
   */
  protected CarbonRowBatch processRowBatch(CarbonRowBatch rowBatch, RowConverter localConverter) {
    while (rowBatch.hasNext()) {
      CarbonRow convertRow = localConverter.convert(rowBatch.next());
      rowBatch.setPreviousRow(convertRow);
    }
    rowCounter.getAndAdd(rowBatch.getSize());
    // reuse the origin batch
    rowBatch.rewind();
    return rowBatch;
  }

  @Override
  protected CarbonRow processRow(CarbonRow row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    if (!closed) {
      if (null != badRecordLogger) {
        badRecordLogger.closeStreams();
        CarbonBadRecordUtil.renameBadRecord(configuration);
      }
      super.close();
      if (converters != null) {
        for (RowConverter converter : converters) {
          if (null != converter) {
            converter.finish();
          }
        }
      }
    }
  }

  @Override protected String getStepName() {
    return "Data Converter";
  }
}
