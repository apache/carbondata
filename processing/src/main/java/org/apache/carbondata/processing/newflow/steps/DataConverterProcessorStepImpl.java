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

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.processing.newflow.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.NewFlowDataLoadUtil;
import org.apache.carbondata.processing.newflow.converter.RowConverter;
import org.apache.carbondata.processing.newflow.converter.impl.RowConverterImpl;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;
import org.apache.carbondata.processing.surrogatekeysgenerator.csvbased.BadRecordsLogger;

/**
 * Replace row data fields with dictionary values if column is configured dictionary encoded.
 * And nondictionary columns as well as complex columns will be converted to byte[].
 */
public class DataConverterProcessorStepImpl extends AbstractDataLoadProcessorStep {

  private RowConverter converter;

  public DataConverterProcessorStepImpl(CarbonDataLoadConfiguration configuration,
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
    BadRecordsLogger badRecordLogger = NewFlowDataLoadUtil.createBadRecordLogger(configuration);
    converter = new RowConverterImpl(child.getOutput(), configuration, badRecordLogger);
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
      RowConverter localConverter = converter.createCopyForNewThread();
      @Override public boolean hasNext() {
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
    CarbonRowBatch newBatch = new CarbonRowBatch();
    Iterator<CarbonRow> batchIterator = rowBatch.getBatchIterator();
    while (batchIterator.hasNext()) {
      newBatch.addRow(localConverter.convert(batchIterator.next()));
    }
    return newBatch;
  }

  @Override
  protected CarbonRow processRow(CarbonRow row) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    super.close();
    if (converter != null) {
      converter.finish();
    }
  }
}
