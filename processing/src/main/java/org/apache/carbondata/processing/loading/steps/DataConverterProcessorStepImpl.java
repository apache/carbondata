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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.schema.BucketingInfo;
import org.apache.carbondata.core.metadata.schema.SortColumnRangeInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.processing.loading.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.loading.BadRecordsLogger;
import org.apache.carbondata.processing.loading.BadRecordsLoggerProvider;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.converter.FieldConverter;
import org.apache.carbondata.processing.loading.converter.RowConverter;
import org.apache.carbondata.processing.loading.converter.impl.RowConverterImpl;
import org.apache.carbondata.processing.loading.partition.Partitioner;
import org.apache.carbondata.processing.loading.partition.impl.HashPartitionerImpl;
import org.apache.carbondata.processing.loading.partition.impl.RangePartitionerImpl;
import org.apache.carbondata.processing.loading.partition.impl.RawRowComparator;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;
import org.apache.carbondata.processing.util.CarbonBadRecordUtil;

import org.apache.commons.lang3.StringUtils;

/**
 * Replace row data fields with dictionary values if column is configured dictionary encoded.
 * And nondictionary columns as well as complex columns will be converted to byte[].
 */
public class DataConverterProcessorStepImpl extends AbstractDataLoadProcessorStep {

  private List<RowConverter> converters;
  private Partitioner<CarbonRow> partitioner;
  private BadRecordsLogger badRecordLogger;
  private boolean isSortColumnRangeEnabled = false;
  private boolean isBucketColumnEnabled = false;

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

    if (null != configuration.getBucketingInfo()) {
      this.isBucketColumnEnabled = true;
      initializeBucketColumnPartitioner();
    } else if (null != configuration.getSortColumnRangeInfo()) {
      this.isSortColumnRangeEnabled = true;
      initializeSortColumnRangesPartitioner();
    }
  }

  /**
   * initialize partitioner for bucket column
   */
  private void initializeBucketColumnPartitioner() {
    List<Integer> indexes = new ArrayList<>();
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    DataField[] inputDataFields = getOutput();
    BucketingInfo bucketingInfo = configuration.getBucketingInfo();
    for (int i = 0; i < inputDataFields.length; i++) {
      for (int j = 0; j < bucketingInfo.getListOfColumns().size(); j++) {
        if (inputDataFields[i].getColumn().getColName()
            .equals(bucketingInfo.getListOfColumns().get(j).getColumnName())) {
          indexes.add(i);
          columnSchemas.add(inputDataFields[i].getColumn().getColumnSchema());
          break;
        }
      }
    }

    // hash partitioner to dispatch rows by bucket column
    this.partitioner =
        new HashPartitionerImpl(indexes, columnSchemas, bucketingInfo.getNumOfRanges());
  }


  /**
   * initialize partitioner for sort column ranges
   */
  private void initializeSortColumnRangesPartitioner() {
    // convert user specified sort-column ranges
    SortColumnRangeInfo sortColumnRangeInfo = configuration.getSortColumnRangeInfo();
    int rangeValueCnt = sortColumnRangeInfo.getUserSpecifiedRanges().length;
    CarbonRow[] convertedSortColumnRanges = new CarbonRow[rangeValueCnt];
    for (int i = 0; i < rangeValueCnt; i++) {
      Object[] fakeOriginRow = new Object[configuration.getDataFields().length];
      String[] oneBound = StringUtils.splitPreserveAllTokens(
          sortColumnRangeInfo.getUserSpecifiedRanges()[i], sortColumnRangeInfo.getSeparator(), -1);
      // set the corresponding sort column
      int j = 0;
      for (int colIdx : sortColumnRangeInfo.getSortColumnIndex()) {
        fakeOriginRow[colIdx] = oneBound[j++];
      }
      CarbonRow fakeCarbonRow = new CarbonRow(fakeOriginRow);
      convertFakeRow(fakeCarbonRow, sortColumnRangeInfo);
      convertedSortColumnRanges[i] = fakeCarbonRow;
    }
    // sort the range bounds (sort in carbon is a little different from what we think)
    Arrays.sort(convertedSortColumnRanges,
        new RawRowComparator(sortColumnRangeInfo.getSortColumnIndex(),
            sortColumnRangeInfo.getIsSortColumnNoDict()));

    // range partitioner to dispatch rows by sort columns
    this.partitioner = new RangePartitionerImpl(convertedSortColumnRanges,
        new RawRowComparator(sortColumnRangeInfo.getSortColumnIndex(),
            sortColumnRangeInfo.getIsSortColumnNoDict()));
  }

  // only convert sort column fields
  private void convertFakeRow(CarbonRow fakeRow, SortColumnRangeInfo sortColumnRangeInfo) {
    FieldConverter[] fieldConverters = converters.get(0).getFieldConverters();
    BadRecordLogHolder logHolder = new BadRecordLogHolder();
    logHolder.setLogged(false);
    for (int colIdx : sortColumnRangeInfo.getSortColumnIndex()) {
      fieldConverters[colIdx].convert(fakeRow, logHolder);
    }
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
      if (isSortColumnRangeEnabled || isBucketColumnEnabled) {
        short rangeNumber = (short) partitioner.getPartition(convertRow);
        convertRow.setRangeId(rangeNumber);
      }
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
    if (isBucketColumnEnabled) {
      return "Data Converter with Bucketing";
    } else if (isSortColumnRangeEnabled) {
      return "Data Converter with sort column range";
    } else {
      return "Data Converter";
    }
  }
}
