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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.constants.LoggerAction;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonLoadOptionConstants;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.schema.BucketingInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.loading.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.loading.BadRecordsLogger;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.loading.converter.RowConverter;
import org.apache.carbondata.processing.loading.converter.impl.RowConverterImpl;
import org.apache.carbondata.processing.loading.partition.Partitioner;
import org.apache.carbondata.processing.loading.partition.impl.HashPartitionerImpl;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

/**
 * Replace row data fields with dictionary values if column is configured dictionary encoded.
 * And nondictionary columns as well as complex columns will be converted to byte[].
 */
public class DataConverterProcessorWithBucketingStepImpl extends AbstractDataLoadProcessorStep {

  private List<RowConverter> converters;

  private Partitioner<Object[]> partitioner;

  private BadRecordsLogger badRecordLogger;

  public DataConverterProcessorWithBucketingStepImpl(CarbonDataLoadConfiguration configuration,
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
    badRecordLogger = createBadRecordLogger();
    RowConverter converter =
        new RowConverterImpl(child.getOutput(), configuration, badRecordLogger);
    configuration.setCardinalityFinder(converter);
    converters.add(converter);
    converter.initialize();
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
    partitioner =
        new HashPartitionerImpl(indexes, columnSchemas, bucketingInfo.getNumberOfBuckets());
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
      RowConverter localConverter;
      private boolean first = true;
      @Override public boolean hasNext() {
        if (first) {
          first = false;
          localConverter = converters.get(0).createCopyForNewThread();
          converters.add(localConverter);
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
      CarbonRow row = rowBatch.next();
      short bucketNumber = (short) partitioner.getPartition(row.getData());
      CarbonRow convertRow = localConverter.convert(row);
      convertRow.bucketNumber = bucketNumber;
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

  private BadRecordsLogger createBadRecordLogger() {
    boolean badRecordsLogRedirect = false;
    boolean badRecordConvertNullDisable = false;
    boolean isDataLoadFail = false;
    boolean badRecordsLoggerEnable = Boolean.parseBoolean(
        configuration.getDataLoadProperty(DataLoadProcessorConstants.BAD_RECORDS_LOGGER_ENABLE)
            .toString());
    Object bad_records_action =
        configuration.getDataLoadProperty(DataLoadProcessorConstants.BAD_RECORDS_LOGGER_ACTION)
            .toString();
    if (null != bad_records_action) {
      LoggerAction loggerAction = null;
      try {
        loggerAction = LoggerAction.valueOf(bad_records_action.toString().toUpperCase());
      } catch (IllegalArgumentException e) {
        loggerAction = LoggerAction.FORCE;
      }
      switch (loggerAction) {
        case FORCE:
          badRecordConvertNullDisable = false;
          break;
        case REDIRECT:
          badRecordsLogRedirect = true;
          badRecordConvertNullDisable = true;
          break;
        case IGNORE:
          badRecordsLogRedirect = false;
          badRecordConvertNullDisable = true;
          break;
        case FAIL:
          isDataLoadFail = true;
          break;
      }
    }
    CarbonTableIdentifier identifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();
    return new BadRecordsLogger(identifier.getBadRecordLoggerKey(),
        identifier.getTableName() + '_' + System.currentTimeMillis(), getBadLogStoreLocation(
        identifier.getDatabaseName() + CarbonCommonConstants.FILE_SEPARATOR + identifier
            .getTableName() + CarbonCommonConstants.FILE_SEPARATOR + configuration.getSegmentId()
            + CarbonCommonConstants.FILE_SEPARATOR + configuration.getTaskNo()),
        badRecordsLogRedirect, badRecordsLoggerEnable, badRecordConvertNullDisable, isDataLoadFail);
  }

  private String getBadLogStoreLocation(String storeLocation) {
    String badLogStoreLocation = (String) configuration
        .getDataLoadProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORD_PATH);
    if (null == badLogStoreLocation) {
      badLogStoreLocation =
          CarbonProperties.getInstance().getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC);
    }
    badLogStoreLocation = badLogStoreLocation + File.separator + storeLocation;

    return badLogStoreLocation;
  }

  @Override
  public void close() {
    if (!closed) {
      super.close();
      if (null != badRecordLogger) {
        badRecordLogger.closeStreams();
        renameBadRecord(configuration);
      }
      if (converters != null) {
        for (RowConverter converter : converters) {
          converter.finish();
        }
      }
    }
  }
  private static void renameBadRecord(CarbonDataLoadConfiguration configuration) {
    // rename the bad record in progress to normal
    CarbonTableIdentifier identifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();
    CarbonDataProcessorUtil.renameBadRecordsFromInProgressToNormal(configuration,
        identifier.getDatabaseName() + File.separator + identifier.getTableName()
            + File.separator + configuration.getSegmentId() + File.separator + configuration
            .getTaskNo());
  }
  @Override protected String getStepName() {
    return "Data Converter with Bucketing";
  }
}
