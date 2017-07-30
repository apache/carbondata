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
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.newflow.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.newflow.converter.RowConverter;
import org.apache.carbondata.processing.newflow.converter.impl.RowConverterImpl;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;
import org.apache.carbondata.processing.surrogatekeysgenerator.csvbased.BadRecordsLogger;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

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
    badRecordLogger = createBadRecordLogger(configuration);
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
    CarbonRowBatch newBatch = new CarbonRowBatch(rowBatch.getSize());
    while (rowBatch.hasNext()) {
      newBatch.addRow(localConverter.convert(rowBatch.next()));
    }
    rowCounter.getAndAdd(newBatch.getSize());
    return newBatch;
  }

  @Override
  protected CarbonRow processRow(CarbonRow row) {
    throw new UnsupportedOperationException();
  }

  public static BadRecordsLogger createBadRecordLogger(CarbonDataLoadConfiguration configuration) {
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
        identifier.getTableName() + '_' + System.currentTimeMillis(),
        getBadLogStoreLocation(configuration,
            identifier.getDatabaseName() + CarbonCommonConstants.FILE_SEPARATOR + identifier
                .getTableName() + CarbonCommonConstants.FILE_SEPARATOR + configuration
                .getSegmentId() + CarbonCommonConstants.FILE_SEPARATOR + configuration.getTaskNo()),
        badRecordsLogRedirect, badRecordsLoggerEnable, badRecordConvertNullDisable, isDataLoadFail);
  }

  public static String getBadLogStoreLocation(CarbonDataLoadConfiguration configuration,
      String storeLocation) {
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
      if (null != badRecordLogger) {
        badRecordLogger.closeStreams();
        renameBadRecord(badRecordLogger, configuration);
      }
      super.close();
      if (converters != null) {
        for (RowConverter converter : converters) {
          converter.finish();
        }
      }
    }
  }

  public static void close(BadRecordsLogger badRecordLogger, CarbonDataLoadConfiguration
      configuration, RowConverter converter) {
    if (badRecordLogger != null) {
      badRecordLogger.closeStreams();
      renameBadRecord(badRecordLogger, configuration);
    }
    if (converter != null) {
      converter.finish();
    }
  }

  private static void renameBadRecord(BadRecordsLogger badRecordLogger,
      CarbonDataLoadConfiguration configuration) {
    // rename operation should be performed only in case either bad reccords loggers is enabled
    // or bad records redirect is enabled
    if (badRecordLogger.isBadRecordLoggerEnable() || badRecordLogger.isBadRecordsLogRedirect()) {
      // rename the bad record in progress to normal
      CarbonTableIdentifier identifier =
          configuration.getTableIdentifier().getCarbonTableIdentifier();
      CarbonDataProcessorUtil.renameBadRecordsFromInProgressToNormal(configuration,
          identifier.getDatabaseName() + CarbonCommonConstants.FILE_SEPARATOR + identifier
              .getTableName() + CarbonCommonConstants.FILE_SEPARATOR + configuration.getSegmentId()
              + CarbonCommonConstants.FILE_SEPARATOR + configuration.getTaskNo());
    }
  }

  @Override protected String getStepName() {
    return "Data Converter";
  }
}
