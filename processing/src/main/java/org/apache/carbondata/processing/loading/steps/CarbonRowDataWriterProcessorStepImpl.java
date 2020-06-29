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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.datastore.row.WriteStepRowUtil;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.CarbonThreadFactory;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.index.IndexWriterListener;
import org.apache.carbondata.processing.loading.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.loading.exception.BadRecordFoundException;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.CarbonFactHandler;
import org.apache.carbondata.processing.store.CarbonFactHandlerFactory;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.apache.log4j.Logger;

/**
 * It reads data from sorted files which are generated in previous sort step.
 * And it writes data to carbondata file. It also generates mdk key while writing to carbondata file
 */
public class CarbonRowDataWriterProcessorStepImpl extends AbstractDataLoadProcessorStep {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonRowDataWriterProcessorStepImpl.class.getName());

  private int dimensionWithComplexCount;

  private int noDictWithComplextCount;

  private boolean[] isNoDictionaryDimensionColumn;

  private int directDictionaryDimensionCount;

  private int measureCount;

  private long[] readCounter;

  private long[] writeCounter;

  private CarbonTableIdentifier tableIdentifier;

  private String tableName;

  private Map<String, LocalDictionaryGenerator> localDictionaryGeneratorMap;

  private List<CarbonFactHandler> carbonFactHandlers;

  private ExecutorService executorService = null;

  /* Below 4 list is used when isLoadWithoutConverterWithoutReArrangeStep is set in load model*/
  private ArrayList<Integer> directDictionaryDimensionIndex = new ArrayList<>();

  private ArrayList<Integer> otherDimensionIndex = new ArrayList<>();

  private ArrayList<Integer> complexTypeIndex = new ArrayList<>();

  private ArrayList<Integer> measureIndex = new ArrayList<>();

  public CarbonRowDataWriterProcessorStepImpl(CarbonDataLoadConfiguration configuration,
      AbstractDataLoadProcessorStep child) {
    super(configuration, child);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2669
    this.localDictionaryGeneratorMap =
        CarbonUtil.getLocalDictionaryModel(configuration.getTableSpec().getCarbonTable());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2817
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3333
    this.carbonFactHandlers = new CopyOnWriteArrayList<>();
  }

  @Override
  public void initialize() throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1326
    super.initialize();
    child.initialize();
  }

  private String[] getStoreLocation() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3042
    String[] storeLocation = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(this.configuration.getTableSpec().getCarbonTable(),
            String.valueOf(configuration.getTaskNo()), configuration.getSegmentId(), false, false);
    CarbonDataProcessorUtil.createLocations(storeLocation);
    return storeLocation;
  }

  @Override
  public Iterator<CarbonRowBatch>[] execute() throws CarbonDataLoadingException {
    final Iterator<CarbonRowBatch>[] iterators = child.execute();
    tableIdentifier = configuration.getTableIdentifier().getCarbonTableIdentifier();
    tableName = tableIdentifier.getTableName();
    try {
      readCounter = new long[iterators.length];
      writeCounter = new long[iterators.length];
      dimensionWithComplexCount = configuration.getDimensionCount();
      noDictWithComplextCount =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2388
          configuration.getNoDictionaryCount() + configuration.getComplexDictionaryColumnCount()
              + configuration.getComplexNonDictionaryColumnCount();
      directDictionaryDimensionCount = configuration.getDimensionCount() - noDictWithComplextCount;
      isNoDictionaryDimensionColumn =
          CarbonDataProcessorUtil.getNoDictionaryMapping(configuration.getDataFields());
      measureCount = configuration.getMeasureCount();
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
          .recordDictionaryValue2MdkAdd2FileTime(CarbonTablePath.DEPRECATED_PARTITION_ID,
              System.currentTimeMillis());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3728
      if (configuration.getDataLoadProperty(
          DataLoadProcessorConstants.NO_REARRANGE_OF_ROWS) != null) {
        initializeNoReArrangeIndexes();
      }
      if (iterators.length == 1) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3333
        doExecute(iterators[0], 0);
      } else {
        executorService = Executors.newFixedThreadPool(iterators.length,
            new CarbonThreadFactory("NoSortDataWriterPool:" + configuration.getTableIdentifier()
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3304
                .getCarbonTableIdentifier().getTableName(), true));
        Future[] futures = new Future[iterators.length];
        for (int i = 0; i < iterators.length; i++) {
          futures[i] = executorService.submit(new DataWriterRunnable(iterators[i], i));
        }
        for (Future future : futures) {
          future.get();
        }
      }
    } catch (CarbonDataWriterException e) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3024
      LOGGER.error("Failed for table: " + tableName + " in DataWriterProcessorStepImpl", e);
      throw new CarbonDataLoadingException(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3107
          "Error while initializing data handler : " + e.getMessage(), e);
    } catch (Exception e) {
      LOGGER.error("Failed for table: " + tableName + " in DataWriterProcessorStepImpl", e);
      if (e instanceof BadRecordFoundException) {
        throw new BadRecordFoundException(e.getMessage(), e);
      }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3162
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3163
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3164
      throw new CarbonDataLoadingException(e.getMessage(), e);
    }
    return null;
  }

  private void initializeNoReArrangeIndexes() {
    // Data might have partition columns in the end in new insert into flow.
    // But when convert to 3 parts, just keep in internal order. so derive index for that.
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3637
    List<CarbonColumn> listOfColumns = new ArrayList<>();
    listOfColumns.addAll(configuration.getTableSpec().getCarbonTable().getVisibleDimensions());
    listOfColumns.addAll(configuration.getTableSpec().getCarbonTable().getVisibleMeasures());
    // In case of partition, partition data will be at the end. So, need to keep data position
    Map<String, Integer> dataPositionMap = new HashMap<>();
    int dataPosition = 0;
    for (DataField field : configuration.getDataFields()) {
      dataPositionMap.put(field.getColumn().getColName(), dataPosition++);
    }
    // get the index of each type and to be used in 3 parts conversion
    for (CarbonColumn column : listOfColumns) {
      if (column.hasEncoding(Encoding.DICTIONARY)) {
        directDictionaryDimensionIndex.add(dataPositionMap.get(column.getColName()));
      } else {
        if (column.getDataType().isComplexType()) {
          complexTypeIndex.add(dataPositionMap.get(column.getColName()));
        } else if (column.isMeasure()) {
          measureIndex.add(dataPositionMap.get(column.getColName()));
        } else {
          // other dimensions
          otherDimensionIndex.add(dataPositionMap.get(column.getColName()));
        }
      }
    }
  }

  private void doExecute(Iterator<CarbonRowBatch> iterator, int iteratorIndex) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3042
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3333
    String[] storeLocation = getStoreLocation();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
    IndexWriterListener listener = getIndexWriterListener(0);
    CarbonFactDataHandlerModel model = CarbonFactDataHandlerModel.createCarbonFactDataHandlerModel(
        configuration, storeLocation, 0, iteratorIndex, listener);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2669
    model.setColumnLocalDictGenMap(localDictionaryGeneratorMap);
    CarbonFactHandler dataHandler = null;
    boolean rowsNotExist = true;
    while (iterator.hasNext()) {
      if (rowsNotExist) {
        rowsNotExist = false;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2720
        dataHandler = CarbonFactHandlerFactory.createCarbonFactHandler(model);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2817
        this.carbonFactHandlers.add(dataHandler);
        dataHandler.initialise();
      }
      processBatch(iterator.next(), dataHandler, iteratorIndex);
    }
    try {
      if (!rowsNotExist) {
        finish(dataHandler, iteratorIndex);
      }
    } finally {
      carbonFactHandlers.remove(dataHandler);
    }


  }

  @Override
  protected String getStepName() {
    return "Data Writer";
  }

  private void finish(CarbonFactHandler dataHandler, int iteratorIndex) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2817
    CarbonDataWriterException exception = null;
    try {
      dataHandler.finish();
    } catch (Exception e) {
      // if throw exception from here dataHandler will not be closed.
      // so just holding exception and later throwing exception
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3024
      LOGGER.error("Failed for table: " + tableName + " in  finishing data handler", e);
      exception = new CarbonDataWriterException(
          "Failed for table: " + tableName + " in  finishing data handler", e);
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Record Processed For table: " + tableName);
      String logMessage =
          "Finished Carbon DataWriterProcessorStepImpl: Read: " + readCounter[iteratorIndex] +
              ": Write: " + readCounter[iteratorIndex];
      LOGGER.debug(logMessage);
    }
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordTotalRecords(rowCounter.get());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2817
    try {
      processingComplete(dataHandler);
    } catch (CarbonDataLoadingException e) {
      // only assign when exception is null
      // else it will erase original root cause
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3058
      if (null == exception) {
        exception = new CarbonDataWriterException(e);
      }
    }
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3208
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3208
        .recordDictionaryValue2MdkAdd2FileTime(CarbonTablePath.DEPRECATED_PARTITION_ID,
            System.currentTimeMillis());
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
        .recordMdkGenerateTotalTime(CarbonTablePath.DEPRECATED_PARTITION_ID,
            System.currentTimeMillis());
    if (null != exception) {
      throw exception;
    }
  }

  private void processingComplete(CarbonFactHandler dataHandler) throws CarbonDataLoadingException {
    if (null != dataHandler) {
      try {
        dataHandler.closeHandler();
      } catch (CarbonDataWriterException e) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3024
        LOGGER.error(e.getMessage(), e);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3107
        throw new CarbonDataLoadingException(e);
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
        throw new CarbonDataLoadingException("There is an unexpected error: " + e.getMessage(), e);
      }
    }
  }

  /**
   * convert input CarbonRow to output CarbonRow
   * e.g. There is a table as following,
   * the number of dictionary dimensions is a,
   * the number of no-dictionary dimensions is b,
   * the number of complex dimensions is c,
   * the number of measures is d.
   * input CarbonRow format:  the length of Object[] data is a+b+c+d, the number of all columns.
   * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   * | Part                     | Object item                    | describe                 |
   * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   * | Object[0 ~ a+b-1]        | Integer, byte[], Integer, ...  | dict + no dict dimensions|
   * ----------------------------------------------------------------------------------------
   * | Object[a+b ~ a+b+c-1]    | byte[], byte[], ...            | complex dimensions       |
   * ----------------------------------------------------------------------------------------
   * | Object[a+b+c ~ a+b+c+d-1]| int, byte[], ...               | measures                 |
   * ----------------------------------------------------------------------------------------
   * output CarbonRow format: the length of object[] data is d + (b+c>0?1:0) + 1.
   * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   * | Part                     | Object item                    | describe                 |
   * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   * | Object[d+1]              | byte[]                         | mdkey                    |
   * ----------------------------------------------------------------------------------------
   * | Object[d]                | byte[b+c][]                    | no dict + complex dim    |
   * ----------------------------------------------------------------------------------------
   * | Object[0 ~ d-1]          | int, byte[], ...               | measures                 |
   * ----------------------------------------------------------------------------------------
   *
   * @param row
   * @return
   */
  private CarbonRow convertRow(CarbonRow row) {
    int dictIndex = 0;
    int nonDicIndex = 0;
    int[] dim = new int[this.directDictionaryDimensionCount];
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2896
    Object[] nonDicArray = new Object[this.noDictWithComplextCount];
    // read dimension values
    int dimCount = 0;
    for (; dimCount < isNoDictionaryDimensionColumn.length; dimCount++) {
      if (isNoDictionaryDimensionColumn[dimCount]) {
        nonDicArray[nonDicIndex++] = row.getObject(dimCount);
      } else {
        dim[dictIndex++] = (int) row.getObject(dimCount);
      }
    }

    for (; dimCount < this.dimensionWithComplexCount; dimCount++) {
      nonDicArray[nonDicIndex++] = row.getObject(dimCount);
    }

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1490
    Object[] measures = new Object[measureCount];
    for (int i = 0; i < this.measureCount; i++) {
      measures[i] = row.getObject(i + this.dimensionWithComplexCount);
    }

    return WriteStepRowUtil.fromColumnCategory(dim, nonDicArray, measures);
  }

  private CarbonRow convertRowWithoutRearrange(CarbonRow row) {
    int[] directDictionaryDimension = new int[directDictionaryDimensionIndex.size()];
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3637
    Object[] otherDimension = new Object[otherDimensionIndex.size() + complexTypeIndex.size()];
    Object[] measures = new Object[measureIndex.size()];
    for (int i = 0; i < directDictionaryDimensionIndex.size(); i++) {
      directDictionaryDimension[i] = (int) row.getObject(directDictionaryDimensionIndex.get(i));
    }
    for (int i = 0; i < otherDimensionIndex.size(); i++) {
      otherDimension[i] = row.getObject(otherDimensionIndex.get(i));
    }
    for (int i = 0; i < complexTypeIndex.size(); i++) {
      otherDimension[otherDimensionIndex.size() + i] = row.getObject(complexTypeIndex.get(i));
    }
    for (int i = 0; i < measureIndex.size(); i++) {
      measures[i] = row.getObject(measureIndex.get(i));
    }
    return WriteStepRowUtil.fromColumnCategory(directDictionaryDimension, otherDimension, measures);
  }

  private void processBatch(CarbonRowBatch batch, CarbonFactHandler dataHandler, int iteratorIndex)
      throws CarbonDataLoadingException {
    try {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3728
      if (configuration.getDataLoadProperty(
          DataLoadProcessorConstants.NO_REARRANGE_OF_ROWS) != null) {
        // convert without re-arrange
        while (batch.hasNext()) {
          CarbonRow row = batch.next();
          CarbonRow converted = convertRowWithoutRearrange(row);
          dataHandler.addDataToStore(converted);
          readCounter[iteratorIndex]++;
        }
      } else {
        while (batch.hasNext()) {
          CarbonRow row = batch.next();
          CarbonRow converted = convertRow(row);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3333
          dataHandler.addDataToStore(converted);
          readCounter[iteratorIndex]++;
        }
      }
      writeCounter[iteratorIndex] += batch.getSize();
    } catch (Exception e) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3058
      throw new CarbonDataLoadingException(e);
    }
    rowCounter.getAndAdd(batch.getSize());
  }

  class DataWriterRunnable implements Runnable {

    private Iterator<CarbonRowBatch> iterator;
    private int iteratorIndex = 0;

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3333
    DataWriterRunnable(Iterator<CarbonRowBatch> iterator, int iteratorIndex) {
      this.iterator = iterator;
      this.iteratorIndex = iteratorIndex;
    }

    @Override
    public void run() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1992
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
      doExecute(this.iterator, iteratorIndex);
    }
  }

  @Override
  public void close() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2817
    if (!closed) {
      super.close();
      if (null != executorService) {
        executorService.shutdownNow();
      }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3333
      if (null != this.carbonFactHandlers && !this.carbonFactHandlers.isEmpty()) {
        for (CarbonFactHandler carbonFactHandler : this.carbonFactHandlers) {
          carbonFactHandler.finish();
          carbonFactHandler.closeHandler();
        }
      }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3812
      if (configuration.getMetrics() != null) {
        configuration.getMetrics().addOutputRows(rowCounter.get());
      }
    }
  }
}
