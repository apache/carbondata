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
import java.util.Comparator;
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
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonThreadFactory;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.datamap.DataMapWriterListener;
import org.apache.carbondata.processing.loading.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.exception.BadRecordFoundException;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.CarbonFactHandler;
import org.apache.carbondata.processing.store.CarbonFactHandlerFactory;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.apache.commons.lang3.tuple.Pair;
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
    this.localDictionaryGeneratorMap =
        CarbonUtil.getLocalDictionaryModel(configuration.getTableSpec().getCarbonTable());
    this.carbonFactHandlers = new CopyOnWriteArrayList<>();
  }

  @Override
  public void initialize() throws IOException {
    super.initialize();
    child.initialize();
  }

  private String[] getStoreLocation() {
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
          configuration.getNoDictionaryCount() + configuration.getComplexDictionaryColumnCount()
              + configuration.getComplexNonDictionaryColumnCount();
      directDictionaryDimensionCount = configuration.getDimensionCount() - noDictWithComplextCount;
      isNoDictionaryDimensionColumn =
          CarbonDataProcessorUtil.getNoDictionaryMapping(configuration.getDataFields());
      measureCount = configuration.getMeasureCount();
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
          .recordDictionaryValue2MdkAdd2FileTime(CarbonTablePath.DEPRECATED_PARTITION_ID,
              System.currentTimeMillis());
      if (configuration.getDataLoadProperty("no_rearrange_of_rows") != null) {
        initializeNoReArrangeIndexes();
      }
      if (iterators.length == 1) {
        doExecute(iterators[0], 0);
      } else {
        executorService = Executors.newFixedThreadPool(iterators.length,
            new CarbonThreadFactory("NoSortDataWriterPool:" + configuration.getTableIdentifier()
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
      LOGGER.error("Failed for table: " + tableName + " in DataWriterProcessorStepImpl", e);
      throw new CarbonDataLoadingException(
          "Error while initializing data handler : " + e.getMessage(), e);
    } catch (Exception e) {
      LOGGER.error("Failed for table: " + tableName + " in DataWriterProcessorStepImpl", e);
      if (e instanceof BadRecordFoundException) {
        throw new BadRecordFoundException(e.getMessage(), e);
      }
      throw new CarbonDataLoadingException(e.getMessage(), e);
    }
    return null;
  }

  private void initializeNoReArrangeIndexes() {
    List<ColumnSchema> listOfColumns =
        configuration.getTableSpec().getCarbonTable().getTableInfo().getFactTable()
            .getListOfColumns();
    List<Integer> internalOrder = new ArrayList<>();
    List<Integer> invisibleIndex = new ArrayList<>();
    for (ColumnSchema col : listOfColumns) {
      // consider the invisible columns other than the dummy measure(-1)
      if (col.isInvisible() && col.getSchemaOrdinal() != -1) {
        invisibleIndex.add(col.getSchemaOrdinal());
      }
    }
    int complexChildCount = 0;
    for (ColumnSchema col : listOfColumns) {
      if (col.isInvisible()) {
        continue;
      }
      if (col.getColumnName().contains(".")) {
        // If the schema ordinal is -1,
        // no need to consider it during shifting columns to derive new shifted ordinal
        if (col.getSchemaOrdinal() != -1) {
          complexChildCount = complexChildCount + 1;
        }
      } else {
        // get number of invisible index count before this column
        int invisibleIndexCount = 0;
        for (int index : invisibleIndex) {
          if (index < col.getSchemaOrdinal()) {
            invisibleIndexCount++;
          }
        }
        if (col.getDataType().isComplexType()) {
          // Calculate re-arrange index by ignoring the complex child count.
          // As projection will have only parent columns
          internalOrder.add(col.getSchemaOrdinal() - complexChildCount - invisibleIndexCount);
        } else {
          internalOrder.add(col.getSchemaOrdinal() - invisibleIndexCount);
        }
      }
    }
    // In case of partition, partition data will be at the end. So, need to keep data position
    List<Pair<DataField, Integer>> dataPositionList = new ArrayList<>();
    int dataPosition = 0;
    for (DataField field : configuration.getDataFields()) {
      dataPositionList.add(Pair.of(field, dataPosition++));
    }
    // convert to original create order
    dataPositionList.sort(Comparator.comparingInt(p -> p.getKey().getColumn().getSchemaOrdinal()));
    // re-arranged data fields
    List<Pair<DataField, Integer>> reArrangedDataFieldList = new ArrayList<>();
    for (int index : internalOrder) {
      reArrangedDataFieldList.add(dataPositionList.get(index));
    }
    // get the index of each type and used for 3 parts conversion
    for (Pair<DataField, Integer> fieldWithDataPosition : reArrangedDataFieldList) {
      if (fieldWithDataPosition.getKey().getColumn().hasEncoding(Encoding.DICTIONARY)) {
        directDictionaryDimensionIndex.add(fieldWithDataPosition.getValue());
      } else {
        if (fieldWithDataPosition.getKey().getColumn().getDataType().isComplexType()) {
          complexTypeIndex.add(fieldWithDataPosition.getValue());
        } else if (fieldWithDataPosition.getKey().getColumn().isMeasure()) {
          measureIndex.add(fieldWithDataPosition.getValue());
        } else {
          // other dimensions
          otherDimensionIndex.add(fieldWithDataPosition.getValue());
        }
      }
    }
  }

  private void doExecute(Iterator<CarbonRowBatch> iterator, int iteratorIndex) {
    String[] storeLocation = getStoreLocation();
    DataMapWriterListener listener = getDataMapWriterListener(0);
    CarbonFactDataHandlerModel model = CarbonFactDataHandlerModel.createCarbonFactDataHandlerModel(
        configuration, storeLocation, 0, iteratorIndex, listener);
    model.setColumnLocalDictGenMap(localDictionaryGeneratorMap);
    CarbonFactHandler dataHandler = null;
    boolean rowsNotExist = true;
    while (iterator.hasNext()) {
      if (rowsNotExist) {
        rowsNotExist = false;
        dataHandler = CarbonFactHandlerFactory.createCarbonFactHandler(model);
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
    CarbonDataWriterException exception = null;
    try {
      dataHandler.finish();
    } catch (Exception e) {
      // if throw exception from here dataHandler will not be closed.
      // so just holding exception and later throwing exception
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
    try {
      processingComplete(dataHandler);
    } catch (CarbonDataLoadingException e) {
      // only assign when exception is null
      // else it will erase original root cause
      if (null == exception) {
        exception = new CarbonDataWriterException(e);
      }
    }
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
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
        LOGGER.error(e.getMessage(), e);
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

    Object[] measures = new Object[measureCount];
    for (int i = 0; i < this.measureCount; i++) {
      measures[i] = row.getObject(i + this.dimensionWithComplexCount);
    }

    return WriteStepRowUtil.fromColumnCategory(dim, nonDicArray, measures);
  }

  private CarbonRow convertRowWithoutRearrange(CarbonRow row) {
    int[] directDictionaryDimension = new int[directDictionaryDimensionIndex.size()];
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
      if (configuration.getDataLoadProperty("no_rearrange_of_rows") != null) {
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
          dataHandler.addDataToStore(converted);
          readCounter[iteratorIndex]++;
        }
      }
      writeCounter[iteratorIndex] += batch.getSize();
    } catch (Exception e) {
      throw new CarbonDataLoadingException(e);
    }
    rowCounter.getAndAdd(batch.getSize());
  }

  class DataWriterRunnable implements Runnable {

    private Iterator<CarbonRowBatch> iterator;
    private int iteratorIndex = 0;

    DataWriterRunnable(Iterator<CarbonRowBatch> iterator, int iteratorIndex) {
      this.iterator = iterator;
      this.iteratorIndex = iteratorIndex;
    }

    @Override
    public void run() {
      doExecute(this.iterator, iteratorIndex);
    }
  }

  @Override
  public void close() {
    if (!closed) {
      super.close();
      if (null != executorService) {
        executorService.shutdownNow();
      }
      if (null != this.carbonFactHandlers && !this.carbonFactHandlers.isEmpty()) {
        for (CarbonFactHandler carbonFactHandler : this.carbonFactHandlers) {
          carbonFactHandler.finish();
          carbonFactHandler.closeHandler();
        }
      }
    }
  }
}
