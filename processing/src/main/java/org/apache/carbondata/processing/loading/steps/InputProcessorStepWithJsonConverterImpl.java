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
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.loading.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.loading.BadRecordsLogger;
import org.apache.carbondata.processing.loading.BadRecordsLoggerProvider;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.complexobjects.ArrayObject;
import org.apache.carbondata.processing.loading.complexobjects.StructObject;
import org.apache.carbondata.processing.loading.converter.impl.RowConverterImpl;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;
import org.apache.carbondata.processing.util.CarbonBadRecordUtil;

import org.apache.htrace.fasterxml.jackson.core.type.TypeReference;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

/**
 * It reads data from record reader and sends data to next step.
 */
public class InputProcessorStepWithJsonConverterImpl extends AbstractDataLoadProcessorStep {

  private CarbonIterator<Object[]>[] inputIterators;
  private BadRecordsLogger badRecordLogger;
  private RowConverterImpl rowConverter;

  public InputProcessorStepWithJsonConverterImpl(CarbonDataLoadConfiguration configuration,
      CarbonIterator<Object[]>[] inputIterators) {
    super(configuration, null);
    this.inputIterators = inputIterators;
  }

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(InputProcessorStepWithJsonConverterImpl.class.getName());

  @Override public DataField[] getOutput() {
    return configuration.getDataFields();
  }

  @Override public void initialize() throws IOException {
    super.initialize();
    badRecordLogger = BadRecordsLoggerProvider.createBadRecordLogger(configuration);
    rowConverter =
        new RowConverterImpl(configuration.getDataFields(), configuration, badRecordLogger);
    rowConverter.initialize();
    configuration.setCardinalityFinder(rowConverter);
  }

  @Override public Iterator<CarbonRowBatch>[] execute() {
    int batchSize = CarbonProperties.getInstance().getBatchSize();
    List<CarbonIterator<Object[]>>[] readerIterators = partitionInputReaderIterators();
    Iterator<CarbonRowBatch>[] outIterators = new Iterator[readerIterators.length];
    for (int i = 0; i < outIterators.length; i++) {
      outIterators[i] =
          new InputProcessorIterator(readerIterators[i], batchSize, rowCounter, configuration);
    }
    return outIterators;
  }

  /**
   * Partition input iterators equally as per the number of threads.
   *
   * @return
   */
  private List<CarbonIterator<Object[]>>[] partitionInputReaderIterators() {
    // Get the number of cores configured in property.
    int numberOfCores = CarbonProperties.getInstance().getNumberOfCores();
    // Get the minimum of number of cores and iterators size to get the number of parallel threads
    // to be launched.
    int parallelThreadNumber = Math.min(inputIterators.length, numberOfCores);

    List<CarbonIterator<Object[]>>[] iterators = new List[parallelThreadNumber];
    for (int i = 0; i < parallelThreadNumber; i++) {
      iterators[i] = new ArrayList<>();
    }
    // Equally partition the iterators as per number of threads
    for (int i = 0; i < inputIterators.length; i++) {
      iterators[i % parallelThreadNumber].add(inputIterators[i]);
    }
    return iterators;
  }

  @Override protected CarbonRow processRow(CarbonRow row) {
    return null;
  }

  @Override public void close() {
    if (!closed) {
      super.close();
      for (CarbonIterator inputIterator : inputIterators) {
        inputIterator.close();
      }
      badRecordLogger.closeStreams();
      CarbonBadRecordUtil.renameBadRecord(configuration);
      rowConverter.finish();
    }
  }

  @Override protected String getStepName() {
    return "Input Processor";
  }

  /**
   * This iterator wraps the list of iterators and it starts iterating the each
   * iterator of the list one by one. It also parse the data while iterating it.
   */
  private class InputProcessorIterator extends CarbonIterator<CarbonRowBatch> {

    private List<CarbonIterator<Object[]>> inputIterators;

    private CarbonIterator<Object[]> currentIterator;

    private int counter;

    private int batchSize;

    private boolean nextBatch;

    private boolean firstTime;

    private AtomicLong rowCounter;

    private DataField[] dataFields;

    private InputProcessorIterator(List<CarbonIterator<Object[]>> inputIterators, int batchSize,
        AtomicLong rowCounter, CarbonDataLoadConfiguration configuration) {
      this.inputIterators = inputIterators;
      this.batchSize = batchSize;
      this.counter = 0;
      // Get the first iterator from the list.
      currentIterator = inputIterators.get(counter++);
      this.rowCounter = rowCounter;
      this.nextBatch = false;
      this.firstTime = true;
      this.dataFields = configuration.getDataFields();
    }

    @Override public boolean hasNext() {
      return nextBatch || internalHasNext();
    }

    private boolean internalHasNext() {
      if (firstTime) {
        firstTime = false;
        currentIterator.initialize();
      }
      boolean hasNext = currentIterator.hasNext();
      // If iterator is finished then check for next iterator.
      if (!hasNext) {
        currentIterator.close();
        // Check next iterator is available in the list.
        if (counter < inputIterators.size()) {
          // Get the next iterator from the list.
          currentIterator = inputIterators.get(counter++);
          // Initialize the new iterator
          currentIterator.initialize();
          hasNext = internalHasNext();
        }
      }
      return hasNext;
    }

    @Override public CarbonRowBatch next() {
      return getBatch();
    }

    private CarbonRowBatch getBatch() {
      // Create batch and fill it.
      CarbonRowBatch carbonRowBatch = new CarbonRowBatch(batchSize);
      int count = 0;
      CarbonRow carbonRow;
      while (internalHasNext() && count < batchSize) {
        Object[] row = new Object[0];
        try {
          Object[] data = currentIterator.next();
          // In case of Json string, First element array itself will contain the whole string.
          row = convertJsonToNoDictionaryToBytes(data[0].toString(), dataFields);
        } catch (IOException e) {
          LOGGER.error("failed to convert Json String: " + e.getMessage());
        }
        // set both data and raw data as same,
        // order doesn't matter for json. As it is key value pair lookup.
        // raw data used for bad record handling here..
        carbonRow = new CarbonRow(row, row);
        CarbonRow carbonRow1 =
            InputProcessorStepWithJsonConverterImpl.this.rowConverter.convert(carbonRow);
        carbonRowBatch.addRow(carbonRow1);
        count++;
      }
      rowCounter.getAndAdd(carbonRowBatch.getSize());
      return carbonRowBatch;
    }

    private Object[] convertJsonToNoDictionaryToBytes(String jsonString, DataField[] dataFields)
        throws IOException {
      ObjectMapper objectMapper = new ObjectMapper();
      try {
        Map<String, Object> jsonNodeMap =
            objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {
            });
        return jsonToCarbonRecord(jsonNodeMap, dataFields);
      } catch (IOException e) {
        throw new IOException("Failed to parse Json String: " + e.getMessage());
      }
    }

    private Object[] jsonToCarbonRecord(Map<String, Object> jsonNodeMap, DataField[] dataFields) {
      List<Object> fields = new ArrayList<>();
      for (DataField dataField : dataFields) {
        Object field = jsonToCarbonObject(jsonNodeMap, dataField.getColumn());
        if (field != null) {
          fields.add(field);
        }
      }
      // use this array object to form carbonRow
      return fields.toArray();
    }

    private Object jsonToCarbonObject(Map<String, Object> jsonNodeMap, CarbonColumn column) {
      DataType type = column.getDataType();
      if (DataTypes.isArrayType(type)) {
        CarbonDimension carbonDimension = (CarbonDimension) column;
        int size = carbonDimension.getNumberOfChild();
        ArrayList array = (ArrayList) jsonNodeMap.get(extractChildColumnName(column));
        // stored as array in carbonObject
        Object[] arrayChildObjects = new Object[size];
        for (int i = 0; i < size; i++) {
          CarbonDimension childCol = carbonDimension.getListOfChildDimensions().get(i);
          arrayChildObjects[i] = jsonChildElementToCarbonChildElement(array.get(i), childCol);
        }
        return new ArrayObject(arrayChildObjects);
      } else if (DataTypes.isStructType(type)) {
        CarbonDimension carbonDimension = (CarbonDimension) column;
        int size = carbonDimension.getNumberOfChild();
        Map<String, Object> jsonMap =
            (Map<String, Object>) jsonNodeMap.get(extractChildColumnName(column));
        Object[] structChildObjects = new Object[size];
        for (int i = 0; i < size; i++) {
          CarbonDimension childCol = carbonDimension.getListOfChildDimensions().get(i);
          Object childObject =
              jsonChildElementToCarbonChildElement(jsonMap.get(extractChildColumnName(childCol)),
                  childCol);
          if (childObject != null) {
            structChildObjects[i] = childObject;
          }
        }
        return new StructObject(structChildObjects);
      } else {
        // primitive type
        return jsonNodeMap.get(extractChildColumnName(column)).toString();
      }
    }

    private Object jsonChildElementToCarbonChildElement(Object childObject,
        CarbonDimension column) {
      DataType type = column.getDataType();
      if (DataTypes.isArrayType(type)) {
        int size = column.getNumberOfChild();
        ArrayList array = (ArrayList) childObject;
        // stored as array in carbonObject
        Object[] arrayChildObjects = new Object[size];
        for (int i = 0; i < size; i++) {
          CarbonDimension childCol = column.getListOfChildDimensions().get(i);
          arrayChildObjects[i] = jsonChildElementToCarbonChildElement(array.get(i), childCol);
        }
        return new ArrayObject(arrayChildObjects);
      } else if (DataTypes.isStructType(type)) {
        Map<String, Object> childFieldsMap = (Map<String, Object>) childObject;
        int size = column.getNumberOfChild();
        Object[] structChildObjects = new Object[size];
        for (int i = 0; i < size; i++) {
          CarbonDimension childCol = column.getListOfChildDimensions().get(i);
          Object child = jsonChildElementToCarbonChildElement(
              childFieldsMap.get(extractChildColumnName(childCol)), childCol);
          if (child != null) {
            structChildObjects[i] = child;
          }
        }
        return new StructObject(structChildObjects);
      } else {
        // primitive type
        return childObject.toString();
      }
    }
  }

  private static String extractChildColumnName(CarbonColumn column) {
    String columnName = column.getColName();
    if (columnName.contains(".")) {
      // complex type child column names can be like following
      // a) struct type --> parent.child
      // b) array type --> parent.val.val...child [If create table flow]
      // c) array type --> parent.val0.val1...child [If SDK flow]
      // But json data's key is only child column name. So, extracting below
      String[] splits = columnName.split("\\.");
      columnName = splits[splits.length - 1];
    }
    return columnName;
  }
}
