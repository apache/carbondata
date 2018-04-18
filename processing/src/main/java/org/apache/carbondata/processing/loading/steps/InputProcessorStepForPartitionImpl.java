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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.loading.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.converter.impl.RowConverterImpl;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

/**
 * It reads data from record reader and sends data to next step.
 */
public class InputProcessorStepForPartitionImpl extends AbstractDataLoadProcessorStep {

  private CarbonIterator<Object[]>[] inputIterators;

  private boolean[] noDictionaryMapping;

  private DataType[] dataTypes;

  private int[] orderOfData;

  public InputProcessorStepForPartitionImpl(CarbonDataLoadConfiguration configuration,
      CarbonIterator<Object[]>[] inputIterators) {
    super(configuration, null);
    this.inputIterators = inputIterators;
  }

  @Override public DataField[] getOutput() {
    return configuration.getDataFields();
  }

  @Override public void initialize() throws IOException {
    super.initialize();
    // if logger is enabled then raw data will be required.
    RowConverterImpl rowConverter =
        new RowConverterImpl(configuration.getDataFields(), configuration, null);
    rowConverter.initialize();
    configuration.setCardinalityFinder(rowConverter);
    noDictionaryMapping =
        CarbonDataProcessorUtil.getNoDictionaryMapping(configuration.getDataFields());
    dataTypes = new DataType[configuration.getDataFields().length];
    for (int i = 0; i < dataTypes.length; i++) {
      if (configuration.getDataFields()[i].getColumn().hasEncoding(Encoding.DICTIONARY)) {
        dataTypes[i] = DataTypes.INT;
      } else {
        dataTypes[i] = configuration.getDataFields()[i].getColumn().getDataType();
      }
    }
    orderOfData = arrangeData(configuration.getDataFields(), configuration.getHeader());
  }

  private int[] arrangeData(DataField[] dataFields, String[] header) {
    int[] data = new int[dataFields.length];
    for (int i = 0; i < dataFields.length; i++) {
      for (int j = 0; j < header.length; j++) {
        if (dataFields[i].getColumn().getColName().equalsIgnoreCase(header[j])) {
          data[i] = j;
          break;
        }
      }
    }
    return data;
  }

  @Override public Iterator<CarbonRowBatch>[] execute() {
    int batchSize = CarbonProperties.getInstance().getBatchSize();
    List<CarbonIterator<Object[]>>[] readerIterators = partitionInputReaderIterators();
    Iterator<CarbonRowBatch>[] outIterators = new Iterator[readerIterators.length];
    for (int i = 0; i < outIterators.length; i++) {
      outIterators[i] =
          new InputProcessorIterator(readerIterators[i], batchSize, configuration.isPreFetch(),
              rowCounter, orderOfData, noDictionaryMapping, dataTypes);
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
    }
  }

  @Override protected String getStepName() {
    return "Input Processor";
  }

  /**
   * This iterator wraps the list of iterators and it starts iterating the each
   * iterator of the list one by one. It also parse the data while iterating it.
   */
  private static class InputProcessorIterator extends CarbonIterator<CarbonRowBatch> {

    private List<CarbonIterator<Object[]>> inputIterators;

    private CarbonIterator<Object[]> currentIterator;

    private int counter;

    private int batchSize;

    private boolean nextBatch;

    private boolean firstTime;

    private AtomicLong rowCounter;

    private boolean[] noDictionaryMapping;

    private DataType[] dataTypes;

    private int[] orderOfData;

    public InputProcessorIterator(List<CarbonIterator<Object[]>> inputIterators, int batchSize,
        boolean preFetch, AtomicLong rowCounter, int[] orderOfData, boolean[] noDictionaryMapping,
        DataType[] dataTypes) {
      this.inputIterators = inputIterators;
      this.batchSize = batchSize;
      this.counter = 0;
      // Get the first iterator from the list.
      currentIterator = inputIterators.get(counter++);
      this.rowCounter = rowCounter;
      this.nextBatch = false;
      this.firstTime = true;
      this.noDictionaryMapping = noDictionaryMapping;
      this.dataTypes = dataTypes;
      this.orderOfData = orderOfData;
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
      while (internalHasNext() && count < batchSize) {
        carbonRowBatch.addRow(new CarbonRow(convertToNoDictionaryToBytes(currentIterator.next())));
        count++;
      }
      rowCounter.getAndAdd(carbonRowBatch.getSize());
      return carbonRowBatch;
    }

    private Object[] convertToNoDictionaryToBytes(Object[] data) {
      Object[] newData = new Object[data.length];
      for (int i = 0; i < noDictionaryMapping.length; i++) {
        if (noDictionaryMapping[i]) {
          newData[i] = DataTypeUtil
              .getBytesDataDataTypeForNoDictionaryColumn(data[orderOfData[i]], dataTypes[i]);
        } else {
          newData[i] = data[orderOfData[i]];
        }
      }
      if (newData.length > noDictionaryMapping.length) {
        for (int i = noDictionaryMapping.length; i < newData.length; i++) {
          newData[i] = data[orderOfData[i]];
        }
      }
      //      System.out.println(Arrays.toString(data));
      return newData;
    }

  }

}
