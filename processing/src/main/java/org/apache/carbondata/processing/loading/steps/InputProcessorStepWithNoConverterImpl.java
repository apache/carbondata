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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.loading.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.converter.impl.FieldEncoderFactory;
import org.apache.carbondata.processing.loading.converter.impl.RowConverterImpl;
import org.apache.carbondata.processing.loading.exception.BadRecordFoundException;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;


/**
 * It reads data from record reader and sends data to next step.
 */
public class InputProcessorStepWithNoConverterImpl extends AbstractDataLoadProcessorStep {

  private CarbonIterator<Object[]>[] inputIterators;

  private boolean[] noDictionaryMapping;

  private DataType[] dataTypes;

  private int[] orderOfData;

  private Map<Integer, GenericDataType> dataFieldsWithComplexDataType;

  // cores used in SDK writer, set by the user
  private short sdkWriterCores;

  public InputProcessorStepWithNoConverterImpl(CarbonDataLoadConfiguration configuration,
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
    // if logger is enabled then raw data will be required.
    RowConverterImpl rowConverter =
        new RowConverterImpl(configuration.getDataFields(), configuration, null);
    rowConverter.initialize();
    configuration.setCardinalityFinder(rowConverter);
    noDictionaryMapping =
        CarbonDataProcessorUtil.getNoDictionaryMapping(configuration.getDataFields());

    dataFieldsWithComplexDataType = new HashMap<>();
    convertComplexDataType(dataFieldsWithComplexDataType);

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

  private void convertComplexDataType(Map<Integer, GenericDataType> dataFieldsWithComplexDataType) {
    DataField[] srcDataField = configuration.getDataFields();
    FieldEncoderFactory fieldConverterFactory = FieldEncoderFactory.getInstance();
    String nullFormat =
        configuration.getDataLoadProperty(DataLoadProcessorConstants.SERIALIZATION_NULL_FORMAT)
            .toString();
    boolean isEmptyBadRecord = Boolean.parseBoolean(
        configuration.getDataLoadProperty(DataLoadProcessorConstants.IS_EMPTY_DATA_BAD_RECORD)
            .toString());
    for (int i = 0; i < srcDataField.length; i++) {
      if (srcDataField[i].getColumn().isComplex()) {
        // create a ComplexDataType
        dataFieldsWithComplexDataType.put(srcDataField[i].getColumn().getOrdinal(),
            fieldConverterFactory
                .createComplexDataType(srcDataField[i], configuration.getTableIdentifier(), null,
                    false, null, i, nullFormat, isEmptyBadRecord));
      }
    }
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
    List<CarbonIterator<Object[]>>[] readerIterators =
        CarbonDataProcessorUtil.partitionInputReaderIterators(this.inputIterators, sdkWriterCores);
    Iterator<CarbonRowBatch>[] outIterators = new Iterator[readerIterators.length];
    for (int i = 0; i < outIterators.length; i++) {
      outIterators[i] =
          new InputProcessorIterator(readerIterators[i], batchSize, configuration.isPreFetch(),
              rowCounter, orderOfData, noDictionaryMapping, dataTypes, configuration,
              dataFieldsWithComplexDataType);
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

    private DataField[] dataFields;

    private int[] orderOfData;

    private Map<Integer, GenericDataType> dataFieldsWithComplexDataType;

    private DirectDictionaryGenerator dateDictionaryGenerator;

    private DirectDictionaryGenerator timestampDictionaryGenerator;

    private BadRecordLogHolder logHolder = new BadRecordLogHolder();

    public InputProcessorIterator(List<CarbonIterator<Object[]>> inputIterators, int batchSize,
        boolean preFetch, AtomicLong rowCounter, int[] orderOfData, boolean[] noDictionaryMapping,
        DataType[] dataTypes, CarbonDataLoadConfiguration configuration,
        Map<Integer, GenericDataType> dataFieldsWithComplexDataType) {
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
      this.dataFields = configuration.getDataFields();
      this.orderOfData = orderOfData;
      this.dataFieldsWithComplexDataType = dataFieldsWithComplexDataType;
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
        carbonRowBatch.addRow(
            new CarbonRow(convertToNoDictionaryToBytes(currentIterator.next(), dataFields)));
        count++;
      }
      rowCounter.getAndAdd(carbonRowBatch.getSize());


      return carbonRowBatch;

    }

    private Object[] convertToNoDictionaryToBytes(Object[] data, DataField[] dataFields) {
      Object[] newData = new Object[data.length];
      for (int i = 0; i < data.length; i++) {
        if (i < noDictionaryMapping.length && noDictionaryMapping[i]) {
          if (DataTypeUtil.isPrimitiveColumn(dataTypes[i])) {
            // keep the no dictionary measure column as original data
            newData[i] = data[orderOfData[i]];
          } else {
            newData[i] = DataTypeUtil
                .getBytesDataDataTypeForNoDictionaryColumn(data[orderOfData[i]], dataTypes[i]);
          }
        } else {
          // if this is a complex column then recursively comver the data into Byte Array.
          if (dataTypes[i].isComplexType()) {
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(byteArray);
            try {
              GenericDataType complextType =
                  dataFieldsWithComplexDataType.get(dataFields[i].getColumn().getOrdinal());
              complextType.writeByteArray(data[orderOfData[i]], dataOutputStream, logHolder);
              dataOutputStream.close();
              newData[i] = byteArray.toByteArray();
            } catch (BadRecordFoundException e) {
              throw new CarbonDataLoadingException("Loading Exception: " + e.getMessage(), e);
            } catch (Exception e) {
              throw new CarbonDataLoadingException("Loading Exception", e);
            }
          } else {
            DataType dataType = dataFields[i].getColumn().getDataType();
            if (dataType == DataTypes.DATE && data[orderOfData[i]] instanceof Long) {
              if (dateDictionaryGenerator == null) {
                dateDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
                    .getDirectDictionaryGenerator(dataType, dataFields[i].getDateFormat());
              }
              newData[i] = dateDictionaryGenerator.generateKey((long) data[orderOfData[i]]);
            } else if (dataType == DataTypes.TIMESTAMP && data[orderOfData[i]] instanceof Long) {
              if (timestampDictionaryGenerator == null) {
                timestampDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
                    .getDirectDictionaryGenerator(dataType, dataFields[i].getTimestampFormat());
              }
              newData[i] = timestampDictionaryGenerator.generateKey((long) data[orderOfData[i]]);
            } else {
              newData[i] = data[orderOfData[i]];
            }
          }
        }
      }
      return newData;
    }

  }

}
