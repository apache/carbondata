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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.BucketingInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.loading.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.converter.RowConverter;
import org.apache.carbondata.processing.loading.converter.impl.FieldEncoderFactory;
import org.apache.carbondata.processing.loading.converter.impl.RowConverterImpl;
import org.apache.carbondata.processing.loading.exception.BadRecordFoundException;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.loading.partition.Partitioner;
import org.apache.carbondata.processing.loading.partition.impl.HashPartitionerImpl;
import org.apache.carbondata.processing.loading.partition.impl.SparkHashExpressionPartitionerImpl;
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

  private RowConverterImpl rowConverter;

  // cores used in SDK writer, set by the user
  private short sdkWriterCores;

  // set to true when there is no need to reArrange the data
  private boolean withoutReArrange;
  private boolean isBucketColumnEnabled = false;
  private Partitioner<CarbonRow> partitioner;

  public InputProcessorStepWithNoConverterImpl(CarbonDataLoadConfiguration configuration,
      CarbonIterator<Object[]>[] inputIterators, boolean withoutReArrange) {
    super(configuration, null);
    this.inputIterators = inputIterators;
    this.sdkWriterCores = configuration.getWritingCoresCount();
    this.withoutReArrange = withoutReArrange;
  }

  @Override
  public DataField[] getOutput() {
    return configuration.getDataFields();
  }

  @Override
  public void initialize() throws IOException {
    super.initialize();
    // if logger is enabled then raw data will be required.
    rowConverter =
        new RowConverterImpl(configuration.getDataFields(), configuration, null);
    rowConverter.initialize();
    if (!withoutReArrange) {
      noDictionaryMapping =
          CarbonDataProcessorUtil.getNoDictionaryMapping(configuration.getDataFields());
    }
    dataFieldsWithComplexDataType = new HashMap<>();
    convertComplexDataType(dataFieldsWithComplexDataType);

    dataTypes = new DataType[configuration.getDataFields().length];
    for (int i = 0; i < dataTypes.length; i++) {
      if (configuration.getDataFields()[i].getColumn().getDataType() == DataTypes.DATE) {
        dataTypes[i] = DataTypes.INT;
      } else {
        dataTypes[i] = configuration.getDataFields()[i].getColumn().getDataType();
      }
    }
    if (!withoutReArrange) {
      orderOfData = arrangeData(configuration.getDataFields(), configuration.getHeader());
    }
    if (null != configuration.getBucketingInfo()) {
      this.isBucketColumnEnabled = true;
      initializeBucketColumnPartitioner();
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
    if (CarbonCommonConstants.BUCKET_HASH_METHOD_DEFAULT.equals(
            configuration.getBucketHashMethod())) {
      // keep consistent with both carbon and spark tables.
      this.partitioner = new SparkHashExpressionPartitionerImpl(
              indexes, columnSchemas, bucketingInfo.getNumOfRanges());
    } else if (CarbonCommonConstants.BUCKET_HASH_METHOD_NATIVE.equals(
            configuration.getBucketHashMethod())) {
      // native does not keep consistent with spark, it just use java hash method directly such as
      // Long, String, etc. May have better performance during convert process.
      // But, do not use it when the table need to join with spark bucket tables!
      this.partitioner = new HashPartitionerImpl(
              indexes, columnSchemas, bucketingInfo.getNumOfRanges());
    } else {
      // by default we use SparkHashExpressionPartitionerImpl hash.
      this.partitioner = new SparkHashExpressionPartitionerImpl(
              indexes, columnSchemas, bucketingInfo.getNumOfRanges());
    }

  }

  private void convertComplexDataType(Map<Integer, GenericDataType> dataFieldsWithComplexDataType) {
    DataField[] srcDataField = configuration.getDataFields();
    String nullFormat =
        configuration.getDataLoadProperty(DataLoadProcessorConstants.SERIALIZATION_NULL_FORMAT)
            .toString();
    for (int i = 0; i < srcDataField.length; i++) {
      if (srcDataField[i].getColumn().isComplex()) {
        // create a ComplexDataType
        dataFieldsWithComplexDataType.put(srcDataField[i].getColumn().getOrdinal(),
            FieldEncoderFactory.createComplexDataType(srcDataField[i], nullFormat, null));
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

  @Override
  public Iterator<CarbonRowBatch>[] execute() {
    int batchSize = CarbonProperties.getInstance().getBatchSize();
    List<CarbonIterator<Object[]>>[] readerIterators =
        CarbonDataProcessorUtil.partitionInputReaderIterators(this.inputIterators, sdkWriterCores);
    Iterator<CarbonRowBatch>[] outIterators = new Iterator[readerIterators.length];
    for (int i = 0; i < outIterators.length; i++) {
      outIterators[i] =
          new InputProcessorIterator(readerIterators[i], batchSize,
              rowCounter, orderOfData, noDictionaryMapping, dataTypes, configuration,
              dataFieldsWithComplexDataType, rowConverter, withoutReArrange, isBucketColumnEnabled,
                  partitioner);
    }
    return outIterators;
  }

  @Override
  public void close() {
    if (!closed) {
      super.close();
      for (CarbonIterator inputIterator : inputIterators) {
        inputIterator.close();
      }
    }
  }

  @Override
  protected String getStepName() {
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

    private boolean isHivePartitionTable = false;

    RowConverter converter;
    CarbonDataLoadConfiguration configuration;
    private boolean isBucketColumnEnabled = false;
    private Partitioner<CarbonRow> partitioner;
    private boolean withoutReArrange;

    public InputProcessorIterator(List<CarbonIterator<Object[]>> inputIterators, int batchSize,
        AtomicLong rowCounter, int[] orderOfData, boolean[] noDictionaryMapping,
        DataType[] dataTypes, CarbonDataLoadConfiguration configuration,
        Map<Integer, GenericDataType> dataFieldsWithComplexDataType, RowConverter converter,
        boolean withoutReArrange, boolean bucketColumnEnabled, Partitioner<CarbonRow> partitioner) {
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
      this.isHivePartitionTable =
          configuration.getTableSpec().getCarbonTable().isHivePartitionTable();
      this.configuration = configuration;
      this.converter = converter;
      this.withoutReArrange = withoutReArrange;
      this.isBucketColumnEnabled = bucketColumnEnabled;
      this.partitioner = partitioner;
    }

    @Override
    public boolean hasNext() {
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

    @Override
    public CarbonRowBatch next() {
      return getBatch();
    }

    private CarbonRowBatch getBatch() {
      // Create batch and fill it.
      CarbonRowBatch carbonRowBatch = new CarbonRowBatch(batchSize);
      int count = 0;
      if (!withoutReArrange) {
        while (internalHasNext() && count < batchSize) {
          CarbonRow carbonRow =
              new CarbonRow(convertToNoDictionaryToBytes(currentIterator.next(), dataFields));
          if (configuration.isNonSchemaColumnsPresent()) {
            carbonRow = converter.convert(carbonRow);
          }
          if (isBucketColumnEnabled) {
            short rangeNumber = (short) partitioner.getPartition(carbonRow);
            carbonRow.setRangeId(rangeNumber);
          }
          carbonRowBatch.addRow(carbonRow);
          count++;
        }
      } else {
        while (internalHasNext() && count < batchSize) {
          CarbonRow carbonRow = new CarbonRow(
              convertToNoDictionaryToBytesWithoutReArrange(currentIterator.next(), dataFields));
          if (configuration.isNonSchemaColumnsPresent()) {
            carbonRow = converter.convert(carbonRow);
          }
          if (isBucketColumnEnabled) {
            short rangeNumber = (short) partitioner.getPartition(carbonRow);
            carbonRow.setRangeId(rangeNumber);
          }
          carbonRowBatch.addRow(carbonRow);
          count++;
        }
      }
      rowCounter.getAndAdd(carbonRowBatch.getSize());


      return carbonRowBatch;

    }

    private Object[] convertToNoDictionaryToBytes(Object[] data, DataField[] dataFields) {
      Object[] newData = new Object[dataFields.length];
      Map<String, String> properties =
          configuration.getTableSpec().getCarbonTable().getTableInfo().getFactTable()
              .getTableProperties();
      String spatialProperty = properties.get(CarbonCommonConstants.SPATIAL_INDEX);
      for (int i = 0; i < dataFields.length; i++) {
        if (spatialProperty != null && dataFields[i].getColumn().getColName()
            .equalsIgnoreCase(spatialProperty.trim())) {
          continue;
        }
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
          if (dataTypes[i].isComplexType() && isHivePartitionTable) {
            newData[i] = data[orderOfData[i]];
          } else if (dataTypes[i].isComplexType()) {
            getComplexTypeByteArray(newData, i, data, dataFields[i], orderOfData[i], false);
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

    private Object[] convertToNoDictionaryToBytesWithoutReArrange(Object[] data,
        DataField[] dataFields) {
      Object[] newData = new Object[dataFields.length];
      // now dictionary is removed, no need of no dictionary mapping
      for (int i = 0; i < dataFields.length; i++) {
        if (DataTypeUtil.isPrimitiveColumn(dataTypes[i])) {
          // keep the no dictionary measure column as original data
          newData[i] = data[i];
        } else if (dataTypes[i].isComplexType()) {
          getComplexTypeByteArray(newData, i, data, dataFields[i], i, true);
        } else if (dataTypes[i] == DataTypes.DATE && data[i] instanceof Long) {
          if (dateDictionaryGenerator == null) {
            dateDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
                .getDirectDictionaryGenerator(dataTypes[i], dataFields[i].getDateFormat());
          }
          newData[i] = dateDictionaryGenerator.generateKey((long) data[i]);
        } else {
          newData[i] =
              DataTypeUtil.getBytesDataDataTypeForNoDictionaryColumn(data[i], dataTypes[i]);
        }
      }
      return newData;
    }

    private void getComplexTypeByteArray(Object[] newData, int index, Object[] data,
        DataField dataField, int orderedIndex, boolean isWithoutConverter) {
      ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(byteArray);
      try {
        GenericDataType complexType =
            dataFieldsWithComplexDataType.get(dataField.getColumn().getOrdinal());
        boolean isEmptyBadRecord = Boolean.parseBoolean(
            configuration.getDataLoadProperty(DataLoadProcessorConstants.IS_EMPTY_DATA_BAD_RECORD)
                .toString());
        complexType.writeByteArray(data[orderedIndex], dataOutputStream, logHolder,
            isWithoutConverter, isEmptyBadRecord);
        dataOutputStream.close();
        newData[index] = byteArray.toByteArray();
      } catch (BadRecordFoundException e) {
        throw new CarbonDataLoadingException("Loading Exception: " + e.getMessage(), e);
      } catch (Exception e) {
        throw new CarbonDataLoadingException("Loading Exception", e);
      }
    }

  }

}
