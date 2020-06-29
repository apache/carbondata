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

package org.apache.carbondata.processing.datatypes;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.row.ComplexColumnInfo;
import org.apache.carbondata.core.devapi.BiDictionary;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.converter.impl.binary.BinaryDecoder;
import org.apache.carbondata.processing.loading.dictionary.DirectDictionary;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

/**
 * Primitive DataType stateless object used in data loading
 */
public class PrimitiveDataType implements GenericDataType<Object> {

  private static final long serialVersionUID = -1518322888733363638L;

  /**
   * surrogate index
   */
  private int index;

  /**
   * column name
   */
  private String name;

  /**
   * column parent name
   */
  private String parentName;

  /**
   * column unique id
   */
  private String columnId;

  /**
   * key size
   */
  private int keySize;

  /**
   * array index
   */
  private int outputArrayIndex;

  /**
   * data counter
   */
  private int dataCounter;

  private transient BiDictionary<Integer, Object> dictionaryGenerator;

  private CarbonDimension carbonDimension;

  private boolean isDictionary;

  private String nullFormat;

  private boolean isDirectDictionary;

  private DataType dataType;

  private transient BinaryDecoder binaryDecoder;

  private PrimitiveDataType(int outputArrayIndex, int dataCounter) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1400
    this.outputArrayIndex = outputArrayIndex;
    this.dataCounter = dataCounter;
  }

  /**
   * constructor
   *
   * @param name
   * @param parentName
   * @param columnId
   * @param isDictionary
   */
  public PrimitiveDataType(String name, DataType dataType, String parentName, String columnId,
      boolean isDictionary, String nullFormat) {
    this.name = name;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3206
    this.parentName = parentName;
    this.columnId = columnId;
    this.isDictionary = isDictionary;
    this.nullFormat = nullFormat;
    this.dataType = dataType;
  }

  /**
   * Constructor
   * @param carbonColumn
   * @param parentName
   * @param columnId
   * @param carbonDimension
   * @param nullFormat
   */
  public PrimitiveDataType(CarbonColumn carbonColumn, String parentName, String columnId,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3655
      CarbonDimension carbonDimension, String nullFormat, BinaryDecoder binaryDecoder) {
    this.name = carbonColumn.getColName();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3206
    this.parentName = parentName;
    this.columnId = columnId;
    this.carbonDimension = carbonDimension;
    this.isDictionary = isDictionaryDimension(carbonDimension);
    this.nullFormat = nullFormat;
    this.binaryDecoder = binaryDecoder;
    this.dataType = carbonColumn.getDataType();

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2471
    if (carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY)
        || carbonColumn.getDataType() == DataTypes.DATE) {
      dictionaryGenerator = new DirectDictionary(DirectDictionaryKeyGeneratorFactory
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2606
          .getDirectDictionaryGenerator(carbonDimension.getDataType(),
              getDateFormat(carbonDimension)));
      isDirectDictionary = true;
    }
  }

  /**
   * get dateformat
   * @param carbonDimension
   * @return
   */
  private String getDateFormat(CarbonDimension carbonDimension) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2606
    String format;
    String dateFormat = null;
    if (this.carbonDimension.getDataType() == DataTypes.DATE) {
      dateFormat = carbonDimension.getDateFormat();
    }
    if (dateFormat != null && !dateFormat.trim().isEmpty()) {
      format = dateFormat;
    } else {
      format = CarbonUtil.getFormatFromProperty(dataType);
    }
    return format;
  }

  private boolean isDictionaryDimension(CarbonDimension carbonDimension) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2388
    if (carbonDimension.hasEncoding(Encoding.DICTIONARY)) {
      return true;
    } else {
      return false;
    }
  }

  /*
   * primitive column will not have any child column
   */
  @Override
  public void addChildren(GenericDataType children) {

  }

  /*
   * get column name
   */
  @Override
  public String getName() {
    return name;
  }

  /*
   * get column parent name
   */
  @Override
  public String getParentName() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3206
    return parentName;
  }

  /*
   * get column unique id
   */
  @Override
  public String getColumnNames() {
    return columnId;
  }

  /*
   * primitive column will not have any children
   */
  @Override
  public void getAllPrimitiveChildren(List<GenericDataType> primitiveChild) {

  }

  /*
   * set surrogate index
   */
  @Override
  public void setSurrogateIndex(int surrIndex) {
    if (this.carbonDimension != null && !this.carbonDimension.hasEncoding(Encoding.DICTIONARY)) {
      index = 0;
    } else if (this.carbonDimension == null && !isDictionary) {
      index = 0;
    } else {
      index = surrIndex;
    }
  }

  @Override
  public boolean getIsColumnDictionary() {
    return isDictionary;
  }

  @Override
  public void writeByteArray(Object input, DataOutputStream dataOutputStream,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3761
      BadRecordLogHolder logHolder, Boolean isWithoutConverter) throws IOException {
    String parsedValue = null;
    // write null value
    if (null == input ||
        (this.carbonDimension.getDataType() == DataTypes.STRING && input.equals(nullFormat))) {
      updateNullValue(dataOutputStream, logHolder);
      return;
    }
    // write null value after converter
    if (!isWithoutConverter) {
      parsedValue = DataTypeUtil.parseValue(input.toString(), carbonDimension);
      if (null == parsedValue || (this.carbonDimension.getDataType() == DataTypes.STRING
          && parsedValue.equals(nullFormat))) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2452
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2451
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2450
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2453
        updateNullValue(dataOutputStream, logHolder);
        return;
      }
    }
    // Transform into ByteArray for No Dictionary.
    try {
      if (!this.carbonDimension.getUseActualData()) {
        byte[] value;
        if (isDirectDictionary) {
          value = writeDirectDictionary(input, parsedValue, isWithoutConverter);
        } else {
          // If the input is a long value then this means that logical type was provided by
          // the user using AvroCarbonWriter. In this case directly generate Bytes from value.
          if (this.carbonDimension.getDataType().equals(DataTypes.DATE)
              || this.carbonDimension.getDataType().equals(DataTypes.TIMESTAMP)
              && input instanceof Long) {
            if (dictionaryGenerator != null) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2884
              value = ByteUtil.toXorBytes(((DirectDictionary) dictionaryGenerator)
                  .generateKey((long) input));
            } else {
              if (isWithoutConverter) {
                value = ByteUtil.toXorBytes((Long)input);
              } else {
                value = ByteUtil.toXorBytes(Long.parseLong(parsedValue));
              }
            }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3495
          } else if (this.carbonDimension.getDataType().equals(DataTypes.BINARY)) {
            // write binary data type
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3655
            if (binaryDecoder == null) {
              value = DataTypeUtil.getBytesDataDataTypeForNoDictionaryColumn(input,
                  this.carbonDimension.getDataType());
            } else {
              if (isWithoutConverter) {
                value = binaryDecoder.decode((String)input);
              } else {
                value = binaryDecoder.decode(parsedValue);
              }
            }
          } else {
            // write other data types
            if (isWithoutConverter) {
              value = DataTypeUtil.getBytesDataDataTypeForNoDictionaryColumn(input,
                  this.carbonDimension.getDataType());
            } else {
              value = DataTypeUtil.getBytesBasedOnDataTypeForNoDictionaryColumn(parsedValue,
                  this.carbonDimension.getDataType(), getDateOrTimeFormat());
            }
          }
          if (this.carbonDimension.getDataType() == DataTypes.STRING
              && value.length > CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT) {
            throw new CarbonDataLoadingException("Dataload failed, String size cannot exceed "
                + CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT + " bytes");
          }
        }
        updateValueToByteStream(dataOutputStream, value);
      } else {
        byte[] value;
        if (dictionaryGenerator instanceof DirectDictionary
            && input instanceof Long) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2884
          value = ByteUtil.toXorBytes(
              ((DirectDictionary) dictionaryGenerator).generateKey((long) input));
        } else {
          if (isWithoutConverter) {
            value = DataTypeUtil.getBytesDataDataTypeForNoDictionaryColumn(input,
                this.carbonDimension.getDataType());
          } else {
            value = DataTypeUtil.getBytesBasedOnDataTypeForNoDictionaryColumn(parsedValue,
                this.carbonDimension.getDataType(), getDateOrTimeFormat());
          }
        }
        checkAndWriteByteArray(input, dataOutputStream, logHolder, isWithoutConverter, parsedValue,
            value);
      }
    } catch (NumberFormatException e) {
      // Update logHolder for bad record and put null in dataOutputStream.
      updateNullValue(dataOutputStream, logHolder);
    }
  }

  private void checkAndWriteByteArray(Object input, DataOutputStream dataOutputStream,
      BadRecordLogHolder logHolder, Boolean isWithoutConverter, String parsedValue, byte[] value)
      throws IOException {
    if (isWithoutConverter) {
      if (this.carbonDimension.getDataType() == DataTypes.STRING && input instanceof String
          && ((String)input).length() > CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT) {
        throw new CarbonDataLoadingException("Dataload failed, String size cannot exceed "
            + CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT + " bytes");
      }
      updateValueToByteStream(dataOutputStream, value);
    } else {
      if (this.carbonDimension.getDataType() == DataTypes.STRING
          && value.length > CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT) {
        throw new CarbonDataLoadingException("Dataload failed, String size cannot exceed "
            + CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT + " bytes");
      }
      if (parsedValue.length() > 0) {
        updateValueToByteStream(dataOutputStream,
            parsedValue.getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
      } else {
        updateNullValue(dataOutputStream, logHolder);
      }
    }
  }

  private String getDateOrTimeFormat() {
    if (this.carbonDimension.getDataType() == DataTypes.DATE) {
      return carbonDimension.getDateFormat();
    } else if (this.carbonDimension.getDataType() == DataTypes.TIMESTAMP) {
      return carbonDimension.getTimestampFormat();
    } else {
      return null;
    }
  }

  private byte[] writeDirectDictionary(
      Object input,
      String parsedValue,
      Boolean isWithoutConverter) {
    byte[] value;
    int surrogateKey;
    // If the input is a long value then this means that logical type was provided by
    // the user using AvroCarbonWriter. In this case directly generate surrogate key
    // using dictionaryGenerator.
    if (input instanceof Long) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2554
      surrogateKey = ((DirectDictionary) dictionaryGenerator).generateKey((long) input);
    } else if (input instanceof Integer) {
      // In case of file format, for complex type date or time type, input data comes as a
      // Integer object, so just assign the surrogate key with the input object value
      surrogateKey = (int) input;
    } else {
      // in case of data frame insert, date can come as string value
      if (isWithoutConverter) {
        surrogateKey = dictionaryGenerator.getOrGenerateKey(input.toString());
      } else {
        surrogateKey = dictionaryGenerator.getOrGenerateKey(parsedValue);
      }
    }
    if (surrogateKey == CarbonCommonConstants.INVALID_SURROGATE_KEY) {
      value = new byte[0];
    } else {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2884
      value = ByteUtil.toXorBytes(surrogateKey);
    }
    return value;
  }

  private void updateValueToByteStream(DataOutputStream dataOutputStream, byte[] value)
      throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3653
    if (DataTypeUtil.isByteArrayComplexChildColumn(dataType)) {
      dataOutputStream.writeInt(value.length);
    } else {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2477
      dataOutputStream.writeShort(value.length);
    }
    dataOutputStream.write(value);
  }

  private void updateNullValue(DataOutputStream dataOutputStream, BadRecordLogHolder logHolder)
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2452
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2451
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2450
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2453
      throws IOException {
    if (this.carbonDimension.getDataType() == DataTypes.STRING) {
      if (DataTypeUtil.isByteArrayComplexChildColumn(dataType)) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2437
        dataOutputStream.writeInt(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length);
      } else {
        dataOutputStream.writeShort(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length);
      }
      dataOutputStream.write(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY);
    } else {
      if (DataTypeUtil.isByteArrayComplexChildColumn(dataType)) {
        dataOutputStream.writeInt(CarbonCommonConstants.EMPTY_BYTE_ARRAY.length);
      } else {
        dataOutputStream.writeShort(CarbonCommonConstants.EMPTY_BYTE_ARRAY.length);
      }
      dataOutputStream.write(CarbonCommonConstants.EMPTY_BYTE_ARRAY);
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2452
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2451
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2450
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2453
    String message = logHolder.getColumnMessageMap().get(carbonDimension.getColName());
    if (null == message) {
      message = CarbonDataProcessorUtil
          .prepareFailureReason(carbonDimension.getColName(), carbonDimension.getDataType());
      logHolder.getColumnMessageMap().put(carbonDimension.getColName(), message);
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3645
    logHolder.setReason(message);
  }

  @Override
  public void parseComplexValue(ByteBuffer byteArrayInput, DataOutputStream dataOutputStream)
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
      throws IOException {
    if (!this.isDictionary) {
      int sizeOfData;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3653
      if (DataTypeUtil.isByteArrayComplexChildColumn(dataType)) {
        sizeOfData = byteArrayInput.getInt();
        dataOutputStream.writeInt(sizeOfData);
      } else {
        sizeOfData = byteArrayInput.getShort();
        dataOutputStream.writeShort(sizeOfData);
      }
      byte[] bb = new byte[sizeOfData];
      byteArrayInput.get(bb, 0, sizeOfData);
      dataOutputStream.write(bb);
    } else {
      int data = byteArrayInput.getInt();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
      byte[] v = ByteUtil.convertIntToBytes(data);
      dataOutputStream.write(v);
    }
  }

  /*
   * get all columns count
   */
  @Override
  public int getColsCount() {
    return 1;
  }

  /*
   * set outputarray
   */
  @Override
  public void setOutputArrayIndex(int outputArrayIndex) {
    this.outputArrayIndex = outputArrayIndex;
  }

  /*
   * get output array
   */
  @Override
  public int getMaxOutputArrayIndex() {
    return outputArrayIndex;
  }

  /*
   * split column and return metadata and primitive column
   */
  @Override
  public void getColumnarDataForComplexType(List<ArrayList<byte[]>> columnsArray,
      ByteBuffer inputArray) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2437
    if (!isDictionary) {
      int length;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3653
      if (DataTypeUtil.isByteArrayComplexChildColumn(dataType)) {
        length = inputArray.getInt();
      } else {
        length = inputArray.getShort();
      }
      byte[] key = new byte[length];
      inputArray.get(key);
      columnsArray.get(outputArrayIndex).add(key);
    } else {
      byte[] key = new byte[keySize];
      inputArray.get(key);
      columnsArray.get(outputArrayIndex).add(key);
    }
    dataCounter++;
  }

  /*
   * return datacounter
   */
  @Override
  public int getDataCounter() {
    return this.dataCounter;
  }

  /**
   * set key size
   * @param keySize
   */
  public void setKeySize(int keySize) {
    this.keySize = keySize;
  }

  @Override
  public GenericDataType<Object> deepCopy() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1400
    PrimitiveDataType dataType = new PrimitiveDataType(this.outputArrayIndex, 0);
    dataType.carbonDimension = this.carbonDimension;
    dataType.isDictionary = this.isDictionary;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3206
    dataType.parentName = this.parentName;
    dataType.columnId = this.columnId;
    dataType.dictionaryGenerator = this.dictionaryGenerator;
    dataType.nullFormat = this.nullFormat;
    dataType.setKeySize(this.keySize);
    dataType.setSurrogateIndex(this.index);
    dataType.name = this.name;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2607
    dataType.dataType = this.dataType;
    return dataType;
  }

  @Override
  public void getComplexColumnInfo(List<ComplexColumnInfo> columnInfoList) {
    columnInfoList.add(
        new ComplexColumnInfo(ColumnType.COMPLEX_PRIMITIVE, dataType,
            name, !isDictionary));
  }

  @Override
  public int getDepth() {
    // primitive type has no children
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3001
    return 1;
  }

}
