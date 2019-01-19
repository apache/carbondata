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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.row.ComplexColumnInfo;
import org.apache.carbondata.core.devapi.BiDictionary;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.dictionary.client.DictionaryClient;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessage;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessageType;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.dictionary.DictionaryServerClientDictionary;
import org.apache.carbondata.processing.loading.dictionary.DirectDictionary;
import org.apache.carbondata.processing.loading.dictionary.PreCreatedDictionary;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.apache.log4j.Logger;

/**
 * Primitive DataType stateless object used in data loading
 */
public class PrimitiveDataType implements GenericDataType<Object> {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(PrimitiveDataType.class.getName());

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

  private BiDictionary<Integer, Object> dictionaryGenerator;

  private CarbonDimension carbonDimension;

  private boolean isDictionary;

  private String nullFormat;

  private boolean isDirectDictionary;

  private DataType dataType;

  private PrimitiveDataType(int outputArrayIndex, int dataCounter) {
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
   * @param absoluteTableIdentifier
   * @param client
   * @param useOnePass
   * @param localCache
   * @param nullFormat
   */
  public PrimitiveDataType(CarbonColumn carbonColumn, String parentName, String columnId,
      CarbonDimension carbonDimension, AbsoluteTableIdentifier absoluteTableIdentifier,
      DictionaryClient client, Boolean useOnePass, Map<Object, Integer> localCache,
      String nullFormat) {
    this.name = carbonColumn.getColName();
    this.parentName = parentName;
    this.columnId = columnId;
    this.carbonDimension = carbonDimension;
    this.isDictionary = isDictionaryDimension(carbonDimension);
    this.nullFormat = nullFormat;
    this.dataType = carbonColumn.getDataType();

    DictionaryColumnUniqueIdentifier identifier =
        new DictionaryColumnUniqueIdentifier(absoluteTableIdentifier,
            carbonDimension.getColumnIdentifier(), carbonDimension.getDataType());
    try {
      if (carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY)
          || carbonColumn.getDataType() == DataTypes.DATE) {
        dictionaryGenerator = new DirectDictionary(DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(carbonDimension.getDataType(),
                getDateFormat(carbonDimension)));
        isDirectDictionary = true;
      } else if (carbonDimension.hasEncoding(Encoding.DICTIONARY)) {
        CacheProvider cacheProvider = CacheProvider.getInstance();
        Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache =
            cacheProvider.createCache(CacheType.REVERSE_DICTIONARY);
        Dictionary dictionary = null;
        if (useOnePass) {
          if (CarbonUtil.isFileExistsForGivenColumn(identifier)) {
            dictionary = cache.get(identifier);
          }
          DictionaryMessage dictionaryMessage = new DictionaryMessage();
          dictionaryMessage.setColumnName(carbonDimension.getColName());
          // for table initialization
          dictionaryMessage
              .setTableUniqueId(absoluteTableIdentifier.getCarbonTableIdentifier().getTableId());
          dictionaryMessage.setData("0");
          // for generate dictionary
          dictionaryMessage.setType(DictionaryMessageType.DICT_GENERATION);
          dictionaryGenerator = new DictionaryServerClientDictionary(dictionary, client,
              dictionaryMessage, localCache);
        } else {
          dictionary = cache.get(identifier);
          dictionaryGenerator = new PreCreatedDictionary(dictionary);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * get dateformat
   * @param carbonDimension
   * @return
   */
  private String getDateFormat(CarbonDimension carbonDimension) {
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
   * get surrogate index
   */
  @Override
  public int getSurrogateIndex() {
    return index;
  }

  /*
   * set surrogate index
   */
  @Override public void setSurrogateIndex(int surrIndex) {
    if (this.carbonDimension != null && !this.carbonDimension.hasEncoding(Encoding.DICTIONARY)) {
      index = 0;
    } else if (this.carbonDimension == null && isDictionary == false) {
      index = 0;
    } else {
      index = surrIndex;
    }
  }

  @Override public boolean getIsColumnDictionary() {
    return isDictionary;
  }

  @Override public void writeByteArray(Object input, DataOutputStream dataOutputStream,
      BadRecordLogHolder logHolder) throws IOException, DictionaryGenerationException {
    String parsedValue =
        input == null ? null : DataTypeUtil.parseValue(input.toString(), carbonDimension);
    String message = logHolder.getColumnMessageMap().get(carbonDimension.getColName());
    if (this.isDictionary) {
      Integer surrogateKey;
      if (null == parsedValue) {
        surrogateKey = CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY;
        if (null == message) {
          message = CarbonDataProcessorUtil
              .prepareFailureReason(carbonDimension.getColName(), carbonDimension.getDataType());
          logHolder.getColumnMessageMap().put(carbonDimension.getColName(), message);
          logHolder.setReason(message);
        }
      } else {
        if (dictionaryGenerator instanceof DirectDictionary && input instanceof Long) {
          surrogateKey = ((DirectDictionary) dictionaryGenerator).generateKey((long) input);
        } else {
          surrogateKey = dictionaryGenerator.getOrGenerateKey(parsedValue);
        }
        if (surrogateKey == CarbonCommonConstants.INVALID_SURROGATE_KEY) {
          surrogateKey = CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY;
          message = CarbonDataProcessorUtil
              .prepareFailureReason(carbonDimension.getColName(), carbonDimension.getDataType());
          logHolder.getColumnMessageMap().put(carbonDimension.getColName(), message);
          logHolder.setReason(message);
        }
      }
      dataOutputStream.writeInt(surrogateKey);
    } else {
      // Transform into ByteArray for No Dictionary.
      // TODO have to refactor and place all the cases present in NonDictionaryFieldConverterImpl
      if (null == parsedValue && this.carbonDimension.getDataType() != DataTypes.STRING) {
        updateNullValue(dataOutputStream, logHolder);
      } else if (null == parsedValue || parsedValue.equals(nullFormat)) {
        updateNullValue(dataOutputStream, logHolder);
      } else {
        String dateFormat = null;
        if (this.carbonDimension.getDataType() == DataTypes.DATE) {
          dateFormat = carbonDimension.getDateFormat();
        } else if (this.carbonDimension.getDataType() == DataTypes.TIMESTAMP) {
          dateFormat = carbonDimension.getTimestampFormat();
        }
        try {
          if (!this.carbonDimension.getUseActualData()) {
            byte[] value = null;
            if (isDirectDictionary) {
              int surrogateKey;
              if (!(input instanceof Long)) {
                SimpleDateFormat parser = new SimpleDateFormat(getDateFormat(carbonDimension));
                parser.parse(parsedValue);
              }
              // If the input is a long value then this means that logical type was provided by
              // the user using AvroCarbonWriter. In this case directly generate surrogate key
              // using dictionaryGenerator.
              if (dictionaryGenerator instanceof DirectDictionary && input instanceof Long) {
                surrogateKey = ((DirectDictionary) dictionaryGenerator).generateKey((long) input);
              } else {
                surrogateKey = dictionaryGenerator.getOrGenerateKey(parsedValue);
              }
              if (surrogateKey == CarbonCommonConstants.INVALID_SURROGATE_KEY) {
                value = new byte[0];
              } else {
                value = ByteUtil.toXorBytes(surrogateKey);
              }
            } else {
              // If the input is a long value then this means that logical type was provided by
              // the user using AvroCarbonWriter. In this case directly generate Bytes from value.
              if (this.carbonDimension.getDataType().equals(DataTypes.DATE)
                  || this.carbonDimension.getDataType().equals(DataTypes.TIMESTAMP)
                  && input instanceof Long) {
                if (dictionaryGenerator != null) {
                  value = ByteUtil.toXorBytes(((DirectDictionary) dictionaryGenerator)
                      .generateKey((long) input));
                } else {
                  value = ByteUtil.toXorBytes(Long.parseLong(parsedValue));
                }
              } else {
                value = DataTypeUtil.getBytesBasedOnDataTypeForNoDictionaryColumn(parsedValue,
                    this.carbonDimension.getDataType(), dateFormat);
              }
              if (this.carbonDimension.getDataType() == DataTypes.STRING
                  && value.length > CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT) {
                throw new CarbonDataLoadingException("Dataload failed, String size cannot exceed "
                    + CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT + " bytes");
              }
            }
            updateValueToByteStream(dataOutputStream, value);
          } else {
            Object value;
            if (dictionaryGenerator instanceof DirectDictionary
                && input instanceof Long) {
              value = ByteUtil.toXorBytes(
                  ((DirectDictionary) dictionaryGenerator).generateKey((long) input));
            } else {
              value = DataTypeUtil.getDataDataTypeForNoDictionaryColumn(parsedValue,
                  this.carbonDimension.getDataType(), dateFormat);
            }
            if (this.carbonDimension.getDataType() == DataTypes.STRING
                && value.toString().length() > CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT) {
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
        } catch (NumberFormatException e) {
          // Update logHolder for bad record and put null in dataOutputStream.
          updateNullValue(dataOutputStream, logHolder);
        } catch (CarbonDataLoadingException e) {
          throw e;
        } catch (ParseException ex) {
          updateNullValue(dataOutputStream, logHolder);
        } catch (Throwable ex) {
          // TODO have to implemented the Bad Records LogHolder.
          // Same like NonDictionaryFieldConverterImpl.
          throw ex;
        }
      }
    }
  }

  private void updateValueToByteStream(DataOutputStream dataOutputStream, byte[] value)
      throws IOException {
    dataOutputStream.writeShort(value.length);
    dataOutputStream.write(value);
  }

  private void updateNullValue(DataOutputStream dataOutputStream, BadRecordLogHolder logHolder)
      throws IOException {
    if (this.carbonDimension.getDataType() == DataTypes.STRING) {
      dataOutputStream.writeShort(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length);
      dataOutputStream.write(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY);
    } else {
      dataOutputStream.writeShort(CarbonCommonConstants.EMPTY_BYTE_ARRAY.length);
      dataOutputStream.write(CarbonCommonConstants.EMPTY_BYTE_ARRAY);
    }
    String message = logHolder.getColumnMessageMap().get(carbonDimension.getColName());
    if (null == message) {
      message = CarbonDataProcessorUtil
          .prepareFailureReason(carbonDimension.getColName(), carbonDimension.getDataType());
      logHolder.getColumnMessageMap().put(carbonDimension.getColName(), message);
      logHolder.setReason(message);
    }
  }

  @Override public void fillCardinality(List<Integer> dimCardWithComplex) {
    if (!this.carbonDimension.hasEncoding(Encoding.DICTIONARY)) {
      return;
    }
    dimCardWithComplex.add(dictionaryGenerator.size());
  }

  @Override
  public void parseComplexValue(ByteBuffer byteArrayInput, DataOutputStream dataOutputStream,
      KeyGenerator[] generator)
      throws IOException, KeyGenException {
    if (!this.isDictionary) {
      int sizeOfData = byteArrayInput.getShort();
      dataOutputStream.writeShort(sizeOfData);
      byte[] bb = new byte[sizeOfData];
      byteArrayInput.get(bb, 0, sizeOfData);
      dataOutputStream.write(bb);
    } else {
      int data = byteArrayInput.getInt();
      byte[] v = generator[index].generateKey(new int[] { data });
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
  @Override public void getColumnarDataForComplexType(List<ArrayList<byte[]>> columnsArray,
      ByteBuffer inputArray) {
    if (!isDictionary) {
      byte[] key = new byte[inputArray.getShort()];
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

  /*
   * fill agg key block
   */
  @Override
  public void fillAggKeyBlock(List<Boolean> aggKeyBlockWithComplex, boolean[] aggKeyBlock) {
    aggKeyBlockWithComplex.add(aggKeyBlock[index]);
  }

  /*
   * fill block key size
   */
  @Override
  public void fillBlockKeySize(List<Integer> blockKeySizeWithComplex, int[] primitiveBlockKeySize) {
    blockKeySizeWithComplex.add(primitiveBlockKeySize[index]);
    this.keySize = primitiveBlockKeySize[index];
  }

  /*
   * fill cardinality
   */
  @Override
  public void fillCardinalityAfterDataLoad(List<Integer> dimCardWithComplex,
      int[] maxSurrogateKeyArray) {
    dimCardWithComplex.add(maxSurrogateKeyArray[index]);
  }

  @Override
  public GenericDataType<Object> deepCopy() {
    PrimitiveDataType dataType = new PrimitiveDataType(this.outputArrayIndex, 0);
    dataType.carbonDimension = this.carbonDimension;
    dataType.isDictionary = this.isDictionary;
    dataType.parentName = this.parentName;
    dataType.columnId = this.columnId;
    dataType.dictionaryGenerator = this.dictionaryGenerator;
    dataType.nullFormat = this.nullFormat;
    dataType.setKeySize(this.keySize);
    dataType.setSurrogateIndex(this.index);
    dataType.name = this.name;
    dataType.dataType = this.dataType;
    return dataType;
  }

  @Override
  public void getComplexColumnInfo(List<ComplexColumnInfo> columnInfoList) {
    columnInfoList.add(
        new ComplexColumnInfo(ColumnType.COMPLEX_PRIMITIVE, dataType,
            name, !isDictionary));
  }

}