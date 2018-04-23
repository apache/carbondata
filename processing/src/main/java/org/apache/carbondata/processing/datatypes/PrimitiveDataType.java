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
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.devapi.BiDictionary;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.dictionary.client.DictionaryClient;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessage;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessageType;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.loading.dictionary.DictionaryServerClientDictionary;
import org.apache.carbondata.processing.loading.dictionary.DirectDictionary;
import org.apache.carbondata.processing.loading.dictionary.PreCreatedDictionary;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;

/**
 * Primitive DataType stateless object used in data loading
 */
public class PrimitiveDataType implements GenericDataType<Object> {

  private static final LogService LOGGER =
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
  private String parentname;

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

  private boolean isEmptyBadRecord;

  private String nullformat;


  private PrimitiveDataType(int outputArrayIndex, int dataCounter) {
    this.outputArrayIndex = outputArrayIndex;
    this.dataCounter = dataCounter;
  }

  /**
   * constructor
   *
   * @param name
   * @param parentname
   * @param columnId
   * @param dimensionOrdinal
   * @param isDictionary
   */
  public PrimitiveDataType(String name, String parentname, String columnId, int dimensionOrdinal,
      boolean isDictionary, String nullformat, boolean isEmptyBadRecord) {
    this.name = name;
    this.parentname = parentname;
    this.columnId = columnId;
    this.isDictionary = isDictionary;
    this.nullformat = nullformat;
    this.isEmptyBadRecord = isEmptyBadRecord;
  }

  /**
   * Constructor
   * @param carbonColumn
   * @param parentname
   * @param columnId
   * @param carbonDimension
   * @param cache
   * @param absoluteTableIdentifier
   * @param client
   * @param useOnePass
   * @param localCache
   * @param nullFormat
   * @param isEmptyBadRecords
   */
  public PrimitiveDataType(CarbonColumn carbonColumn, String parentname, String columnId,
      CarbonDimension carbonDimension, Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache,
      AbsoluteTableIdentifier absoluteTableIdentifier, DictionaryClient client, Boolean useOnePass,
      Map<Object, Integer> localCache, String nullFormat, Boolean isEmptyBadRecords) {
    this.name = carbonColumn.getColName();
    this.parentname = parentname;
    this.columnId = columnId;
    this.carbonDimension = carbonDimension;
    this.isDictionary = isDictionaryDimension(carbonDimension);
    this.nullformat = nullFormat;
    this.isEmptyBadRecord = isEmptyBadRecords;

    DictionaryColumnUniqueIdentifier identifier =
        new DictionaryColumnUniqueIdentifier(absoluteTableIdentifier,
            carbonDimension.getColumnIdentifier(), carbonDimension.getDataType());
    try {
      if (carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        dictionaryGenerator = new DirectDictionary(DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(carbonDimension.getDataType()));
      } else if (carbonDimension.hasEncoding(Encoding.DICTIONARY)) {
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
  public String getParentname() {
    return parentname;
  }

  /*
   * get column unique id
   */
  @Override
  public String getColumnId() {
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

  @Override public void writeByteArray(Object input, DataOutputStream dataOutputStream)
      throws IOException, DictionaryGenerationException {

    String parsedValue =
        input == null ? null : DataTypeUtil.parseValue(input.toString(), carbonDimension);
    if (this.isDictionary) {
      Integer surrogateKey;
      if (null == parsedValue) {
        surrogateKey = CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY;
      } else {
        surrogateKey = dictionaryGenerator.getOrGenerateKey(parsedValue);
        if (surrogateKey == CarbonCommonConstants.INVALID_SURROGATE_KEY) {
          surrogateKey = CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY;
        }
      }
      dataOutputStream.writeInt(surrogateKey);
    } else {
      // Transform into ByteArray for No Dictionary.
      // TODO have to refactor and place all the cases present in NonDictionaryFieldConverterImpl
      if (null == parsedValue && this.carbonDimension.getDataType() != DataTypes.STRING) {
        updateNullValue(dataOutputStream);
      } else if (null == parsedValue || parsedValue.equals(nullformat)) {
        updateNullValue(dataOutputStream);
      } else {
        String dateFormat = null;
        if (this.carbonDimension.getDataType() == DataTypes.DATE) {
          dateFormat = this.carbonDimension.getDateFormat();
        } else if (this.carbonDimension.getDataType() == DataTypes.TIMESTAMP) {
          dateFormat = this.carbonDimension.getTimestampFormat();
        }

        try {
          if (!this.carbonDimension.getUseActualData()) {
            byte[] value = DataTypeUtil.getBytesBasedOnDataTypeForNoDictionaryColumn(parsedValue,
                this.carbonDimension.getDataType(), dateFormat);
            if (this.carbonDimension.getDataType() == DataTypes.STRING
                && value.length > CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT) {
              throw new CarbonDataLoadingException("Dataload failed, String size cannot exceed "
                  + CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT + " bytes");
            }
            updateValueToByteStream(dataOutputStream, value);
          } else {
            Object value = DataTypeUtil.getDataDataTypeForNoDictionaryColumn(parsedValue,
                this.carbonDimension.getDataType(), dateFormat);
            if (this.carbonDimension.getDataType() == DataTypes.STRING
                && value.toString().length() > CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT) {
              throw new CarbonDataLoadingException("Dataload failed, String size cannot exceed "
                  + CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT + " bytes");
            }
            if (parsedValue.length() > 0) {
              updateValueToByteStream(dataOutputStream,
                  parsedValue.getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
            } else {
              updateNullValue(dataOutputStream);
            }
          }
        } catch (CarbonDataLoadingException e) {
          throw e;
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
    dataOutputStream.writeInt(value.length);
    dataOutputStream.write(value);
  }

  private void updateNullValue(DataOutputStream dataOutputStream) throws IOException {
    if (this.carbonDimension.getDataType() == DataTypes.STRING) {
      dataOutputStream.write(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY);
    } else {
      dataOutputStream.write(CarbonCommonConstants.EMPTY_BYTE_ARRAY);
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
      int sizeOfData = byteArrayInput.getInt();
      dataOutputStream.writeInt(sizeOfData);
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
  @Override
  public void getColumnarDataForComplexType(List<ArrayList<byte[]>> columnsArray,
      ByteBuffer inputArray) {
    byte[] key = new byte[keySize];
    inputArray.get(key);
    columnsArray.get(outputArrayIndex).add(key);
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
    dataType.parentname = this.parentname;
    dataType.columnId = this.columnId;
    dataType.dictionaryGenerator = this.dictionaryGenerator;
    dataType.isEmptyBadRecord = this.isEmptyBadRecord;
    dataType.nullformat = this.nullformat;
    dataType.setKeySize(this.keySize);
    dataType.setSurrogateIndex(this.index);

    return dataType;
  }
}
