/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.processing.datatypes;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.devapi.BiDictionary;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.dictionary.client.DictionaryClient;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryKey;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.CarbonUtilException;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.newflow.dictionary.DictionaryServerClientDictionary;
import org.apache.carbondata.processing.newflow.dictionary.DirectDictionary;
import org.apache.carbondata.processing.newflow.dictionary.PreCreatedDictionary;
import org.apache.carbondata.processing.surrogatekeysgenerator.csvbased.CarbonCSVBasedDimSurrogateKeyGen;

import org.pentaho.di.core.exception.KettleException;

/**
 * Primitive DataType stateless object used in data loading
 */
public class PrimitiveDataType implements GenericDataType<Object> {

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
   * dimension ordinal of primitive type column
   */
  private int dimensionOrdinal;

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

  /**
   * constructor
   *
   * @param name
   * @param parentname
   * @param columnId
   */
  public PrimitiveDataType(String name, String parentname, String columnId, int dimensionOrdinal) {
    this.name = name;
    this.parentname = parentname;
    this.columnId = columnId;
    this.dimensionOrdinal = dimensionOrdinal;
  }

  /**
   * constructor
   *
   * @param name
   * @param parentname
   * @param columnId
   */
  public PrimitiveDataType(String name, String parentname, String columnId,
                           CarbonDimension carbonDimension,
                           Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache,
                           CarbonTableIdentifier carbonTableIdentifier,
                           DictionaryClient client, Boolean useOnePass, String storePath) {
    this.name = name;
    this.parentname = parentname;
    this.columnId = columnId;
    this.carbonDimension = carbonDimension;
    DictionaryColumnUniqueIdentifier identifier =
        new DictionaryColumnUniqueIdentifier(carbonTableIdentifier,
            carbonDimension.getColumnIdentifier(), carbonDimension.getDataType());
    try {
      if (carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        dictionaryGenerator = new DirectDictionary(DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(carbonDimension.getDataType()));
      } else {
        Dictionary dictionary = null;
        if (useOnePass) {
          if (CarbonUtil.isFileExistsForGivenColumn(storePath, identifier)) {
            try {
              dictionary = cache.get(identifier);
            } catch (CarbonUtilException e) {
              throw new RuntimeException(e);
            }
          }
          String threadNo = "initial";
          DictionaryKey dictionaryKey = new DictionaryKey();
          dictionaryKey.setColumnName(carbonDimension.getColName());
          dictionaryKey.setTableUniqueName(carbonTableIdentifier.getTableUniqueName());
          dictionaryKey.setThreadNo(threadNo);
          // for table initialization
          dictionaryKey.setType("TABLE_INTIALIZATION");
          dictionaryKey.setData("0");
          client.getDictionary(dictionaryKey);
          Map<Object, Integer> localCache = new HashMap<>();
          // for generate dictionary
          dictionaryKey.setType("DICTIONARY_GENERATION");
          dictionaryGenerator = new DictionaryServerClientDictionary(dictionary, client,
                  dictionaryKey, localCache);
        } else {
          dictionary = cache.get(identifier);
          dictionaryGenerator = new PreCreatedDictionary(dictionary);
        }
      }
    } catch (CarbonUtilException e) {
      throw new RuntimeException(e);
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
  @Override
  public void setSurrogateIndex(int surrIndex) {
    index = surrIndex;
  }

  /*
   * parse string and generate surrogate
   */
  @Override public void parseStringAndWriteByteArray(String tableName, String inputString,
      String[] delimiter, int delimiterIndex, DataOutputStream dataOutputStream,
      CarbonCSVBasedDimSurrogateKeyGen surrogateKeyGen) throws KettleException, IOException {
    String parsedValue = DataTypeUtil.parseValue(inputString,
        surrogateKeyGen.getDimensionOrdinalToDimensionMapping()[dimensionOrdinal]);
    Integer surrogateKey = null;
    if (null == parsedValue) {
      surrogateKey = CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY;
    } else {
      surrogateKey = surrogateKeyGen
          .generateSurrogateKeys(parsedValue, tableName + CarbonCommonConstants.UNDERSCORE + name,
              this.getColumnId());
      if (surrogateKey == CarbonCommonConstants.INVALID_SURROGATE_KEY) {
        surrogateKey = CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY;
      }
    }
    dataOutputStream.writeInt(surrogateKey);
  }

  @Override public void writeByteArray(Object input, DataOutputStream dataOutputStream)
      throws IOException, DictionaryGenerationException {
    String parsedValue =
        input == null ? null : DataTypeUtil.parseValue(input.toString(), carbonDimension);
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
  }

  @Override
  public void fillCardinality(List<Integer> dimCardWithComplex) {
    dimCardWithComplex.add(dictionaryGenerator.size());
  }

  @Override
  public void parseAndBitPack(ByteBuffer byteArrayInput, DataOutputStream dataOutputStream,
      KeyGenerator[] generator) throws IOException, KeyGenException {
    int data = byteArrayInput.getInt();
    dataOutputStream.write(generator[index].generateKey(new int[] { data }));
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
}
