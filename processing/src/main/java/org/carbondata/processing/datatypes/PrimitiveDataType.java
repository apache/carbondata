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

package org.carbondata.processing.datatypes;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.processing.surrogatekeysgenerator.csvbased.CarbonCSVBasedDimSurrogateKeyGen;

import org.pentaho.di.core.exception.KettleException;

/**
 * Primitive DataType stateless object used in data loading
 */
public class PrimitiveDataType implements GenericDataType {

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

  /**
   * constructor
   * @param name
   * @param parentname
   * @param columnId
   */
  public PrimitiveDataType(String name, String parentname, String columnId) {
    this.name = name;
    this.parentname = parentname;
    this.columnId = columnId;
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
   * set column name
   */
  @Override
  public void setName(String name) {
    this.name = name;
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
  @Override
  public void parseStringAndWriteByteArray(String tableName, String inputString,
      String[] delimiter, int delimiterIndex, DataOutputStream dataOutputStream,
      CarbonCSVBasedDimSurrogateKeyGen surrogateKeyGen) throws KettleException, IOException {
    dataOutputStream.writeInt(surrogateKeyGen.generateSurrogateKeys(inputString, tableName
        + CarbonCommonConstants.UNDERSCORE + name, this.getColumnId()));
  }

  /*
   * parse bytearray and bit pack
   */
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
