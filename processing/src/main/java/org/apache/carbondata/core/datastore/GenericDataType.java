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

package org.apache.carbondata.core.datastore;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;

/**
 * Generic DataType interface which will be used while data loading for complex types like Array &
 * Struct
 */
public interface GenericDataType<T> {

  /**
   * @return name of the column
   */
  String getName();

  /**
   * @return - columns parent name
   */
  String getParentname();

  /**
   * @param children - To add children dimension for parent complex type
   */
  void addChildren(GenericDataType children);

  /**
   * @param primitiveChild - Returns all primitive type columns in complex type
   */
  void getAllPrimitiveChildren(List<GenericDataType> primitiveChild);

  /**
   * writes to byte stream
   * @param dataOutputStream
   * @throws IOException
   */
  void writeByteArray(T input, DataOutputStream dataOutputStream)
      throws IOException, DictionaryGenerationException;

  /**
   * @return surrogateIndex for primitive column in complex type
   */
  int getSurrogateIndex();

  /**
   * @param surrIndex - surrogate index of primitive column in complex type
   */
  void setSurrogateIndex(int surrIndex);

  /**
   * converts integer surrogate to bit packed surrogate value
   * @param byteArrayInput
   * @param dataOutputStream
   * @param generator
   * @throws IOException
   * @throws KeyGenException
   */
  void parseAndBitPack(ByteBuffer byteArrayInput, DataOutputStream dataOutputStream,
      KeyGenerator[] generator) throws IOException, KeyGenException;

  /**
   * @return columns count of each complex type
   */
  int getColsCount();

  /**
   * @return column uuid string
   */
  String getColumnId();

  /**
   * set array index to be referred while creating metadata column
   * @param outputArrayIndex
   */
  void setOutputArrayIndex(int outputArrayIndex);

  /**
   * @return array index count of metadata column
   */
  int getMaxOutputArrayIndex();

  /**
   * Split byte array into complex metadata column and primitive column
   * @param columnsArray
   * @param inputArray
   */
  void getColumnarDataForComplexType(List<ArrayList<byte[]>> columnsArray, ByteBuffer inputArray);

  /**
   * @return current read row count
   */
  int getDataCounter();

  /**
   * fill agg key block including complex types
   * @param aggKeyBlockWithComplex
   * @param aggKeyBlock
   */
  void fillAggKeyBlock(List<Boolean> aggKeyBlockWithComplex, boolean[] aggKeyBlock);

  /**
   * fill block key size including complex types
   * @param blockKeySizeWithComplex
   * @param primitiveBlockKeySize
   */
  void fillBlockKeySize(List<Integer> blockKeySizeWithComplex, int[] primitiveBlockKeySize);

  /**
   * fill cardinality value including complex types
   * @param dimCardWithComplex
   * @param maxSurrogateKeyArray
   */
  void fillCardinalityAfterDataLoad(List<Integer> dimCardWithComplex, int[] maxSurrogateKeyArray);

  /**
   * Fill the cardinality of the primitive datatypes
   * @param dimCardWithComplex
   */
  void fillCardinality(List<Integer> dimCardWithComplex);

}
